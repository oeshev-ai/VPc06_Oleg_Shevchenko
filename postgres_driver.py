"""Драйвер PostgreSQL для использования во внешних проектах."""

from __future__ import annotations

import logging
import os
import threading
from collections.abc import Mapping as ABCMapping
from contextlib import contextmanager
from typing import Any, Generator, Iterable, Mapping, Sequence

import psycopg2
import psycopg2.extensions
from dotenv import load_dotenv
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)


class PostgresDriver:
    """Клиент для CRUD-операций с PostgreSQL.

    Параметры подключения читаются из окружения / `.env`
    ::
        PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

    Дополнительно можно передать любые аргументы ``psycopg2.connect``
    через ``connect_kwargs`` (например ``sslmode``, ``connect_timeout``,
    ``application_name``).

    ``where`` поддерживает операторы через двойное подчёркивание::

        {"id": 1}                    # id = %s
        {"age__gte": 18}             # age >= %s
        {"name__ilike": "%foo%"}     # name ILIKE %s
        {"id__in": [1, 2, 3]}        # id IN (%s, %s, %s)
        {"id__notin": [4, 5]}        # id NOT IN (...)
        {"deleted_at__isnull": True} # deleted_at IS NULL
        {"deleted_at__isnull": False}# deleted_at IS NOT NULL
        {"email": None}              # email IS NULL
        {"email__ne": None}          # email IS NOT NULL

    Поддерживаемые операторы: ``eq, ne, lt, lte, gt, gte, like, ilike,
    in, notin, isnull``.
    """

    _OPERATORS: dict[str, str] = {
        "eq": "=",
        "ne": "<>",
        "lt": "<",
        "lte": "<=",
        "gt": ">",
        "gte": ">=",
        "like": "LIKE",
        "ilike": "ILIKE",
    }

    # Стандартный лимит числа параметров в PostgreSQL.
    _MAX_PARAMS: int = 65535

    def __init__(
        self,
        *,
        host: str | None = None,
        port: str | int | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        dotenv_path: str | None = None,
        autocommit: bool = False,
        connect_kwargs: Mapping[str, Any] | None = None,
        reconnect_on_failure: bool = False,
    ) -> None:
        load_dotenv(dotenv_path)

        resolved_port = os.getenv("PGPORT", "5432") if port is None else port

        self._config: dict[str, Any] = {
            "host": host if host is not None else os.getenv("PGHOST", "localhost"),
            "port": str(resolved_port),
            "dbname": dbname if dbname is not None else os.getenv("PGDATABASE", "postgres"),
            "user": user if user is not None else os.getenv("PGUSER", "postgres"),
            "password": password if password is not None else os.getenv("PGPASSWORD", ""),
        }

        if connect_kwargs:
            self._config.update(dict(connect_kwargs))

        self._autocommit = autocommit
        self._reconnect_on_failure = reconnect_on_failure
        self._conn: psycopg2.extensions.connection | None = None
        self._lock = threading.RLock()
        self._in_transaction = False

    # --- служебные утилиты ---

    @staticmethod
    def _safe_config(config: Mapping[str, Any]) -> dict[str, Any]:
        """Возвращает конфигурацию для логов без чувствительных данных."""
        safe: dict[str, Any] = {}

        for key, value in config.items():
            lower_key = key.lower()

            if lower_key == "password":
                continue

            if lower_key == "dsn" and isinstance(value, str) and "@" in value:
                safe[key] = "<dsn with credentials>"
            else:
                safe[key] = value

        return safe

    # --- подключение ---

    def connect(self) -> "PostgresDriver":
        """Открыть соединение с БД."""
        with self._lock:
            if self._conn is None or self._conn.closed:
                logger.debug(
                    "Открываю соединение с PostgreSQL: %s",
                    self._safe_config(self._config),
                )

                conn = psycopg2.connect(**self._config)
                try:
                    conn.autocommit = self._autocommit
                except Exception:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    raise

                self._conn = conn

        return self

    def close(self) -> None:
        """Закрыть соединение."""
        with self._lock:
            conn = self._conn
            self._conn = None
            self._in_transaction = False

        if conn is not None and not conn.closed:
            try:
                conn.close()
            except Exception:
                logger.debug("Ошибка при закрытии соединения", exc_info=True)

    @property
    def connection(self) -> psycopg2.extensions.connection:
        """Вернуть активное соединение.

        Если соединение закрыто и включён ``reconnect_on_failure``,
        драйвер попробует переподключиться.

        Если соединение потеряно во время транзакции, молча переподключаться
        нельзя, потому что состояние транзакции уже потеряно.
        """
        with self._lock:
            if self._conn is None:
                if self._in_transaction:
                    self._in_transaction = False
                    raise RuntimeError("Соединение потеряно во время транзакции")

                if self._reconnect_on_failure:
                    logger.debug("Нет активного соединения, подключаюсь")
                    self.connect()
                else:
                    raise RuntimeError(
                        "Нет активного соединения. Вызовите connect() сначала."
                    )

            elif self._conn.closed:
                if self._in_transaction:
                    self._in_transaction = False
                    raise RuntimeError("Соединение потеряно во время транзакции")

                if self._reconnect_on_failure:
                    logger.debug("Соединение потеряно, переподключаюсь")
                    self._conn = None
                    self.connect()
                else:
                    raise RuntimeError(
                        "Нет активного соединения. Вызовите connect() сначала."
                    )

            assert self._conn is not None
            return self._conn

    def __enter__(self) -> "PostgresDriver":
        return self.connect()

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            conn = self._conn
            if (
                exc_type is not None
                and conn is not None
                and not conn.closed
                and not conn.autocommit
            ):
                try:
                    conn.rollback()
                except Exception:
                    logger.exception("Не удалось выполнить rollback при выходе")
        finally:
            self.close()

    def commit(self) -> None:
        """Зафиксировать текущую транзакцию."""
        conn = self._conn
        if conn is not None and not conn.closed and not conn.autocommit:
            conn.commit()

    def rollback(self) -> None:
        """Откатить текущую транзакцию."""
        conn = self._conn
        if conn is not None and not conn.closed and not conn.autocommit:
            conn.rollback()

    # --- контекстные менеджеры ---

    @contextmanager
    def cursor(self, *, commit: bool = True) -> Generator[RealDictCursor, None, None]:
        """Контекстный курсор с результатами в виде dict.

        ``commit=True`` — закоммитить транзакцию при успешном выполнении,
        если соединение не в режиме autocommit и мы не внутри :meth:`transaction`.
        """
        conn = self.connection
        cur = conn.cursor(cursor_factory=RealDictCursor)

        try:
            yield cur

            if (
                commit
                and not self._in_transaction
                and not conn.closed
                and not conn.autocommit
            ):
                conn.commit()

        except BaseException:
            if (
                not self._in_transaction
                and not conn.closed
                and not conn.autocommit
            ):
                try:
                    conn.rollback()
                except Exception:
                    logger.exception("Не удалось выполнить rollback в cursor()")
            raise

        finally:
            try:
                cur.close()
            except Exception:
                logger.debug("Ошибка при закрытии курсора", exc_info=True)

    @contextmanager
    def transaction(self) -> Generator[RealDictCursor, None, None]:
        """Явная транзакция: коммит при успехе, откат при исключении.

        Внутри блока :meth:`cursor` не коммитит самостоятельно.
        Вложенные транзакции не поддерживаются.
        """
        with self._lock:
            if self._in_transaction:
                raise RuntimeError("Вложенные транзакции не поддерживаются")

            # Получаем соединение до установки флага, чтобы ошибка подключения
            # не оставила _in_transaction=True.
            conn = self.connection
            self._in_transaction = True

        try:
            prev_autocommit = conn.autocommit

            if prev_autocommit:
                conn.autocommit = False

            cur = conn.cursor(cursor_factory=RealDictCursor)

            try:
                yield cur
                conn.commit()

            except BaseException:
                if not conn.closed:
                    try:
                        conn.rollback()
                    except Exception:
                        logger.exception(
                            "Не удалось выполнить rollback в transaction()"
                        )
                raise

            finally:
                try:
                    cur.close()
                except Exception:
                    logger.debug("Ошибка при закрытии курсора", exc_info=True)

                if prev_autocommit and not conn.closed:
                    try:
                        conn.autocommit = True
                    except Exception:
                        logger.debug(
                            "Не удалось вернуть режим autocommit",
                            exc_info=True,
                        )

        finally:
            with self._lock:
                self._in_transaction = False

    # --- диагностическое ---

    def ping(self) -> dict[str, Any]:
        """Проверить соединение и вернуть базовую информацию о сервере."""
        with self.cursor(commit=False) as cur:
            cur.execute(
                "SELECT version() AS version, "
                "current_database() AS dbname, "
                'current_user AS "user";'
            )
            row = cur.fetchone()
            return dict(row) if row else {}

    def _log_query(self, query: Any, params: Any) -> None:
        if not logger.isEnabledFor(logging.DEBUG):
            return

        try:
            if isinstance(query, str):
                rendered = query
            else:
                # Используем текущее соединение без принудительного reconnect.
                rendered = query.as_string(self._conn)
        except Exception:
            rendered = str(query)

        logger.debug("SQL: %s | params=%r", rendered, params)

    # --- CRUD ---

    def create(
        self,
        table: str,
        data: Mapping[str, Any],
        *,
        returning: str | Sequence[str] | None = "*",
    ) -> dict[str, Any] | None:
        """INSERT одной строки.

        Возвращает запись по ``RETURNING``.
        Если ``returning=None``, возвращает ``None``.
        """
        if not data:
            raise ValueError("data не может быть пустым")

        columns = list(data.keys())
        for col in columns:
            if not col:
                raise ValueError("Имя колонки в data не может быть пустым")

        values = [data[col] for col in columns]

        query = sql.SQL(
            "INSERT INTO {table} ({fields}) VALUES ({placeholders})"
        ).format(
            table=sql.Identifier(table),
            fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(", ").join(sql.Placeholder() * len(columns)),
        )

        returning_sql = self._returning(returning)
        if returning_sql is not None:
            query += sql.SQL(" RETURNING {returning}").format(
                returning=returning_sql
            )

        self._log_query(query, values)

        with self.cursor() as cur:
            cur.execute(query, values)

            if returning_sql is None or cur.description is None:
                return None

            row = cur.fetchone()
            return dict(row) if row else None

    def create_many(
        self,
        table: str,
        rows: Sequence[Mapping[str, Any]],
        *,
        returning: str | Sequence[str] | None = None,
        chunk_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """INSERT нескольких строк с одинаковым набором колонок.

        Работает через многострочный ``INSERT ... VALUES (...), (...)``,
        поэтому поддерживает ``RETURNING``.

        При ``returning=None`` строки не возвращаются.

        По умолчанию все чанки выполняются в одном курсоре, поэтому при
        ``autocommit=False`` операция будет атомарной, если не произошло
        ошибки в середине.
        """
        if not rows:
            return []

        if chunk_size <= 0:
            raise ValueError("chunk_size должен быть положительным")

        try:
            columns = list(rows[0].keys())
        except AttributeError as exc:
            raise ValueError("rows должны быть последовательностью маппингов") from exc

        if not columns:
            raise ValueError("data не может быть пустым")

        for col in columns:
            if not col:
                raise ValueError("Имя колонки в rows не может быть пустым")

        expected = set(columns)

        for i, row in enumerate(rows):
            try:
                keys = set(row.keys())
            except AttributeError as exc:
                raise ValueError(f"Строка #{i} не является маппингом") from exc

            if keys != expected:
                raise ValueError(f"Строка #{i} имеет другой набор колонок")

        if len(columns) > self._MAX_PARAMS:
            raise ValueError(
                "Слишком много колонок для одного INSERT: "
                f"{len(columns)} > {self._MAX_PARAMS}"
            )

        # Ограничиваем число строк в чанке, чтобы не превысить лимит параметров.
        max_rows_per_chunk = max(1, self._MAX_PARAMS // len(columns))
        chunk_size = min(chunk_size, max_rows_per_chunk)

        one_row = sql.SQL("({ph})").format(
            ph=sql.SQL(", ").join(sql.Placeholder() * len(columns))
        )

        returning_sql = self._returning(returning)
        result: list[dict[str, Any]] = []

        with self.cursor() as cur:
            for start in range(0, len(rows), chunk_size):
                chunk = rows[start : start + chunk_size]

                query = sql.SQL(
                    "INSERT INTO {table} ({fields}) VALUES {values}"
                ).format(
                    table=sql.Identifier(table),
                    fields=sql.SQL(", ").join(map(sql.Identifier, columns)),
                    values=sql.SQL(", ").join(one_row for _ in chunk),
                )

                if returning_sql is not None:
                    query += sql.SQL(" RETURNING {returning}").format(
                        returning=returning_sql
                    )

                params: list[Any] = []
                for row in chunk:
                    params.extend(row[c] for c in columns)

                self._log_query(query, params)

                cur.execute(query, params)

                if returning_sql is not None and cur.description is not None:
                    result.extend(dict(r) for r in cur.fetchall())

        return result

    def read(
        self,
        table: str,
        *,
        columns: Sequence[str] | str | None = None,
        where: Mapping[str, Any] | None = None,
        order_by: str | Sequence[str] | None = None,
        desc: bool = False,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[dict[str, Any]]:
        """SELECT строк по условию ``where``."""
        if isinstance(columns, str):
            columns = None if columns == "*" else [columns]
        elif columns is not None:
            try:
                columns = list(columns)
            except TypeError as exc:
                raise ValueError("columns должен быть строкой или списком строк") from exc

            if "*" in columns:
                columns = None

        if isinstance(order_by, str):
            order_by = [order_by]
        elif order_by is not None:
            try:
                order_by = list(order_by)
            except TypeError as exc:
                raise ValueError("order_by должен быть строкой или списком строк") from exc

        if columns:
            for col in columns:
                if not col:
                    raise ValueError("Имя колонки в columns не может быть пустым")

        if order_by:
            for col in order_by:
                if not col:
                    raise ValueError("Имя колонки в order_by не может быть пустым")

        select_cols = (
            sql.SQL(", ").join(map(sql.Identifier, columns))
            if columns
            else sql.SQL("*")
        )

        query = sql.SQL("SELECT {fields} FROM {table}").format(
            fields=select_cols,
            table=sql.Identifier(table),
        )

        params: list[Any] = []
        query, params = self._append_where(query, params, where)

        if order_by:
            direction = sql.SQL("DESC") if desc else sql.SQL("ASC")
            query += sql.SQL(" ORDER BY {cols} {direction}").format(
                cols=sql.SQL(", ").join(map(sql.Identifier, order_by)),
                direction=direction,
            )

        if limit is not None:
            limit = int(limit)
            if limit < 0:
                raise ValueError("limit не может быть отрицательным")
            query += sql.SQL(" LIMIT {limit}").format(limit=sql.Literal(limit))

        if offset is not None:
            offset = int(offset)
            if offset < 0:
                raise ValueError("offset не может быть отрицательным")
            query += sql.SQL(" OFFSET {offset}").format(offset=sql.Literal(offset))

        self._log_query(query, params)

        with self.cursor(commit=False) as cur:
            cur.execute(query, params)
            return [dict(row) for row in cur.fetchall()]

    def read_one(
        self,
        table: str,
        *,
        columns: Sequence[str] | str | None = None,
        where: Mapping[str, Any] | None = None,
        order_by: str | Sequence[str] | None = None,
        desc: bool = False,
    ) -> dict[str, Any] | None:
        """SELECT одной строки (LIMIT 1)."""
        rows = self.read(
            table,
            columns=columns,
            where=where,
            order_by=order_by,
            desc=desc,
            limit=1,
        )
        return rows[0] if rows else None

    def update(
        self,
        table: str,
        data: Mapping[str, Any],
        *,
        where: Mapping[str, Any],
        returning: str | Sequence[str] | None = "*",
    ) -> list[dict[str, Any]]:
        """UPDATE строк по where.

        ``where`` обязателен — защита от полного UPDATE.
        Если ``returning=None``, возвращает пустой список.
        """
        if not data:
            raise ValueError("data не может быть пустым")

        if not where:
            raise ValueError("where обязателен для update")

        set_cols = list(data.keys())
        for col in set_cols:
            if not col:
                raise ValueError("Имя колонки в data не может быть пустым")

        set_values = [data[col] for col in set_cols]

        query = sql.SQL("UPDATE {table} SET {assignments}").format(
            table=sql.Identifier(table),
            assignments=sql.SQL(", ").join(
                sql.SQL("{col} = {ph}").format(
                    col=sql.Identifier(col),
                    ph=sql.Placeholder(),
                )
                for col in set_cols
            ),
        )

        params: list[Any] = list(set_values)
        query, params = self._append_where(query, params, where)

        returning_sql = self._returning(returning)
        if returning_sql is not None:
            query += sql.SQL(" RETURNING {returning}").format(
                returning=returning_sql
            )

        self._log_query(query, params)

        with self.cursor() as cur:
            cur.execute(query, params)

            if returning_sql is None or cur.description is None:
                return []

            return [dict(row) for row in cur.fetchall()]

    def delete(
        self,
        table: str,
        *,
        where: Mapping[str, Any],
        returning: str | Sequence[str] | None = "*",
    ) -> list[dict[str, Any]]:
        """DELETE строк по where.

        ``where`` обязателен — защита от полного DELETE.
        Если ``returning=None``, возвращает пустой список.
        """
        if not where:
            raise ValueError("where обязателен для delete")

        query = sql.SQL("DELETE FROM {table}").format(table=sql.Identifier(table))

        params: list[Any] = []
        query, params = self._append_where(query, params, where)

        returning_sql = self._returning(returning)
        if returning_sql is not None:
            query += sql.SQL(" RETURNING {returning}").format(
                returning=returning_sql
            )

        self._log_query(query, params)

        with self.cursor() as cur:
            cur.execute(query, params)

            if returning_sql is None or cur.description is None:
                return []

            return [dict(row) for row in cur.fetchall()]

    def execute(
        self,
        query: str | sql.Composable,
        params: Sequence[Any] | Mapping[str, Any] | None = None,
        *,
        commit: bool = True,
    ) -> list[dict[str, Any]]:
        """Выполнить произвольный SQL и вернуть строки, если они есть.

        ``commit=False`` полезно для чистых ``SELECT`` вне транзакции.
        """
        self._log_query(query, params)

        with self.cursor(commit=commit) as cur:
            cur.execute(query, params)

            if cur.description is None:
                return []

            return [dict(row) for row in cur.fetchall()]

    def execute_many(
        self,
        query: str | sql.Composable,
        params_seq: Iterable[Sequence[Any] | Mapping[str, Any]],
        *,
        chunk_size: int | None = None,
        commit: bool = True,
    ) -> int:
        """Выполнить SQL для набора параметров.

        Возвращает число затронутых строк.

        ``RETURNING`` в ``executemany`` не поддерживается psycopg2.
        Если нужно вернуть строки — используйте :meth:`create_many`.
        """
        if chunk_size is not None and chunk_size <= 0:
            raise ValueError("chunk_size должен быть положительным")

        total = 0

        with self.cursor(commit=commit) as cur:
            if chunk_size is None:
                self._log_query(query, "<iterator>")
                cur.executemany(query, params_seq)

                if cur.rowcount and cur.rowcount > 0:
                    total = cur.rowcount
            else:
                buf: list[Any] = []

                for item in params_seq:
                    buf.append(item)

                    if len(buf) >= chunk_size:
                        self._log_query(query, f"<{len(buf)} rows>")
                        cur.executemany(query, buf)

                        if cur.rowcount and cur.rowcount > 0:
                            total += cur.rowcount

                        buf.clear()

                if buf:
                    self._log_query(query, f"<{len(buf)} rows>")
                    cur.executemany(query, buf)

                    if cur.rowcount and cur.rowcount > 0:
                        total += cur.rowcount

        return total

    # --- helpers ---

    @staticmethod
    def _returning(returning: str | Sequence[str] | None) -> sql.Composable | None:
        if returning is None:
            return None

        if isinstance(returning, str):
            if returning == "*":
                return sql.SQL("*")

            if not returning or "," in returning or " " in returning:
                raise ValueError(
                    "returning-строка должна содержать одно имя колонки или '*'. "
                    "Для нескольких колонок передайте список/кортеж."
                )

            return sql.Identifier(returning)

        try:
            returning_list = list(returning)
        except TypeError as exc:
            raise ValueError(
                "returning должен быть строкой, списком/кортежом строк или None"
            ) from exc

        if not returning_list:
            raise ValueError("returning не может быть пустым")

        parts: list[sql.Composable] = []

        for col in returning_list:
            if not isinstance(col, str):
                raise ValueError(f"Некорректное имя колонки в returning: {col!r}")

            if col == "*":
                parts.append(sql.SQL("*"))
            elif not col or "," in col or " " in col:
                raise ValueError(f"Некорректное имя колонки в returning: {col!r}")
            else:
                parts.append(sql.Identifier(col))

        return sql.SQL(", ").join(parts)

    @classmethod
    def _parse_col(cls, raw_col: str) -> tuple[str, str]:
        if not isinstance(raw_col, str) or not raw_col:
            raise ValueError(f"Некорректное имя колонки: {raw_col!r}")

        if "__" in raw_col:
            col, op = raw_col.rsplit("__", 1)
        else:
            col, op = raw_col, "eq"

        if not col:
            raise ValueError(f"Пустое имя колонки в {raw_col!r}")

        return col, op

    @staticmethod
    def _in_values(raw_col: str, value: Any) -> list[Any]:
        if value is None:
            raise ValueError(f"{raw_col}: значение не может быть None")

        if isinstance(value, (str, bytes, ABCMapping)):
            raise ValueError(
                f"{raw_col}: для in/notin передайте список/кортеж/множество, "
                "а не строку/байты/маппинг"
            )

        try:
            return list(value)
        except TypeError as exc:
            raise ValueError(
                f"{raw_col}: значение для in/notin должно быть итерируемым"
            ) from exc

    @classmethod
    def _append_where(
        cls,
        query: sql.Composable,
        params: list[Any],
        where: Mapping[str, Any] | None,
    ) -> tuple[sql.Composable, list[Any]]:
        if not where:
            return query, params

        if not isinstance(where, ABCMapping):
            raise ValueError("where должен быть маппингом")

        clauses: list[sql.Composable] = []

        for raw_col, value in where.items():
            col, op = cls._parse_col(raw_col)
            ident = sql.Identifier(col)

            if op == "in":
                seq = cls._in_values(raw_col, value)

                if not seq:
                    clauses.append(sql.SQL("FALSE"))
                    continue

                clauses.append(
                    sql.SQL("{c} IN ({ph})").format(
                        c=ident,
                        ph=sql.SQL(", ").join(sql.Placeholder() * len(seq)),
                    )
                )
                params.extend(seq)

            elif op == "notin":
                seq = cls._in_values(raw_col, value)

                if not seq:
                    clauses.append(sql.SQL("TRUE"))
                    continue

                clauses.append(
                    sql.SQL("{c} NOT IN ({ph})").format(
                        c=ident,
                        ph=sql.SQL(", ").join(sql.Placeholder() * len(seq)),
                    )
                )
                params.extend(seq)

            elif op == "isnull":
                if not isinstance(value, bool):
                    raise ValueError(
                        f"{raw_col}: для isnull нужно True/False, получено {value!r}"
                    )

                clauses.append(
                    sql.SQL("{c} IS NULL" if value else "{c} IS NOT NULL").format(
                        c=ident
                    )
                )

            elif op in cls._OPERATORS:
                if value is None:
                    if op == "eq":
                        clauses.append(sql.SQL("{c} IS NULL").format(c=ident))
                    elif op == "ne":
                        clauses.append(sql.SQL("{c} IS NOT NULL").format(c=ident))
                    else:
                        raise ValueError(f"{raw_col}: значение не может быть None")
                else:
                    clauses.append(
                        sql.SQL("{c} {op} {ph}").format(
                            c=ident,
                            op=sql.SQL(cls._OPERATORS[op]),
                            ph=sql.Placeholder(),
                        )
                    )
                    params.append(value)

            else:
                allowed = sorted(set(cls._OPERATORS) | {"in", "notin", "isnull"})
                raise ValueError(
                    f"Неизвестный оператор {op!r} в {raw_col!r}. "
                    f"Допустимые: {allowed}"
                )

        query += sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses)
        return query, params