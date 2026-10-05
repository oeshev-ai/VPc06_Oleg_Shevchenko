"""Демонстрация работы с БД `test` через PostgresDriver.

Все параметры подключения берутся из .env:
    PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
"""

from __future__ import annotations

import logging

import psycopg2

from postgres_driver import PostgresDriver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# --- DDL --------------------------------------------------------------------
DDL_USERS = """
CREATE TABLE IF NOT EXISTS users (
    id   SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    age  INT CHECK (age >= 0)
);
"""

DDL_ORDERS = """
CREATE TABLE IF NOT EXISTS orders (
    id         SERIAL PRIMARY KEY,
    user_id    INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    amount     NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
"""

# --- Агрегирующий запрос -----------------------------------------------------
TOTALS_SQL = """
SELECT u.name,
       COALESCE(SUM(o.amount), 0) AS total_amount
FROM users u
LEFT JOIN orders o ON o.user_id = u.id
GROUP BY u.id, u.name
ORDER BY total_amount DESC;
"""


def print_totals(db: PostgresDriver, title: str) -> None:
    print(f"\n=== {title} ===")
    rows = db.execute(TOTALS_SQL, commit=False)
    if not rows:
        print("(пусто)")
        return
    for row in rows:
        print(f"{row['name']} — {float(row['total_amount']):.2f}")


def main() -> None:
    # Параметры подключения НЕ передаём — драйвер возьмёт их из .env
    db = PostgresDriver()

    try:
        db.connect()

        info = db.ping()
        print(f"[OK] Подключено: {info.get('dbname')} / {info.get('user')}")

        db.execute("DROP TABLE IF EXISTS orders;")
        db.execute("DROP TABLE IF EXISTS users;")

        # 1. Таблицы
        db.execute(DDL_USERS)
        db.execute(DDL_ORDERS)
        print("[OK] Таблицы users и orders готовы.")

        # 2. Пользователи
        alice = db.create("users", {"name": "Alice", "age": 28})
        bob   = db.create("users", {"name": "Bob",   "age": 35})
        carol = db.create("users", {"name": "Carol", "age": 22})
        assert alice and bob and carol
        print(
            f"[OK] Пользователи: "
            f"Alice={alice['id']}, Bob={bob['id']}, Carol={carol['id']}"
        )

        # 3. Заказы одной транзакцией (Alice×2, Bob×1, Carol — без заказов)
        with db.transaction() as cur:
            cur.execute(
                "INSERT INTO orders (user_id, amount) VALUES (%s, %s)",
                (alice["id"], 499.90),
            )
            cur.execute(
                "INSERT INTO orders (user_id, amount) VALUES (%s, %s)",
                (alice["id"], 750.10),
            )
            cur.execute(
                "INSERT INTO orders (user_id, amount) VALUES (%s, %s)",
                (bob["id"], 1230.00),
            )
        print("[OK] Заказы добавлены: Alice×2, Bob×1, Carol×0.")

        # 4. Агрегат: LEFT JOIN + SUM + GROUP BY + ORDER BY
        print_totals(db, "Сумма заказов по пользователям")

        # 5. Проверка ON DELETE CASCADE
        print("\n=== Проверка ON DELETE CASCADE ===")
        before = db.execute(
            "SELECT COUNT(*) AS cnt FROM orders WHERE user_id = %s",
            (alice["id"],),
            commit=False,
        )[0]["cnt"]
        print(f"Заказов у Alice до удаления: {before}")

        db.delete("users", where={"id": alice["id"]})

        after = db.execute(
            "SELECT COUNT(*) AS cnt FROM orders WHERE user_id = %s",
            (alice["id"],),
            commit=False,
        )[0]["cnt"]
        print(f"Заказов у Alice после удаления: {after}  (ожидаем 0)")

        # 6. Итог — Alice уже удалена, Carol с нулём
        print_totals(db, "Итоговый отчёт")

    except psycopg2.Error as e:
        print(f"[DB ERROR] {e}")
    except Exception as e:
        print(f"[ERROR] {type(e).__name__}: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    main()