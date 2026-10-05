# VPc06 — PostgreSQL Driver

Переиспользуемый драйвер `PostgresDriver` для работы с PostgreSQL: подключение через `.env` и CRUD-методы.

## Структура

| Файл | Назначение |
|------|------------|
| `postgres_driver.py` | Драйвер для внешних проектов |
| `main.py` | Демонстрация работы драйвера |
| `requirements.txt` | Зависимости Python |
| `.env.example` | Шаблон переменных окружения |

## Быстрый старт

### 1. Окружение Python

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Конфиг

```bash
copy .env.example .env
```

Укажите параметры своей PostgreSQL:

```env
PGHOST=localhost
PGPORT=5432
PGDATABASE=test
PGUSER=postgres
PGPASSWORD=your_password
```

Драйвер читает те же переменные, что и `python-dotenv` / окружение процесса. При необходимости параметры можно передать в конструктор (`host`, `port`, `dbname`, `user`, `password`) или через `connect_kwargs` (`sslmode`, `connect_timeout` и т.д.).

### 3. Запуск демо

```bash
python main.py
```

## main.py

Скрипт показывает сценарий работы с `PostgresDriver` на БД из `.env`. Параметры подключения в код не передаются — их берёт драйвер.

Порядок выполнения:

1. `connect()` — открытие соединения
2. `ping()` — проверка соединения
3. `DROP TABLE IF EXISTS` для `orders` и `users` — чистый старт
4. Создание таблиц `users` и `orders` (`CREATE TABLE IF NOT EXISTS`; у `orders` — FK на `users` с `ON DELETE CASCADE`)
5. `create` — добавление пользователей Alice, Bob, Carol
6. `transaction` — вставка заказов одной транзакцией (Alice×2, Bob×1, Carol без заказов)
7. Агрегат через `execute`: `LEFT JOIN` + `SUM` + `GROUP BY` + `ORDER BY`
8. `delete` пользователя Alice и проверка каскадного удаления заказов
9. Итоговый отчёт
10. `close()` — закрытие соединения

## Использование драйвера

Скопируйте `postgres_driver.py` в свой проект и поставьте зависимости из `requirements.txt`.

```python
from postgres_driver import PostgresDriver

with PostgresDriver() as db:
    db.create("users", {"name": "Alice", "age": 28})
    users = db.read("users", where={"age__gte": 18})
    db.update("users", {"age": 29}, where={"name": "Alice"})
    db.delete("users", where={"id": 1})
```

Либо явно, как в `main.py`:

```python
db = PostgresDriver()
db.connect()
try:
    ...
finally:
    db.close()
```

### Основные методы

| Метод | Описание |
|-------|----------|
| `connect()` / `close()` | Открыть / закрыть соединение |
| `ping()` | Проверка соединения |
| `create(table, data)` | INSERT одной строки |
| `create_many(table, rows)` | INSERT нескольких строк |
| `read(table, where=...)` | SELECT |
| `read_one(table, where=...)` | SELECT одной строки |
| `update(table, data, where=...)` | UPDATE |
| `delete(table, where=...)` | DELETE |
| `execute(sql, params, commit=True)` | Произвольный SQL |
| `execute_many(sql, params_seq)` | SQL для набора параметров |
| `transaction()` | Явная транзакция |
| `commit()` / `rollback()` | Управление транзакцией |

### Операторы в `where`

```python
{"id": 1}                     # =
{"age__gte": 18}              # >=
{"name__ilike": "%ali%"}      # ILIKE
{"id__in": [1, 2, 3]}         # IN
{"id__notin": [4, 5]}         # NOT IN
{"deleted_at__isnull": True}  # IS NULL
{"email": None}               # IS NULL
```

Поддерживаются: `eq`, `ne`, `lt`, `lte`, `gt`, `gte`, `like`, `ilike`, `in`, `notin`, `isnull`.

`update` и `delete` требуют непустой `where` — защита от массовых операций.
