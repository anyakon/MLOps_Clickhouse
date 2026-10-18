# HW2 — Kafka + Clickhouse: поток транзакций и оптимизация хранения

Домашнее задание №2. Датасет фродовых транзакций (Kaggle,
`train.csv`, 786 431 строк, 2019-01-01 … 2020-03-10) загружается из CSV
python-скриптом в топик Kafka; из топика табличным движком Kafka наполняется
таблица ClickHouse. Затем пишется SQL-запрос «категория крупнейшей транзакции
по каждому штату», результат выгружается в CSV, и хранение оптимизируется
(типизация, кодеки, партиционирование, порядок сортировки, проекция).

## Архитектура

```
data/train.csv
      │  python scripts/csv_to_kafka.py (pandas, JSONEachRow)
      ▼
Kafka (apache/kafka:3.9.0, KRaft)  ── топик transactions, 6 партиций
      │  движок Kafka + Materialized View
      ▼
ClickHouse (clickhouse-server:24.8)
      ├── hw2.transactions          — таблица п.2/п.4 (оптимизированный DDL)
      ├── hw2.transactions_baseline — таблица-базлайн для бенчмарка (sql/04)
      └── results/max_category_by_state.csv — ответ на запрос п.3
```

## Структура репозитория

| Путь | Назначение |
|---|---|
| `docker-compose.yml` | Kafka (KRaft) + создание топика + ClickHouse; схема поднимается автоматически при первом старте |
| `scripts/csv_to_kafka.py` | п.1 — продюсер CSV → Kafka |
| `sql/01_ddl.sql` | п.2 — базовый DDL (engine Kafka + MV + MergeTree-таблица «как есть») |
| `sql/02_ddl_optimized.sql` | п.4 — оптимизированный DDL (используется при автоматическом создании схемы) |
| `sql/03_max_category_by_state.sql` | п.3 — запрос «категория крупнейшей транзакции по штату» |
| `sql/04_benchmark.sql` | п.4 — воспроизводимый бенчмарк базовой vs оптимизированной таблицы |
| `results/max_category_by_state.csv` | результат запроса п.3 (51 штат + header) |
| `requirements.txt` | python-зависимости (pandas, kafka-python) |
| `data/train.csv` | датасет с Kaggle (не в git — добавьте сами) |

## Требования

- Docker Desktop с Compose v2 (`docker compose version` ≥ 2.x)
- Python 3.10+
- датасет: скачайте `train.csv` с Kaggle по ссылке из задания
  и положите в `data/train.csv`

Занятые порты: `9092` (Kafka), `8123` и `9000` (ClickHouse HTTP/native).

## Быстрый старт

Команды ниже — для PowerShell; в bash/linux замените `.\.venv\Scripts\Activate.ps1`
на `source .venv/bin/activate`.

### 1. Поднять стек

```powershell
docker compose up -d --wait
docker compose ps          # kafka, kafka-init (Exited 0), clickhouse — все healthy
```

При первом старте (пустой volume) ClickHouse сам применяет
`sql/02_ddl_optimized.sql` из `/docker-entrypoint-initdb.d` — схема
с проекцией создаётся автоматически.

### 2. Python-окружение

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 3. Загрузить данные в Kafka и ClickHouse

```powershell
python scripts/csv_to_kafka.py
# ожидаемо: отправлено 786,431 сообщений примерно за 2–3 мин (~5 000–6 000 msg/s)
```

ClickHouse читает топик непрерывно; дождитесь полного счётчика:

```powershell
Start-Sleep -Seconds 10
docker compose exec -T clickhouse clickhouse-client --query "SELECT count() FROM hw2.transactions"
# ожидаемо: 786431
```

Если счётчик меньше — подождите и проверьте ещё раз (потребление топика
идёт в фоне). При необходимости смотрите логи: `docker compose logs -f clickhouse`.
Файл с другим именем: `python scripts/csv_to_kafka.py --csv "data/train (1).csv"`.

### 4. Запрос п.3 и выгрузка результата в CSV

```powershell
docker compose exec -T clickhouse clickhouse-client --queries-file /sql/03_max_category_by_state.sql
docker compose exec -T clickhouse sh -c "clickhouse-client --queries-file /sql/03_max_category_by_state.sql --format CSVWithNames > /results/max_category_by_state.csv"
(Get-Content results\max_category_by_state.csv).Count   # ожидаемо: 52 (header + 51 штат)
```

Экспорт делается внутри контейнера: PowerShell-редирект `>` пишет UTF-16,
а `sh -c "... > /results/..."` даёт корректный UTF-8.

### 5. Бенчмарк оптимизации (п.4)

```powershell
docker compose exec -T clickhouse clickhouse-client --queries-file /sql/04_benchmark.sql
```

Скрипт создаёт таблицу-базлайн из `sql/01_ddl.sql`, копирует в неё данные,
приводит обе таблицы к слитому состоянию (`OPTIMIZE FINAL`), печатает размеры
и гоняет запрос п.3 попеременно 5 раз на каждой таблице, выводя сводку
из `system.query_log`. Если перезапускаете файл — выдержите паузу 2–3 минуты,
чтобы в сводку не попали прогоны предыдущего запуска.

Ориентировочные результаты (замеры на этом репозитории):

| Метрика | базовая (01) | оптимизированная (02) |
|---|---|---|
| min / avg / max, мс | 11 / 13.4 / 17 | 10 / 11.8 / 14 |
| память (peak) | 892 KiB | 487 KiB |
| прочитано строк | 786 431 | **751** (проекция) |
| on disk | 47 MiB | **26 MiB** |
| uncompressed | 122 MiB | **47 MiB** |

## Что оптимизировано и что это дало

Реализовано в `sql/02_ddl_optimized.sql` (и использовано в проде):

1. **`LowCardinality(String)`** для cat_id/gender/us_state/merch/one_city/street/
   post_code/jobs/name_1/name_2 — словарь вместо повторов строки
   (уникальных значений от 2 до 965 на колонку).
2. **CODEC**: `ZSTD` для строк, `DoubleDelta` для DateTime — сжатие 0.38 → 0.55,
   uncompressed 122 MiB → 47 MiB.
3. **`PARTITION BY toYYYYMM(transaction_time)`** — отбрасывание партиций по
   фильтру даты. Запрос п.3 с `WHERE transaction_time >= '2019-10-01'`:
   базовая — 1 part / 786 431 строк / 96 гранул,
   оптимизированная — 6 parts / 302 440 строк / 40 гранул (`EXPLAIN ESTIMATE`).
4. **`ORDER BY (us_state, amount)`** — порядок хранения под группировку/фильтры
   по штату и сумме.
5. **`PROJECTION p_state_max`** (агрегирующий: `us_state`, `max(amount)`,
   `argMax(...)`) — запрос п.3 целиком обслуживается проекцией:
   `EXPLAIN indexes=1` показывает `ReadFromMergeTree (p_state_max)`,
   прочитывается ~751 строка вместо 786 431. Это главный выигрыш: память
   вдвое меньше, чтение в тысячи раз меньше.

Честная оговорка по времени: на выборке 0.8 млн строк обе версии укладываются
в ~10–17 мс — запрос и так быстрый, а разница в wall-clock укладывается
в шум (агрегация идёт по хеш-таблице в памяти). Стабильные индикаторы
улучшения — `read_rows` (×1000), память (×2) и размер хранения (−45%);
выигрыш по времени растёт пропорционально объёму данных (проекция всегда
читает ~51 строк на партицию, базовая таблица — все строки).

## Пункты задания → файлы

| Пункт | Файл |
|---|---|
| 1. CSV → Kafka python-скриптом | `scripts/csv_to_kafka.py` |
| 2. DDL из топика Kafka (движок + MV + таблица) | `sql/01_ddl.sql`, `sql/02_ddl_optimized.sql` |
| 3. SQL-запрос + CSV-результат | `sql/03_max_category_by_state.sql`, `results/max_category_by_state.csv` |
| 4. Оптимизация хранения + обоснование | `sql/02_ddl_optimized.sql`, `sql/04_benchmark.sql`, раздел выше |

## Проверка работоспособности

```powershell
# все данные на месте
docker compose exec -T clickhouse clickhouse-client --query "SELECT count(), uniqExact(us_state) FROM hw2.transactions"
# ожидаемо: 786431  51

# запрос п.3 использует проекцию
docker compose exec -T clickhouse clickhouse-client --query "EXPLAIN indexes=1 SELECT us_state AS state, argMax(cat_id, (amount, transaction_time)) AS category_of_largest_transaction, max(amount) AS largest_transaction_amount, argMax(transaction_time, (amount, transaction_time)) AS largest_transaction_time FROM hw2.transactions GROUP BY us_state ORDER BY state" | Select-String "p_state_max"
# ожидаемо: ReadFromMergeTree (p_state_max)

# результат в CSV корректен (перекрёстная проверка pandas по train.csv)
python -c "import pandas as pd; df=pd.read_csv('data/train.csv', parse_dates=['transaction_time']); c=pd.read_csv('results/max_category_by_state.csv'); print(all(c.set_index('state').loc[s,'largest_transaction_amount']==df.loc[df[df.us_state==s]['amount'].idxmax(),'amount'] for s in c.state))"
# ожидаемо: True
```

## Остановка и очистка

```powershell
docker compose down       # остановить, данные сохранить
docker compose down -v    # остановить и удалить volumes (полный сброс: Kafka и ClickHouse схема с нуля)
```

После `down -v` порядок действий прежний: `up -d --wait` → загрузчик → проверка `count`.

## Возможные проблемы

- **Порт занят** (`9092`/`8123`/`9000`) — остановите другие контейнеры
  или поменяйте маппинг в `docker-compose.yml`.
- **Счётчик строк меньше 786 431** — consumption топика ещё идёт, подождите;
  `docker compose logs -f clickhouse` покажет ошибки парсинга, если формат
  сообщений неожиданный.
- **Бенчмарк 04 показывает меньше 5 прогонов или выбросы** — запускайте файл
  не чаще раза в 2–3 минуты (в сводке фильтруются последние 2 минуты лога),
  таблицы к замеру приводятся `OPTIMIZE FINAL` самим скриптом.
- **Китай/корейские символы в выводе PowerShell** — артефакт кодировки консоли;
  в самих файлах (`results/*.csv`, логах) текст корректный UTF-8.
- **Kafka падает с `advertised.listeners ... 0.0.0.0`** — если поднимаете
  Kafka самостоятельно: в KRaft combined mode в `advertised.listeners`
  должен быть объявлен и `CONTROLLER` (см. комментарии в
  `docker-compose.yml:20-23`).
- **Первый запуск продюсера до готовности Kafka** — скрипт ждёт брокер до 90 секунд
  (`--connect-timeout`), это нормально.
