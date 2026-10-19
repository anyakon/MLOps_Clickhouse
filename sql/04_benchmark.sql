-- 1) Создаёт таблицу с DDL из sql/01_ddl.sql (без LowCardinality,
--    PARTITION BY, ORDER BY, проекции) и копирует в неё те же данные.
-- 2) Сравнивает размеры хранения и сжатие (system.parts).
-- 3) Гоняет запрос п.3 попеременно 5 раз на каждой из таблиц и выводит
--    агрегированную статистику из system.query_log (длительность, память,
--    число прочитанных строк - для таблицы с проекцией оно кратно меньше).
--    Повторный запуск файла: выдержите паузу 2-3 минуты, чтобы в отчёт
--    не попали прогоны предыдущего запуска.
--
-- Запуск (из корня репозитория):
--   docker compose exec -T clickhouse clickhouse-client --queries-file /sql/04_benchmark.sql
-- =============================================================================

DROP TABLE IF EXISTS hw2.transactions_basic;

CREATE TABLE IF NOT EXISTS hw2.transactions_basic
(
    transaction_time DateTime,
    merch             String,
    cat_id            String,
    amount            Float64,
    name_1            String,
    name_2            String,
    gender            String,
    street            String,
    one_city          String,
    us_state          String,
    post_code         String,
    lat               Float64,
    lon               Float64,
    population_city   UInt32,
    jobs              String,
    merchant_lat      Float64,
    merchant_lon      Float64,
    target            UInt8
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO hw2.transactions_basic
SELECT * FROM hw2.transactions;

OPTIMIZE TABLE hw2.transactions FINAL;
OPTIMIZE TABLE hw2.transactions_basic FINAL;

SELECT
    table,
    sum(rows)                                        AS rows,
    formatReadableSize(sum(bytes_on_disk))           AS on_disk,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed,
    round(sum(data_compressed_bytes) / sum(data_uncompressed_bytes), 3) AS compression_ratio
FROM system.parts
WHERE database = 'hw2'
  AND table IN ('transactions', 'transactions_basic')
  AND active
GROUP BY table
ORDER BY table
FORMAT Vertical;

SELECT
    table,
    partition_key,
    sorting_key
FROM system.tables
WHERE database = 'hw2' AND table IN ('transactions', 'transactions_basic')
ORDER BY table
FORMAT Vertical;

SELECT 'bench_basic' AS tag, count() AS states FROM (
    SELECT us_state, argMax(cat_id, (amount, transaction_time)) AS c,
           max(amount) AS m, argMax(transaction_time, (amount, transaction_time)) AS tt
    FROM hw2.transactions_basic GROUP BY us_state
);
SELECT 'bench_optimized' AS tag, count() AS states FROM (
    SELECT us_state, argMax(cat_id, (amount, transaction_time)) AS c,
           max(amount) AS m, argMax(transaction_time, (amount, transaction_time)) AS tt
    FROM hw2.transactions GROUP BY us_state
);
SELECT 'bench_basic' AS tag, count() AS states FROM (
    SELECT us_state, argMax(cat_id, (amount, transaction_time)) AS c,
           max(amount) AS m, argMax(transaction_time, (amount, transaction_time)) AS tt
    FROM hw2.transactions_basic GROUP BY us_state
);
SELECT 'bench_optimized' AS tag, count() AS states FROM (
    SELECT us_state, argMax(cat_id, (amount, transaction_time)) AS c,
           max(amount) AS m, argMax(transaction_time, (amount, transaction_time)) AS tt
    FROM hw2.transactions GROUP BY us_state
);
SELECT 'bench_basic' AS tag, count() AS states FROM (
    SELECT us_state, argMax(cat_id, (amount, transaction_time)) AS c,
           max(amount) AS m, argMax(transaction_time, (amount, transaction_time)) AS tt
    FROM hw2.transactions_basic GROUP BY us_state
);
SELECT 'bench_optimized' AS tag, count() AS states FROM (
    SELECT us_state, argMax(cat_id, (amount, transaction_time)) AS c,
           max(amount) AS m, argMax(transaction_time, (amount, transaction_time)) AS tt
    FROM hw2.transactions GROUP BY us_state
);
SELECT 'bench_basic' AS tag, count() AS states FROM (
    SELECT us_state, argMax(cat_id, (amount, transaction_time)) AS c,
           max(amount) AS m, argMax(transaction_time, (amount, transaction_time)) AS tt
    FROM hw2.transactions_basic GROUP BY us_state
);
SELECT 'bench_optimized' AS tag, count() AS states FROM (
    SELECT us_state, argMax(cat_id, (amount, transaction_time)) AS c,
           max(amount) AS m, argMax(transaction_time, (amount, transaction_time)) AS tt
    FROM hw2.transactions GROUP BY us_state
);
SELECT 'bench_basic' AS tag, count() AS states FROM (
    SELECT us_state, argMax(cat_id, (amount, transaction_time)) AS c,
           max(amount) AS m, argMax(transaction_time, (amount, transaction_time)) AS tt
    FROM hw2.transactions_basic GROUP BY us_state
);
SELECT 'bench_optimized' AS tag, count() AS states FROM (
    SELECT us_state, argMax(cat_id, (amount, transaction_time)) AS c,
           max(amount) AS m, argMax(transaction_time, (amount, transaction_time)) AS tt
    FROM hw2.transactions GROUP BY us_state
);

SYSTEM FLUSH LOGS;

SELECT
    if(position(query, 'bench_basic') > 0,
       '1) basic (sql/01_ddl.sql): ORDER BY tuple, без LowCardinality и проекции',
       '2) optimized (sql/02_ddl_optimized.sql): LowCardinality + ORDER BY + ZSTD + PARTITION BY + PROJECTION') AS version,
    count()                                          AS runs,
    min(query_duration_ms)                           AS min_ms,
    round(avg(query_duration_ms), 1)                 AS avg_ms,
    max(query_duration_ms)                           AS max_ms,
    formatReadableSize(max(memory_usage))            AS peak_memory,
    max(read_rows)                                   AS read_rows
FROM system.query_log
WHERE type = 'QueryFinish'
  AND (position(query, 'bench_basic') > 0 OR position(query, 'bench_optimized') > 0)
  AND position(query, 'Итоговая сводка') = 0     
  AND event_time > now() - 120
GROUP BY version
ORDER BY version
FORMAT Vertical;
