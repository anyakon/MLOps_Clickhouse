-- 1) ORDER BY (us_state, amount) вместо tuple().
--    Первичный ключ совпадает с полями GROUP BY/ORDER BY запроса п.3:
--    строки одного штата лежат рядом (локальность чтения), при фильтрации
--    по us_state и amount работает первичный индекс (пропуск гранул),
--    а при включённом optimize_aggregation_in_order агрегация идёт по
--    уже отсортированному потоку - без построения хеш-таблицы GROUP BY.

-- 2) PARTITION BY toYYYYMM(transaction_time).
--    Данные разбиты по месяцам (15 партиций 2019-01..2020-03): запросы с
--    фильтром по дате не сканируют лишние месяцы, мерж-фонды меньше,
--    старые месяцы можно отбрасывать одной командой ALTER ... DROP PARTITION
--    (или TTL), не трогая остальную историю.
--
-- 3) LowCardinality(String) для 10 колонок с < 1000 уникальных значений
--    (cat_id - 14, gender - 2, us_state - 51, merch - 693 и т.д.).

-- 4) CODEC(ZSTD)/CODEC(DoubleDelta, ZSTD).
--    DateTime хранится дельтами + zstd, строки - zstd 

-- 5) kafka_num_consumers = 6 (по числу партиций топика)
--    и kafka_max_block_size = 262144 - данные пишутся более крупными блоками,
--    в таблице появляется меньше мелких частей
--
-- 6) PROJECTION p_state_max - готовый
--    агрегат (argMax по (amount, transaction_time) + max(amount) в разрезе
--    us_state), который обновляется автоматически при каждой вставке из
--    кафки.

CREATE DATABASE IF NOT EXISTS hw2;

CREATE TABLE IF NOT EXISTS hw2.transactions
(
    transaction_time DateTime                  CODEC(DoubleDelta, ZSTD),
    merch             LowCardinality(String)   CODEC(ZSTD),
    cat_id            LowCardinality(String)   CODEC(ZSTD),
    amount            Float64                  CODEC(ZSTD),
    name_1            LowCardinality(String)   CODEC(ZSTD),
    name_2            LowCardinality(String)   CODEC(ZSTD),
    gender            LowCardinality(String)   CODEC(ZSTD),
    street            LowCardinality(String)   CODEC(ZSTD),
    one_city          LowCardinality(String)   CODEC(ZSTD),
    us_state          LowCardinality(String)   CODEC(ZSTD),
    post_code         LowCardinality(String)   CODEC(ZSTD),
    lat               Float64                  CODEC(ZSTD),
    lon               Float64                  CODEC(ZSTD),
    population_city   UInt32                   CODEC(ZSTD),
    jobs              LowCardinality(String)   CODEC(ZSTD),
    merchant_lat      Float64                  CODEC(ZSTD),
    merchant_lon      Float64                  CODEC(ZSTD),
    target            UInt8                    CODEC(ZSTD),
    PROJECTION p_state_max
    (
        SELECT
            us_state,
            argMax(cat_id, (amount, transaction_time))          AS max_category,
            max(amount)                                         AS max_amount,
            argMax(transaction_time, (amount, transaction_time)) AS max_time
        GROUP BY us_state
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(transaction_time)
ORDER BY (us_state, amount)
SETTINGS
    index_granularity = 8192;

CREATE TABLE IF NOT EXISTS hw2.kafka_transactions
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
ENGINE = Kafka
SETTINGS
    kafka_broker_list          = 'kafka:19092',
    kafka_topic_list           = 'transactions',
    kafka_group_name           = 'clickhouse_hw2',
    kafka_format               = 'JSONEachRow',
    kafka_num_consumers        = 6,
    kafka_max_block_size       = 262144,
    kafka_skip_broken_messages = 100;

CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.mv_transactions
TO hw2.transactions
AS
SELECT *
FROM hw2.kafka_transactions;
