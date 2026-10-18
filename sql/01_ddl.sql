-- =============================================================================
-- П.2 задания: базовый DDL - таблицы ClickHouse, получающие данные из топика Kafka.
--
-- Поток данных:  CSV -> (python) -> Kafka topic "transactions"
--                          -> таблица Kafka engine -> MV -> таблица MergeTree.
--
-- Формат сообщения в топике - JSONEachRow (по одному JSON-объекту на строку CSV).
-- Базовый вариант: без партиционирования, ORDER BY tuple() (данные лежат
-- в порядке загрузки), без компрессии и без LowCardinality - используется
-- в п.2, для сравнения с оптимизированной версией (sql/02_ddl_optimized.sql).
-- =============================================================================

CREATE DATABASE IF NOT EXISTS hw2;

-- Итоговая таблица (стейдж) -----------------------------------------------
CREATE TABLE IF NOT EXISTS hw2.transactions
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

-- Ворота из Kafka -----------------------------------------------------------
-- Кликхаус сам поднимает consumers и кладёт данные в MV.
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
    kafka_broker_list      = 'kafka:19092',
    kafka_topic_list       = 'transactions',
    kafka_group_name       = 'clickhouse_hw2',
    kafka_format           = 'JSONEachRow',
    kafka_num_consumers    = 1,
    kafka_skip_broken_messages = 100;

-- MV: Kafka -> итоговая таблица --------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS hw2.mv_transactions
TO hw2.transactions
AS
SELECT *
FROM hw2.kafka_transactions;
