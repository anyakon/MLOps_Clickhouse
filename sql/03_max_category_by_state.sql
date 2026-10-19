-- П.3: категория наибольшей транзакции по каждому штату

-- argMax(cat_id, (amount, transaction_time)) - cat_id той строки, у которой
-- максимум amount; при равных amount применяется таймстемп как тай-брейк,
-- чтобы результат был детерминированным.
--
-- Экспорт в CSV (из корня репозитория, Windows PowerShell / bash):
--   docker compose exec -T clickhouse sh -c \
--     "clickhouse-client --queries-file /sql/03_max_category_by_state.sql \
--      --format CSVWithNames > /results/max_category_by_state.csv"

SELECT
    us_state                                        AS state,
    argMax(cat_id, (amount, transaction_time))      AS category_of_largest_transaction,
    max(amount)                                     AS largest_transaction_amount,
    argMax(transaction_time, (amount, transaction_time)) AS largest_transaction_time
FROM hw2.transactions
GROUP BY us_state
ORDER BY state;
