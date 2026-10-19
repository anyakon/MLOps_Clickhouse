import argparse
import json
import os
import sys
import time

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

REQUIRED_COLUMNS = ("transaction_time", "merch", "cat_id", "amount", "us_state")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CSV -> Kafka loader")
    parser.add_argument("--csv", default="data/train.csv", help="путь к CSV-файлу датасета")
    parser.add_argument("--topic", default="transactions", help="имя топика Kafka")
    parser.add_argument(
        "--bootstrap",
        default="localhost:9092",
        help="bootstrap-server Kafka (через запятую для нескольких брокеров)",
    )
    parser.add_argument("--chunksize", type=int, default=50_000, help="строк в чанке pandas")
    parser.add_argument("--batch-size", type=int, default=512 * 1024, help="kafka batch size, байт")
    parser.add_argument("--linger-ms", type=int, default=100, help="kafka linger_ms")
    parser.add_argument("--limit", type=int, default=0, help="ограничить число сообщений (0 - все)")
    parser.add_argument(
        "--connect-timeout", type=int, default=90, help="сколько секунд ждать доступности Kafka"
    )
    return parser.parse_args()


def json_default(value):
    """Сериализация numpy-скаляров, которые pandas отдаёт в to_dict()."""
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def connect_producer(args: argparse.Namespace) -> KafkaProducer:
    """Подключение к Kafka с повторами (Kafka может подниматься дольше скрипта)."""
    deadline = time.monotonic() + args.connect_timeout
    attempt = 0
    while True:
        attempt += 1
        try:
            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap.split(","),
                acks=1,
                retries=5,
                batch_size=args.batch_size,
                linger_ms=args.linger_ms,
                compression_type="gzip",
                client_id="hw2-csv-loader",
            )
            print(f"[kafka] подключение установлено: {args.bootstrap}")
            return producer
        except NoBrokersAvailable as exc:
            if time.monotonic() >= deadline:
                sys.exit(f"[kafka] не удалось подключиться за {args.connect_timeout} с: {exc}")
            if attempt == 1:
                print(f"[kafka] ждём брокер {args.bootstrap} ...")
            time.sleep(2)


def iter_messages(path: str, columns, chunksize: int, limit: int):
    """Читает CSV чанками и отдаёт JSON-сообщения (bytes) построчно."""
    sent = 0
    for chunk in pd.read_csv(path, chunksize=chunksize):
        if "transaction_time" in chunk.columns:
            # В датасете время с точностью до минуты: "2019-04-17 23:09".
            # Отправляем в ClickHouse полноформатную строку DateTime.
            chunk["transaction_time"] = pd.to_datetime(
                chunk["transaction_time"], format="mixed"
            ).dt.strftime("%Y-%m-%d %H:%M:%S")
        chunk = chunk[columns]
        # NaN -> null (ClickHouse положит значение по умолчанию в NOT NULL колонку)
        chunk = chunk.where(pd.notna(chunk), None)
        for record in chunk.to_dict("records"):
            if limit and sent >= limit:
                return
            yield json.dumps(
                record, ensure_ascii=False, separators=(",", ":"), default=json_default
            ).encode("utf-8")
            sent += 1


def main() -> None:
    args = parse_args()

    if not os.path.isfile(args.csv):
        sys.exit(
            f"[csv] файл не найден: {args.csv}\n"
            "Скачайте датасет на Kaggle и положите train.csv в папку data/"
        )

    columns = list(pd.read_csv(args.csv, nrows=0).columns)
    missing = [c for c in REQUIRED_COLUMNS if c not in columns]
    if missing:
        sys.exit(f"[csv] в файле нет обязательных колонок: {', '.join(missing)}")

    print(f"[csv] файл:        {args.csv}")
    print(f"[csv] колонки:     {', '.join(columns)}")
    print(f"[csv] топик:       {args.topic}")

    producer = connect_producer(args)

    started = time.monotonic()
    sent = 0
    next_report = 100_000
    try:
        for message in iter_messages(args.csv, columns, args.chunksize, args.limit):
            producer.send(args.topic, value=message)
            sent += 1
            if sent >= next_report:
                producer.flush(timeout=60)
                elapsed = time.monotonic() - started
                print(f"[kafka] отправлено {sent:>9,} сообщений  ({sent / elapsed:,.0f} msg/s)")
                next_report += 100_000
        producer.flush(timeout=120)
    except KeyboardInterrupt:
        print("\n[!] прерывание пользователем")
    finally:
        producer.close()

    elapsed = time.monotonic() - started
    print(f"[done] отправлено {sent:,} сообщений в '{args.topic}' за {elapsed:,.1f} с "
          f"({sent / max(elapsed, 1e-9):,.0f} msg/s)")
    print("[done] дождитесь полной загрузки в ClickHouse и проверьте:")
    print("       docker compose exec -T clickhouse clickhouse-client "
          "--query \"SELECT count() FROM hw2.transactions\"")


if __name__ == "__main__":
    main()
