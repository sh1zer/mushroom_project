import argparse
import csv
import json
import random
import time
import uuid

from kafka import KafkaProducer

from mushroom import config as C


def load_rows() -> list:
    with open(C.SECONDARY_CSV, newline="", encoding="utf-8") as f:
        return [{k.replace("-", "_"): v for k, v in row.items()} for row in csv.DictReader(f, delimiter=";")]


def to_event(row: dict) -> dict:
    event = {"event_id": str(uuid.uuid4()), "true_class": row["class"]}
    event.update({c: float(row[c]) for c in C.NUMERIC_FEATURES})
    event.update({c: row[c] or None for c in C.CATEGORICAL_FEATURES})
    return event


def main() -> None:
    parser = argparse.ArgumentParser(description="Wysyła losowe rekordy z secondary_data.csv do Kafki")
    parser.add_argument("--rate", type=float, default=5.0, help="zdarzeń na sekundę")
    parser.add_argument("--limit", type=int, default=1000)
    args = parser.parse_args()

    rows = load_rows()
    producer = KafkaProducer(bootstrap_servers=C.KAFKA_BOOTSTRAP)
    for i in range(args.limit):
        producer.send(C.TOPIC_OBSERVATIONS, json.dumps(to_event(random.choice(rows))).encode("utf-8"))
        if (i + 1) % 100 == 0:
            print(f"sent {i + 1}")
        time.sleep(1 / args.rate)
    producer.flush()


if __name__ == "__main__":
    main()
