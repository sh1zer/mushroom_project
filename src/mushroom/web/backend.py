import json
import threading
import time
import uuid
from collections import Counter, deque
from functools import lru_cache
from typing import Optional

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from kafka import KafkaConsumer, KafkaProducer
from pydantic import create_model

from mushroom import config as C

latest = deque(maxlen=50)
totals = Counter()

Observation = create_model(
    "Observation",
    **{c: (float, ...) for c in C.NUMERIC_FEATURES},
    **{c: (Optional[str], None) for c in C.CATEGORICAL_FEATURES},
)


def consume() -> None:
    while True:
        try:
            consumer = KafkaConsumer(C.TOPIC_PREDICTIONS, bootstrap_servers=C.KAFKA_BOOTSTRAP)
            for message in consumer:
                p = json.loads(message.value)
                latest.appendleft(p)
                totals["events"] += 1
                totals[p["decision"]] += 1
                if p.get("true_class") == "p" and p["decision"] != "NIE_JESC":
                    totals["missed_poisonous"] += 1
        except Exception as exc:
            print(f"Kafka consumer error, retrying in 5 s: {exc}")
            time.sleep(5)


@lru_cache(maxsize=1)
def producer() -> KafkaProducer:
    return KafkaProducer(bootstrap_servers=C.KAFKA_BOOTSTRAP)


app = FastAPI(title="Mushroom Big Data API")
threading.Thread(target=consume, daemon=True).start()


@app.get("/api/schema")
def schema() -> dict:
    return {
        "numeric": {c: C.FEATURE_LABELS[c] for c in C.NUMERIC_FEATURES},
        "categorical": {c: {"label": C.FEATURE_LABELS[c], "options": C.CODEBOOK[c]} for c in C.CATEGORICAL_FEATURES},
    }


@app.get("/api/predictions")
def predictions() -> dict:
    return {"totals": dict(totals), "latest": list(latest)}


@app.post("/api/observations")
def submit(observation: Observation) -> dict:
    event = {"event_id": str(uuid.uuid4()), "true_class": None, **observation.model_dump()}
    producer().send(C.TOPIC_OBSERVATIONS, json.dumps(event).encode("utf-8")).get(timeout=10)
    return {"event_id": event["event_id"]}


app.mount("/", StaticFiles(directory=C.PROJECT_ROOT / "frontend", html=True))
