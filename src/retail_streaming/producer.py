"""Simulated POS event producer.

Writes newline-delimited JSON files (one file per micro-batch) for local runs, or publishes to
Kafka / Azure Event Hubs (Kafka endpoint) with --kafka. Injects duplicates, late and malformed events.
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

SKUS = {f"SKU{i:04d}": round(random.Random(i).uniform(2, 120), 2) for i in range(1, 201)}


def make_batch(rng: random.Random, start: datetime, minutes: int, n: int) -> List[dict]:
    events = []
    for _ in range(n):
        sku = rng.choice(list(SKUS))
        events.append({
            "event_id": str(uuid.UUID(int=rng.getrandbits(128))),
            "event_type": rng.choices(["SALE", "RETURN"], [0.95, 0.05])[0],
            "store_id": f"S{rng.randint(1, 12):03d}",
            "register_id": f"R{rng.randint(1, 8)}",
            "sku": sku,
            "quantity": rng.randint(1, 4),
            "unit_price": SKUS[sku],
            "event_ts": (start + timedelta(seconds=rng.randint(0, minutes * 60 - 1))).isoformat(),
        })
    return events


def simulate(out_dir: str, batches: int = 8, events_per_batch: int = 500, minutes_per_batch: int = 5,
             seed: int = 3, start: datetime = datetime(2026, 1, 1, 9, 0)) -> dict:
    rng = random.Random(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    stats = {"events": 0, "duplicates": 0, "late": 0, "malformed": 0}
    for b in range(batches):
        batch_start = start + timedelta(minutes=b * minutes_per_batch)
        events = make_batch(rng, batch_start, minutes_per_batch, events_per_batch)
        dupes = rng.sample(events, k=events_per_batch // 50)                  # at-least-once redelivery
        lines = [json.dumps(e) for e in events + dupes]
        if b >= 5:                                                           # very late event (> watermark)
            late = make_batch(rng, start, 1, 1)[0]
            lines.append(json.dumps(late))
            stats["late"] += 1
        lines += ['{"event_id": "bad-1", "store_id": "S001", "quantity": "two"}', "not json at all"]
        stats["events"] += len(events)
        stats["duplicates"] += len(dupes)
        stats["malformed"] += 2
        (out / f"pos_batch_{b:04d}.json").write_text("\n".join(lines) + "\n")
    return stats


def publish_kafka(bootstrap: str, topic: str, rate_per_sec: int) -> None:  # pragma: no cover - needs a broker
    from kafka import KafkaProducer

    producer = KafkaProducer(bootstrap_servers=bootstrap, value_serializer=lambda v: json.dumps(v).encode())
    rng = random.Random()
    while True:
        for e in make_batch(rng, datetime.utcnow() - timedelta(seconds=5), 1, rate_per_sec):
            producer.send(topic, e)
        producer.flush()
        time.sleep(1)


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--out", default="data/landing/pos")
    p.add_argument("--kafka", help="bootstrap servers, e.g. localhost:9092")
    p.add_argument("--topic", default="pos-events")
    p.add_argument("--rate", type=int, default=50)
    a = p.parse_args()
    if a.kafka:
        publish_kafka(a.kafka, a.topic, a.rate)
    else:
        print(simulate(a.out))
