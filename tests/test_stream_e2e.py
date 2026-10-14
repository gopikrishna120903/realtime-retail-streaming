import json
from collections import Counter
from datetime import datetime, timedelta

from retail_streaming.producer import simulate
from retail_streaming.stream_job import start

START = datetime(2026, 1, 1, 9, 0)


def _expected_window_counts(landing):
    """Unique, on-time events per (5-minute window, store), computed independently of Spark."""
    seen, counts, late = set(), Counter(), 0
    for f in sorted(landing.iterdir()):
        batch_start = START + timedelta(minutes=5 * int(f.stem.split("_")[-1]))
        for line in f.read_text().splitlines():
            try:
                e = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(e.get("quantity"), int):
                continue
            ts = datetime.fromisoformat(e["event_ts"])
            if ts < batch_start:          # arrives far behind the watermark
                late += 1
                continue
            if e["event_id"] in seen:     # redelivery
                continue
            seen.add(e["event_id"])
            w = ts.replace(minute=ts.minute - ts.minute % 5, second=0)
            counts[(w, e["store_id"])] += 1
    return counts, late


def test_streaming_dedup_late_data_and_windows(spark, tmp_path):
    landing, lake = tmp_path / "landing", tmp_path / "lake"
    stats = simulate(str(landing), batches=8, events_per_batch=200, minutes_per_batch=5)

    raw = spark.readStream.option("maxFilesPerTrigger", 1).text(str(landing))
    for q in start(spark, raw, str(lake), available_now=True):
        q.awaitTermination()

    bronze = spark.read.parquet(str(lake / "bronze_pos_events"))
    dead = spark.read.parquet(str(lake / "dead_letter_pos_events"))
    gold = spark.read.parquet(str(lake / "gold_store_revenue_5min")).collect()

    assert dead.count() == stats["malformed"]
    assert bronze.count() == stats["events"] + stats["duplicates"] + stats["late"]  # raw audit trail

    expected, late = _expected_window_counts(landing)
    assert late == stats["late"] and gold, "early windows should be finalized by the watermark"
    for r in gold:   # every emitted window counts each event exactly once and ignores late arrivals
        assert r.events == expected[(r.window_start, r.store_id)], (r.window_start, r.store_id)
