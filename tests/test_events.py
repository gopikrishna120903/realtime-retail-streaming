import json

from retail_streaming.events import parse


def test_parse_routes_bad_events_to_dead_letter(spark):
    good = {"event_id": "e1", "event_type": "SALE", "store_id": "S001", "register_id": "R1", "sku": "SKU1",
            "quantity": 2, "unit_price": 10.5, "event_ts": "2026-01-01T09:00:00"}
    ret = dict(good, event_id="e2", event_type="RETURN", quantity=1)
    raw = spark.createDataFrame([(json.dumps(good),), (json.dumps(ret),), ("not json",),
                                 (json.dumps(dict(good, event_id="e3", quantity=0)),)], "value string")
    valid, dead = parse(raw)
    rows = {r.event_id: r for r in valid.collect()}
    assert set(rows) == {"e1", "e2"}
    assert rows["e1"].line_amount == 21.0 and rows["e2"].line_amount == -10.5
    assert dead.count() == 2


def test_dead_letter_records_why_each_event_was_rejected(spark):
    good = {"event_id": "e1", "event_type": "SALE", "store_id": "S001", "register_id": "R1", "sku": "SKU1",
            "quantity": 2, "unit_price": 10.5, "event_ts": "2026-01-01T09:00:00"}
    bad = {
        "unparseable_json": "not json",
        "missing_store_id": json.dumps(dict(good, store_id=None)),
        "unknown_event_type": json.dumps(dict(good, event_type="REFUND")),
        "non_positive_quantity": json.dumps(dict(good, quantity=-3)),
        "negative_unit_price": json.dumps(dict(good, unit_price=-1.0)),
        "bad_timestamp": json.dumps(dict(good, event_ts="yesterday")),
    }
    raw = spark.createDataFrame([(v,) for v in bad.values()], "value string")
    _, dead = parse(raw)
    assert {r.raw_json: r.reason for r in dead.collect()} == {v: k for k, v in bad.items()}