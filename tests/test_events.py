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
