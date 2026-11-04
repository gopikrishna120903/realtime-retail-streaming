"""Event schema and parsing: valid events continue, malformed ones go to a dead-letter table."""
from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

POS_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),     # SALE | RETURN
    StructField("store_id", StringType()),
    StructField("register_id", StringType()),
    StructField("sku", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("event_ts", StringType()),
])

def is_valid() -> F.Column:
    return (
        F.col("event_id").isNotNull()
        & F.col("store_id").isNotNull()
        & F.col("event_type").isin("SALE", "RETURN")
        & (F.col("quantity") > 0)
        & (F.col("unit_price") >= 0)
        & F.col("event_time").isNotNull()
    )


def rejection_reason() -> F.Column:
    """The first rule an event breaks, so the dead-letter table says why it was rejected."""
    return (
        F.when(F.col("event_id").isNull() & F.col("event_type").isNull() & F.col("store_id").isNull(),
               "unparseable_json")
        .when(F.col("event_id").isNull(), "missing_event_id")
        .when(F.col("store_id").isNull(), "missing_store_id")
        .when(F.col("event_type").isNull() | ~F.col("event_type").isin("SALE", "RETURN"), "unknown_event_type")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "non_positive_quantity")
        .when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "negative_unit_price")
        .when(F.col("event_time").isNull(), "bad_timestamp")
        .otherwise("unknown")
    )


def parse(raw: DataFrame, value_col: str = "value") -> Tuple[DataFrame, DataFrame]:
    """`raw` has a string/binary JSON column (Kafka `value` or a text file `value`)."""
    parsed = (
        raw.select(F.col(value_col).cast("string").alias("raw_json"))
        .withColumn("e", F.from_json("raw_json", POS_EVENT_SCHEMA))
        .select("raw_json", "e.*")
        .withColumn("event_time", F.to_timestamp("event_ts"))
        .withColumn("signed_qty", F.when(F.col("event_type") == "RETURN", -F.col("quantity")).otherwise(F.col("quantity")))
        .withColumn("line_amount", F.round(F.col("signed_qty") * F.col("unit_price"), 2))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    valid = parsed.filter(is_valid()).drop("raw_json", "event_ts")
    dead = parsed.filter(~F.coalesce(is_valid(), F.lit(False))).select(
               "raw_json", rejection_reason().alias("reason"), "_ingested_at")
    return valid, dead


def store_revenue_windows(events: DataFrame, window: str = "5 minutes", watermark: str = "10 minutes") -> DataFrame:
    """Deduplicate replayed events and aggregate revenue per store per tumbling window.

    The watermark bounds state: duplicates are tracked and windows kept open only for `watermark`,
    and events arriving later than that are dropped instead of reopening finalized windows.
    """
    return (
        events.withWatermark("event_time", watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
        .groupBy(F.window("event_time", window).alias("w"), "store_id")
        .agg(
            F.round(F.sum("line_amount"), 2).alias("net_revenue"),
            F.sum("signed_qty").alias("net_units"),
            F.count("*").alias("events"),
            F.sum(F.when(F.col("event_type") == "RETURN", 1).otherwise(0)).alias("returns"),
        )
        .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"),
                "store_id", "net_revenue", "net_units", "events", "returns")
    )
