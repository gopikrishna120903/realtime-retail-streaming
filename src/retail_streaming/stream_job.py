"""Structured Streaming job: POS events -> Bronze (all valid events) + Dead letter + Gold (store revenue windows).

Local (file source):
    python -m retail_streaming.stream_job --source files --input data/landing/pos --lake data/lake --available-now
Kafka / Event Hubs:
    python -m retail_streaming.stream_job --source kafka --bootstrap <ns>.servicebus.windows.net:9093 --topic pos-events
"""
import argparse
import os
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from .events import parse, store_revenue_windows
from .spark import get_spark, table_format


def kafka_options(bootstrap: str, topic: str) -> Dict[str, str]:
    opts = {"kafka.bootstrap.servers": bootstrap, "subscribe": topic, "startingOffsets": "latest",
            "maxOffsetsPerTrigger": "50000", "failOnDataLoss": "false"}
    conn = os.getenv("EVENTHUBS_CONNECTION_STRING")   # injected from Key Vault on Databricks
    if conn:  # Azure Event Hubs exposes a Kafka-compatible endpoint
        opts.update({
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.sasl.jaas.config": (
                "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="$ConnectionString" password="{conn}";'
            ),
        })
    return opts


def read_stream(spark: SparkSession, source: str, input_path: str = "", bootstrap: str = "",
                topic: str = "") -> DataFrame:
    if source == "kafka":
        return spark.readStream.format("kafka").options(**kafka_options(bootstrap, topic)).load()
    return spark.readStream.option("maxFilesPerTrigger", 1).text(input_path)


def start(spark: SparkSession, raw: DataFrame, lake: str, available_now: bool = False) -> List[StreamingQuery]:
    valid, dead = parse(raw)
    fmt = table_format()
    ckpt = os.path.join(lake, "_checkpoints")

    def sink(df: DataFrame, name: str, mode: str = "append") -> StreamingQuery:
        w = (df.writeStream.format(fmt).outputMode(mode).queryName(name)
             .option("checkpointLocation", os.path.join(ckpt, name)))
        w = w.trigger(availableNow=True) if available_now else w.trigger(processingTime="30 seconds")
        return w.start(os.path.join(lake, name))

    return [
        sink(valid, "bronze_pos_events"),
        sink(dead, "dead_letter_pos_events"),
        sink(store_revenue_windows(valid), "gold_store_revenue_5min"),
    ]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["files", "kafka"], default="files")
    p.add_argument("--input", default="data/landing/pos")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="pos-events")
    p.add_argument("--lake", default="data/lake")
    p.add_argument("--available-now", action="store_true", help="process what is there, then stop")
    a = p.parse_args()

    spark = get_spark("retail-streaming")
    spark.sparkContext.setLogLevel("WARN")
    queries = start(spark, read_stream(spark, a.source, a.input, a.bootstrap, a.topic), a.lake, a.available_now)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
