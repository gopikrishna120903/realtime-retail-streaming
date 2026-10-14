"""Spark session helper that works locally (with or without Delta) and on Databricks."""
import os

from pyspark.sql import SparkSession


def delta_enabled() -> bool:
    """Delta is always on in Databricks; locally it can be switched off with USE_DELTA=0."""
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return True
    return os.getenv("USE_DELTA", "1") == "1"


def table_format() -> str:
    return "delta" if delta_enabled() else "parquet"


def get_spark(app_name: str = "retail-streaming") -> SparkSession:
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return SparkSession.builder.getOrCreate()

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    if delta_enabled():
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        builder = configure_spark_with_delta_pip(builder)
    return builder.getOrCreate()
