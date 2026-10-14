import os

import pytest

os.environ.setdefault("USE_DELTA", "0")  # unit tests run on plain Spark + Parquet


@pytest.fixture(scope="session")
def spark():
    from retail_streaming.spark import get_spark

    s = get_spark("tests")
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
