import pytest


@pytest.fixture(scope="session")
def spark_session():
    """Create or get existing SparkSession for tests."""
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder \
            .master("local[*]") \
            .appName("HappyBooking_Tests") \
            .getOrCreate()
    except ImportError:
        pytest.skip("PySpark not installed — skipping Spark-dependent tests")
