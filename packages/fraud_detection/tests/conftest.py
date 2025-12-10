"""Pytest fixtures for fraud detection tests."""

from datetime import date
from decimal import Decimal
from typing import Generator

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Create a Spark session for testing."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("FraudDetectionTests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def claims_schema() -> StructType:
    """Create standard claims schema for testing."""
    return StructType(
        [
            StructField("claim_id", StringType(), False),
            StructField("patient_id", StringType(), False),
            StructField("provider_id", StringType(), False),
            StructField("procedure_code", StringType(), False),
            StructField("service_date", DateType(), False),
            StructField("charge_amount", DecimalType(10, 2), False),
            StructField("patient_state", StringType(), True),
            StructField("provider_state", StringType(), True),
        ]
    )


@pytest.fixture
def sample_claims(request: pytest.FixtureRequest) -> DataFrame:
    """Create sample claims data for testing."""
    spark_session: SparkSession = request.getfixturevalue("spark")
    schema: StructType = request.getfixturevalue("claims_schema")
    d = Decimal
    data = [
        ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
        ("CLM002", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
        ("CLM003", "PAT002", "PRV001", "99214", date(2024, 1, 16), d("150.00"), "CA", "CA"),
        ("CLM004", "PAT003", "PRV002", "99215", date(2024, 1, 17), d("5000.00"), "NY", "NY"),
        ("CLM005", "PAT004", "PRV002", "99213", date(2024, 1, 18), d("100.00"), "TX", "CA"),
        ("CLM006", "PAT005", "PRV003", "97110", date(2024, 1, 20), d("75.00"), "FL", "FL"),
        ("CLM007", "PAT006", "PRV003", "97110", date(2024, 1, 21), d("1000.00"), "FL", "FL"),
    ]
    return spark_session.createDataFrame(data, schema)  # type: ignore[arg-type]


@pytest.fixture
def high_volume_provider_claims(request: pytest.FixtureRequest) -> DataFrame:
    """Create claims data with a provider exceeding daily limits."""
    spark_session: SparkSession = request.getfixturevalue("spark")
    schema: StructType = request.getfixturevalue("claims_schema")
    d = Decimal
    data: list[tuple[str, str, str, str, date, Decimal, str, str]] = []
    # Provider with 60 procedures on same day (exceeds limit of 50)
    for i in range(60):
        data.append((f"CLM{i:03d}", f"PAT{i:03d}", "PRV_HIGH", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"))
    # Normal provider
    for i in range(10):
        data.append((f"CLMN{i:03d}", f"PATN{i:03d}", "PRV_NORMAL", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"))
    return spark_session.createDataFrame(data, schema)  # type: ignore[arg-type]


@pytest.fixture
def weekend_claims(request: pytest.FixtureRequest) -> DataFrame:
    """Create claims with weekend billing patterns."""
    spark_session: SparkSession = request.getfixturevalue("spark")
    schema: StructType = request.getfixturevalue("claims_schema")
    d = Decimal
    data = [
        # Provider with high weekend billing (Saturday/Sunday)
        ("CLM001", "PAT001", "PRV_WEEKEND", "99213", date(2024, 1, 13), d("100.00"), "CA", "CA"),
        ("CLM002", "PAT002", "PRV_WEEKEND", "99213", date(2024, 1, 14), d("100.00"), "CA", "CA"),
        ("CLM003", "PAT003", "PRV_WEEKEND", "99213", date(2024, 1, 20), d("100.00"), "CA", "CA"),
        ("CLM004", "PAT004", "PRV_WEEKEND", "99213", date(2024, 1, 21), d("100.00"), "CA", "CA"),
        # Normal provider (weekdays only)
        ("CLM005", "PAT005", "PRV_NORMAL", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
        ("CLM006", "PAT006", "PRV_NORMAL", "99213", date(2024, 1, 16), d("100.00"), "CA", "CA"),
    ]
    return spark_session.createDataFrame(data, schema)  # type: ignore[arg-type]
