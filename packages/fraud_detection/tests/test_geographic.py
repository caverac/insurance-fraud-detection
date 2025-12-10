"""Tests for geographic anomaly detection rules."""

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from fraud_detection.detector import DetectionConfig
from fraud_detection.rules.geographic import GeographicRules


class TestGeographicRules:
    """Tests for GeographicRules."""

    @pytest.fixture
    def config(self) -> DetectionConfig:
        """Create test configuration."""
        return DetectionConfig(max_provider_patient_distance_miles=100.0)

    @pytest.fixture
    def rules(self, spark: SparkSession, config: DetectionConfig) -> GeographicRules:
        """Create GeographicRules instance."""
        return GeographicRules(spark, config)

    @pytest.fixture
    def claims_schema_with_coords(self) -> StructType:
        """Schema with coordinate columns."""
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
                StructField("patient_lat", DoubleType(), True),
                StructField("patient_lon", DoubleType(), True),
                StructField("provider_lat", DoubleType(), True),
                StructField("provider_lon", DoubleType(), True),
            ]
        )

    @pytest.fixture
    def claims_schema_basic(self) -> StructType:
        """Basic schema without coordinates."""
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

    def test_check_provider_patient_distance_with_coords(
        self,
        rules: GeographicRules,
        spark: SparkSession,
        claims_schema_with_coords: StructType,
    ) -> None:
        """Test distance check with coordinates."""
        d = Decimal
        data = [
            # Close distance (same area in LA)
            (
                "CLM001",
                "PAT001",
                "PRV001",
                "99213",
                date(2024, 1, 15),
                d("100.00"),
                "CA",
                "CA",
                34.05,
                -118.25,
                34.06,
                -118.26,
            ),
            # Far distance (LA to NY - ~2450 miles)
            (
                "CLM002",
                "PAT002",
                "PRV002",
                "99213",
                date(2024, 1, 16),
                d("100.00"),
                "NY",
                "CA",
                40.71,
                -74.01,
                34.05,
                -118.25,
            ),
        ]
        claims = spark.createDataFrame(data, claims_schema_with_coords)  # type: ignore[arg-type]

        result = rules.check_provider_patient_distance(claims)

        # Check columns exist
        assert "distance_miles" in result.columns
        assert "distance_exceeded" in result.columns

        # Close claim should not exceed
        close = result.filter(result.claim_id == "CLM001").first()
        assert close
        assert close["distance_exceeded"] is False

        # Far claim should exceed
        far = result.filter(result.claim_id == "CLM002").first()
        assert far
        assert far["distance_exceeded"] is True
        assert far["distance_miles"] > 2000  # LA to NY is ~2450 miles

    def test_check_provider_patient_distance_without_coords(
        self,
        rules: GeographicRules,
        spark: SparkSession,
        claims_schema_basic: StructType,
    ) -> None:
        """Test distance check without coordinates falls back gracefully."""
        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV002", "99213", date(2024, 1, 16), d("100.00"), "NY", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema_basic)  # type: ignore[arg-type]

        result = rules.check_provider_patient_distance(claims)

        # distance_exceeded should be False for all when no coordinates
        assert "distance_exceeded" in result.columns
        assert result.filter(result.distance_exceeded).count() == 0

    def test_check_state_mismatch(
        self,
        rules: GeographicRules,
        spark: SparkSession,
        claims_schema_basic: StructType,
    ) -> None:
        """Test state mismatch detection."""
        d = Decimal
        data: list[tuple[str, str, str, str, date, Decimal, str | None, str | None]] = [
            # Same state
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            # Different states
            ("CLM002", "PAT002", "PRV002", "99213", date(2024, 1, 16), d("100.00"), "NY", "CA"),
            # Null patient state
            ("CLM003", "PAT003", "PRV003", "99213", date(2024, 1, 17), d("100.00"), None, "CA"),
            # Null provider state
            ("CLM004", "PAT004", "PRV004", "99213", date(2024, 1, 18), d("100.00"), "CA", None),
        ]
        claims = spark.createDataFrame(data, claims_schema_basic)  # type: ignore[arg-type]

        result = rules.check_state_mismatch(claims)

        assert "state_mismatch" in result.columns

        # Same state - no mismatch
        same = result.filter(result.claim_id == "CLM001").first()
        assert same
        assert same["state_mismatch"] is False

        # Different states - mismatch
        different = result.filter(result.claim_id == "CLM002").first()
        assert different
        assert different["state_mismatch"] is True

        # Null states - no mismatch (null handling)
        null_patient = result.filter(result.claim_id == "CLM003").first()
        assert null_patient
        assert null_patient["state_mismatch"] is False

        null_provider = result.filter(result.claim_id == "CLM004").first()
        assert null_provider
        assert null_provider["state_mismatch"] is False

    def test_check_state_mismatch_no_state_columns(
        self,
        rules: GeographicRules,
        spark: SparkSession,
    ) -> None:
        """Test state mismatch when state columns are missing."""
        schema = StructType(
            [
                StructField("claim_id", StringType(), False),
                StructField("patient_id", StringType(), False),
                StructField("provider_id", StringType(), False),
                StructField("procedure_code", StringType(), False),
                StructField("service_date", DateType(), False),
                StructField("charge_amount", DecimalType(10, 2), False),
            ]
        )

        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00")),
        ]
        claims = spark.createDataFrame(data, schema)  # type: ignore[arg-type]

        result = rules.check_state_mismatch(claims)

        assert "state_mismatch" in result.columns
        assert result.filter(result.state_mismatch).count() == 0

    def test_check_geographic_clustering(
        self,
        rules: GeographicRules,
        spark: SparkSession,
        claims_schema_basic: StructType,
    ) -> None:
        """Test geographic clustering detection."""
        d = Decimal
        data: list[tuple[str, str, str, str, date, Decimal, str, str]] = []

        # Provider with 150 patients all from NY but provider in CA (suspicious)
        for i in range(150):
            data.append(
                (
                    f"CLM_CLUST{i:03d}",
                    f"PAT_CLUST{i:03d}",
                    "PRV_CLUSTERED",
                    "99213",
                    date(2024, 1, 15),
                    d("100.00"),
                    "NY",
                    "CA",
                )
            )

        # Normal provider with patients from multiple states
        for i, state in enumerate(["CA", "CA", "CA", "NV", "AZ", "OR", "WA", "CA", "CA", "CA"]):
            data.append(
                (
                    f"CLM_NORM{i:03d}",
                    f"PAT_NORM{i:03d}",
                    "PRV_NORMAL",
                    "99213",
                    date(2024, 1, 15),
                    d("100.00"),
                    state,
                    "CA",
                )
            )

        claims = spark.createDataFrame(data, claims_schema_basic)  # type: ignore[arg-type]

        result = rules.check_geographic_clustering(claims)

        assert "unique_patient_states" in result.columns
        assert "total_patients" in result.columns
        assert "geographic_clustering_flag" in result.columns

        # Clustered provider should be flagged (all patients from single state != provider state)
        clustered = result.filter(result.provider_id == "PRV_CLUSTERED").first()
        assert clustered
        assert clustered["geographic_clustering_flag"] is True
        assert clustered["unique_patient_states"] == 1
        assert clustered["total_patients"] == 150

        # Normal provider should not be flagged (multiple patient states)
        normal = result.filter(result.provider_id == "PRV_NORMAL").first()
        assert normal
        assert normal["geographic_clustering_flag"] is False

    def test_check_impossible_travel_with_coords(
        self,
        rules: GeographicRules,
        spark: SparkSession,
        claims_schema_with_coords: StructType,
    ) -> None:
        """Test impossible travel detection with coordinates."""
        d = Decimal
        data = [
            # Patient with 4 different provider locations on same day (impossible)
            (
                "CLM001",
                "PAT_TRAVEL",
                "PRV001",
                "99213",
                date(2024, 1, 15),
                d("100.00"),
                "CA",
                "CA",
                34.05,
                -118.25,
                34.06,
                -118.26,
            ),
            (
                "CLM002",
                "PAT_TRAVEL",
                "PRV002",
                "99213",
                date(2024, 1, 15),
                d("100.00"),
                "CA",
                "NY",
                34.05,
                -118.25,
                40.71,
                -74.01,
            ),
            (
                "CLM003",
                "PAT_TRAVEL",
                "PRV003",
                "99213",
                date(2024, 1, 15),
                d("100.00"),
                "CA",
                "TX",
                34.05,
                -118.25,
                29.76,
                -95.37,
            ),
            (
                "CLM004",
                "PAT_TRAVEL",
                "PRV004",
                "99213",
                date(2024, 1, 15),
                d("100.00"),
                "CA",
                "FL",
                34.05,
                -118.25,
                25.76,
                -80.19,
            ),
            # Normal patient with one provider
            (
                "CLM005",
                "PAT_NORMAL",
                "PRV001",
                "99213",
                date(2024, 1, 15),
                d("100.00"),
                "CA",
                "CA",
                34.05,
                -118.25,
                34.06,
                -118.26,
            ),
        ]
        claims = spark.createDataFrame(data, claims_schema_with_coords)  # type: ignore[arg-type]

        result = rules.check_impossible_travel(claims)

        assert "impossible_travel_flag" in result.columns
        assert "num_providers_same_day" in result.columns
        assert "provider_locations" in result.columns

        # Patient with impossible travel should be flagged
        travel_claims = result.filter(result.patient_id == "PAT_TRAVEL").collect()
        for claim in travel_claims:
            assert claim["impossible_travel_flag"] is True
            assert claim["num_providers_same_day"] == 4

        # Normal patient should not be flagged
        normal = result.filter(result.patient_id == "PAT_NORMAL").first()
        assert normal
        assert normal["impossible_travel_flag"] is False

    def test_check_impossible_travel_without_coords(
        self,
        rules: GeographicRules,
        spark: SparkSession,
        claims_schema_basic: StructType,
    ) -> None:
        """Test impossible travel without coordinates falls back gracefully."""
        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT001", "PRV002", "99213", date(2024, 1, 15), d("100.00"), "CA", "NY"),
        ]
        claims = spark.createDataFrame(data, claims_schema_basic)  # type: ignore[arg-type]

        result = rules.check_impossible_travel(claims)

        assert "impossible_travel_flag" in result.columns
        assert result.filter(result.impossible_travel_flag).count() == 0

    def test_haversine_distance_calculation(
        self,
        rules: GeographicRules,
        spark: SparkSession,
        claims_schema_with_coords: StructType,
    ) -> None:
        """Test haversine distance calculation accuracy."""
        d = Decimal
        # Known distance: LA (34.05, -118.25) to SF (37.77, -122.42) is ~347 miles
        data = [
            (
                "CLM001",
                "PAT001",
                "PRV001",
                "99213",
                date(2024, 1, 15),
                d("100.00"),
                "CA",
                "CA",
                34.05,
                -118.25,
                37.77,
                -122.42,
            ),
        ]
        claims = spark.createDataFrame(data, claims_schema_with_coords)  # type: ignore[arg-type]

        result = rules.check_provider_patient_distance(claims)

        row = result.first()
        # Allow 10% tolerance for earth radius approximations
        assert row
        assert 310 < row["distance_miles"] < 380
