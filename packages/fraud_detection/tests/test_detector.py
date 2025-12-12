"""Tests for the main fraud detector orchestrator."""

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

from fraud_detection.detector import DetectionConfig, FraudDetector


class TestDetectionConfig:
    """Tests for DetectionConfig dataclass."""

    def test_default_values(self) -> None:
        """Test that default configuration values are set correctly."""
        config = DetectionConfig()

        assert config.outlier_zscore_threshold == 3.0
        assert config.outlier_iqr_multiplier == 1.5
        assert config.duplicate_similarity_threshold == 0.9
        assert config.duplicate_time_window_days == 30
        assert config.max_provider_patient_distance_miles == 500.0
        assert config.max_daily_procedures_per_provider == 50
        assert config.max_claims_per_patient_per_day == 5
        assert config.weight_rule_violation == 0.3
        assert config.weight_statistical_anomaly == 0.25
        assert config.weight_duplicate == 0.45

    def test_custom_values(self) -> None:
        """Test that custom configuration values are applied."""
        config = DetectionConfig(
            outlier_zscore_threshold=2.5,
            duplicate_similarity_threshold=0.85,
            max_daily_procedures_per_provider=100,
        )

        assert config.outlier_zscore_threshold == 2.5
        assert config.duplicate_similarity_threshold == 0.85
        assert config.max_daily_procedures_per_provider == 100


class TestFraudDetector:
    """Tests for FraudDetector."""

    @pytest.fixture
    def config(self) -> DetectionConfig:
        """Create test configuration."""
        return DetectionConfig(
            max_daily_procedures_per_provider=50,
            max_claims_per_patient_per_day=5,
        )

    @pytest.fixture
    def detector(self, spark: SparkSession, config: DetectionConfig) -> FraudDetector:
        """Create FraudDetector instance."""
        return FraudDetector(spark, config)

    @pytest.fixture
    def detector_default_config(self, spark: SparkSession) -> FraudDetector:
        """Create FraudDetector with default config."""
        return FraudDetector(spark)

    @pytest.fixture
    def claims_schema_with_coords(self) -> StructType:
        """Schema with coordinate columns for geographic tests."""
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

    def test_detector_initialization(self, detector: FraudDetector, config: DetectionConfig) -> None:
        """Test that detector initializes all components correctly."""
        assert detector.config == config
        assert detector.billing_rules is not None
        assert detector.duplicate_detector is not None
        assert detector.geographic_rules is not None
        assert detector.outlier_detector is not None
        assert detector.benfords_analyzer is not None

    def test_detector_default_config(self, detector_default_config: FraudDetector) -> None:
        """Test that detector uses default config when none provided."""
        assert detector_default_config.config is not None
        assert detector_default_config.config.outlier_zscore_threshold == 3.0

    def test_detect_full_pipeline(
        self,
        detector: FraudDetector,
        spark: SparkSession,
        claims_schema_with_coords: StructType,
    ) -> None:
        """Test the complete fraud detection pipeline."""
        d = Decimal
        data = [
            # Normal claim
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
            # Potential duplicate
            (
                "CLM002",
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
            # High charge outlier
            (
                "CLM003",
                "PAT002",
                "PRV001",
                "99213",
                date(2024, 1, 16),
                d("5000.00"),
                "CA",
                "CA",
                34.05,
                -118.25,
                34.06,
                -118.26,
            ),
            # State mismatch
            (
                "CLM004",
                "PAT003",
                "PRV002",
                "99214",
                date(2024, 1, 17),
                d("150.00"),
                "NY",
                "CA",
                40.71,
                -74.01,
                34.06,
                -118.26,
            ),
            # More normal claims for statistical baseline
            (
                "CLM005",
                "PAT004",
                "PRV001",
                "99213",
                date(2024, 1, 18),
                d("110.00"),
                "CA",
                "CA",
                34.05,
                -118.25,
                34.06,
                -118.26,
            ),
            (
                "CLM006",
                "PAT005",
                "PRV001",
                "99213",
                date(2024, 1, 19),
                d("95.00"),
                "CA",
                "CA",
                34.05,
                -118.25,
                34.06,
                -118.26,
            ),
            (
                "CLM007",
                "PAT006",
                "PRV001",
                "99213",
                date(2024, 1, 20),
                d("105.00"),
                "CA",
                "CA",
                34.05,
                -118.25,
                34.06,
                -118.26,
            ),
        ]
        claims = spark.createDataFrame(data, claims_schema_with_coords)  # type: ignore[arg-type]

        result = detector.detect(claims)

        # Check output columns exist
        assert "fraud_score" in result.columns
        assert "fraud_reasons" in result.columns
        assert "rule_violations" in result.columns
        assert "statistical_flags" in result.columns
        assert "is_duplicate" in result.columns
        assert "duplicate_of" in result.columns
        assert "processed_at" in result.columns

        # Verify all claims are processed
        assert result.count() == 7

    def test_detect_with_rule_violations(
        self,
        detector: FraudDetector,
        spark: SparkSession,
    ) -> None:
        """Test detection catches rule violations."""
        # Create schema without coords
        schema = StructType(
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

        d = Decimal
        # Provider with 60 procedures on same day (exceeds limit of 50)
        data: list[tuple[str, str, str, str, date, Decimal, str, str]] = []
        for i in range(60):
            data.append(
                (
                    f"CLM{i:03d}",
                    f"PAT{i:03d}",
                    "PRV_HIGH",
                    "99213",
                    date(2024, 1, 15),
                    d("100.00"),
                    "CA",
                    "CA",
                )
            )

        claims = spark.createDataFrame(data, schema)  # type: ignore[arg-type]

        result = detector.detect(claims)

        # Check that fraud scores exist and some are > 0
        scores = result.select("fraud_score").collect()
        assert all(row["fraud_score"] is not None for row in scores)

        # High volume provider should have some fraud flags
        flagged = result.filter(result.fraud_score > 0).count()
        assert flagged > 0

    def test_apply_rules(
        self,
        detector: FraudDetector,
        spark: SparkSession,
    ) -> None:
        """Test _apply_rules method directly."""
        schema = StructType(
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

        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "NY"),
        ]
        claims = spark.createDataFrame(data, schema)  # type: ignore[arg-type]

        # pylint: disable=protected-access
        result = detector._apply_rules(claims)  # pyright: ignore[reportPrivateUsage]

        # Check that rule columns were added
        assert "daily_procedure_limit_exceeded" in result.columns
        assert "patient_frequency_exceeded" in result.columns
        assert "weekend_billing_flag" in result.columns
        assert "round_amount_flag" in result.columns
        assert "state_mismatch" in result.columns

    def test_apply_statistics(
        self,
        detector: FraudDetector,
        spark: SparkSession,
    ) -> None:
        """Test _apply_statistics method directly."""
        schema = StructType(
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

        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV001", "99213", date(2024, 1, 16), d("110.00"), "CA", "CA"),
            ("CLM003", "PAT003", "PRV001", "99213", date(2024, 1, 17), d("105.00"), "CA", "CA"),
            ("CLM004", "PAT004", "PRV001", "99213", date(2024, 1, 18), d("5000.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, schema)  # type: ignore[arg-type]

        # pylint: disable=protected-access
        result = detector._apply_statistics(claims)  # pyright: ignore[reportPrivateUsage]

        # Check that statistical columns were added
        assert "charge_zscore_outlier" in result.columns
        assert "charge_iqr_outlier" in result.columns
        assert "benfords_anomaly" in result.columns

    def test_detect_duplicates(
        self,
        detector: FraudDetector,
        spark: SparkSession,
    ) -> None:
        """Test _detect_duplicates method directly."""
        schema = StructType(
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

        d = Decimal
        data = [
            # Exact duplicates
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            # Different claim
            ("CLM003", "PAT002", "PRV001", "99214", date(2024, 1, 16), d("150.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, schema)  # type: ignore[arg-type]

        # pylint: disable=protected-access
        result = detector._detect_duplicates(claims)  # pyright: ignore[reportPrivateUsage]

        # Check that duplicate columns were added
        assert "is_duplicate" in result.columns
        assert "duplicate_of" in result.columns

    def test_calculate_fraud_score(
        self,
        detector: FraudDetector,
        spark: SparkSession,
    ) -> None:
        """Test _calculate_fraud_score method."""
        schema = StructType(
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

        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, schema)  # type: ignore[arg-type]

        # Run through full pipeline to get all columns
        result = detector.detect(claims)

        # Check final output columns
        output_cols = result.columns
        expected_cols = [
            "claim_id",
            "patient_id",
            "provider_id",
            "charge_amount",
            "fraud_score",
            "fraud_reasons",
            "rule_violations",
            "statistical_flags",
            "is_duplicate",
            "duplicate_of",
            "processed_at",
        ]
        for col in expected_cols:
            assert col in output_cols, f"Missing column: {col}"
