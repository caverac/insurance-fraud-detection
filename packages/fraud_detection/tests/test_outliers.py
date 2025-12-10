"""Tests for outlier detection."""

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from fraud_detection.detector import DetectionConfig
from fraud_detection.statistics.outliers import OutlierDetector


class TestOutlierDetector:
    """Tests for OutlierDetector."""

    @pytest.fixture
    def detector(self, spark: SparkSession) -> OutlierDetector:
        """Create OutlierDetector instance."""
        config = DetectionConfig(
            outlier_zscore_threshold=2.0,  # Lower threshold for testing
            outlier_iqr_multiplier=1.5,
        )
        return OutlierDetector(spark, config)

    def test_zscore_outliers_detected(
        self,
        detector: OutlierDetector,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test Z-score outlier detection."""
        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV001", "99213", date(2024, 1, 16), d("110.00"), "CA", "CA"),
            ("CLM003", "PAT003", "PRV001", "99213", date(2024, 1, 17), d("105.00"), "CA", "CA"),
            ("CLM004", "PAT004", "PRV001", "99213", date(2024, 1, 18), d("95.00"), "CA", "CA"),
            ("CLM005", "PAT005", "PRV001", "99213", date(2024, 1, 19), d("102.00"), "CA", "CA"),
            ("CLM006", "PAT006", "PRV001", "99213", date(2024, 1, 20), d("5000.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = detector.detect_zscore_outliers(claims, "charge_amount", "is_outlier")

        outliers = result.filter(result.is_outlier == True).collect()  # noqa: E712

        # The $5000 claim should be detected as an outlier
        assert len(outliers) == 1
        assert outliers[0]["claim_id"] == "CLM006"

    def test_iqr_outliers_detected(
        self,
        detector: OutlierDetector,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test IQR outlier detection."""
        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV001", "99213", date(2024, 1, 16), d("110.00"), "CA", "CA"),
            ("CLM003", "PAT003", "PRV001", "99213", date(2024, 1, 17), d("105.00"), "CA", "CA"),
            ("CLM004", "PAT004", "PRV001", "99213", date(2024, 1, 18), d("95.00"), "CA", "CA"),
            ("CLM005", "PAT005", "PRV001", "99213", date(2024, 1, 19), d("102.00"), "CA", "CA"),
            ("CLM006", "PAT006", "PRV001", "99213", date(2024, 1, 20), d("5.00"), "CA", "CA"),
            ("CLM007", "PAT007", "PRV001", "99213", date(2024, 1, 21), d("5000.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = detector.detect_iqr_outliers(claims, "charge_amount", "is_outlier")

        outliers = result.filter(result.is_outlier == True).collect()  # noqa: E712
        outlier_ids = {r["claim_id"] for r in outliers}

        # Both low and high outliers should be detected
        assert "CLM006" in outlier_ids  # Low outlier
        assert "CLM007" in outlier_ids  # High outlier

    def test_grouped_zscore_outliers(
        self,
        detector: OutlierDetector,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test Z-score outliers by group (procedure code)."""
        d = Decimal
        data = [
            # Procedure 99213: normal range ~100 (need enough points for meaningful stddev)
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV001", "99213", date(2024, 1, 16), d("105.00"), "CA", "CA"),
            ("CLM003", "PAT003", "PRV001", "99213", date(2024, 1, 17), d("95.00"), "CA", "CA"),
            ("CLM008", "PAT008", "PRV001", "99213", date(2024, 1, 22), d("102.00"), "CA", "CA"),
            ("CLM009", "PAT009", "PRV001", "99213", date(2024, 1, 23), d("98.00"), "CA", "CA"),
            ("CLM010", "PAT010", "PRV001", "99213", date(2024, 1, 24), d("103.00"), "CA", "CA"),
            ("CLM004", "PAT004", "PRV001", "99213", date(2024, 1, 18), d("500.00"), "CA", "CA"),
            # Procedure 99215: normal range ~500
            ("CLM005", "PAT005", "PRV001", "99215", date(2024, 1, 19), d("500.00"), "CA", "CA"),
            ("CLM006", "PAT006", "PRV001", "99215", date(2024, 1, 20), d("520.00"), "CA", "CA"),
            ("CLM007", "PAT007", "PRV001", "99215", date(2024, 1, 21), d("480.00"), "CA", "CA"),
            ("CLM011", "PAT011", "PRV001", "99215", date(2024, 1, 25), d("510.00"), "CA", "CA"),
            ("CLM012", "PAT012", "PRV001", "99215", date(2024, 1, 26), d("490.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = detector.detect_zscore_outliers(claims, "charge_amount", "is_outlier", group_by=["procedure_code"])

        outliers = result.filter(result.is_outlier == True).collect()  # noqa: E712

        # Only CLM004 should be an outlier (500 is outlier for 99213, not for 99215)
        assert len(outliers) == 1
        assert outliers[0]["claim_id"] == "CLM004"

    def test_no_outliers_in_uniform_data(
        self,
        detector: OutlierDetector,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test that uniform data produces no outliers."""
        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV001", "99213", date(2024, 1, 16), d("100.00"), "CA", "CA"),
            ("CLM003", "PAT003", "PRV001", "99213", date(2024, 1, 17), d("100.00"), "CA", "CA"),
            ("CLM004", "PAT004", "PRV001", "99213", date(2024, 1, 18), d("100.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = detector.detect_zscore_outliers(claims, "charge_amount", "is_outlier")

        outliers = result.filter(result.is_outlier == True).count()  # noqa: E712

        # No outliers in perfectly uniform data
        assert outliers == 0
