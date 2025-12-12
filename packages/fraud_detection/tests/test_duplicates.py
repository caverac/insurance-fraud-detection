"""Tests for duplicate detection."""

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from fraud_detection.detector import DetectionConfig
from fraud_detection.rules.duplicates import DuplicateDetector


class TestDuplicateDetector:
    """Tests for DuplicateDetector."""

    @pytest.fixture
    def detector(self, spark: SparkSession) -> DuplicateDetector:
        """Create DuplicateDetector instance."""
        config = DetectionConfig(
            duplicate_similarity_threshold=0.9,
            duplicate_time_window_days=30,
        )
        return DuplicateDetector(spark, config)

    def test_exact_duplicates_detected(
        self,
        detector: DuplicateDetector,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test detection of exact duplicate claims."""
        d = Decimal
        data = [
            # Exact duplicates
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            # Unique claim
            ("CLM003", "PAT002", "PRV001", "99214", date(2024, 1, 16), d("150.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = detector.detect(claims)

        # One of the duplicates should be flagged
        duplicates = result.filter(result.is_duplicate).collect()

        assert len(duplicates) == 1
        assert duplicates[0]["claim_id"] == "CLM002"  # Second claim is the duplicate
        assert duplicates[0]["duplicate_of"] == "CLM001"

    def test_near_duplicates_detected(
        self,
        detector: DuplicateDetector,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test detection of near-duplicate claims."""
        d = Decimal
        data = [
            # Near duplicate - same patient/provider, similar procedure, slightly different amount
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT001", "PRV001", "99213", date(2024, 1, 16), d("105.00"), "CA", "CA"),
            # Different patient - not a duplicate
            ("CLM003", "PAT002", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = detector.detect(claims)

        # Check if near-duplicate is detected
        near_dupes = result.filter((result.is_duplicate) & (result.claim_id == "CLM002")).collect()

        # Near duplicate should be detected
        assert len(near_dupes) == 1

    def test_no_duplicates_in_unique_claims(
        self,
        detector: DuplicateDetector,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test that unique claims are not flagged as duplicates."""
        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV002", "99214", date(2024, 2, 20), d("150.00"), "NY", "NY"),
            ("CLM003", "PAT003", "PRV003", "99215", date(2024, 3, 25), d("200.00"), "TX", "TX"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = detector.detect(claims)

        duplicates = result.filter(result.is_duplicate).count()

        assert duplicates == 0

    def test_multiple_duplicate_groups(
        self,
        detector: DuplicateDetector,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test detection of multiple separate duplicate groups."""
        d = Decimal
        data = [
            # First duplicate group
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM003", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            # Second duplicate group
            ("CLM004", "PAT002", "PRV002", "99214", date(2024, 2, 20), d("150.00"), "NY", "NY"),
            ("CLM005", "PAT002", "PRV002", "99214", date(2024, 2, 20), d("150.00"), "NY", "NY"),
            # Unique claim
            ("CLM006", "PAT003", "PRV003", "99215", date(2024, 3, 25), d("200.00"), "TX", "TX"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = detector.detect(claims)

        # Should detect 3 duplicates (2 from first group, 1 from second)
        duplicates = result.filter(result.is_duplicate).count()

        assert duplicates == 3

    def test_outside_time_window_not_duplicate(
        self,
        detector: DuplicateDetector,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test that claims outside time window are not flagged as near-duplicates."""
        d = Decimal
        data = [
            # Same patient/provider but 60 days apart (outside 30-day window)
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT001", "PRV001", "99213", date(2024, 3, 15), d("105.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = detector.detect(claims)

        duplicates = result.filter(result.is_duplicate).count()

        # Should not be flagged as duplicates (outside time window)
        assert duplicates == 0
