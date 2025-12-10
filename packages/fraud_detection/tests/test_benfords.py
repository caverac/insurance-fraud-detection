"""Tests for Benford's Law analysis."""

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from fraud_detection.statistics.benfords import BenfordsLawAnalyzer


class TestBenfordsLawAnalyzer:
    """Tests for BenfordsLawAnalyzer."""

    @pytest.fixture
    def analyzer(self, spark: SparkSession) -> BenfordsLawAnalyzer:
        """Create BenfordsLawAnalyzer instance."""
        return BenfordsLawAnalyzer(spark)

    @pytest.fixture
    def claims_schema(self) -> StructType:
        """Standard claims schema for testing."""
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

    def test_benfords_expected_values(self, analyzer: BenfordsLawAnalyzer) -> None:
        """Test that expected Benford's distribution values are correct."""
        expected = analyzer.BENFORDS_EXPECTED
        assert len(expected) == 9
        assert expected[1] == 0.301
        assert expected[9] == 0.046
        # Sum should be approximately 1
        assert abs(sum(expected.values()) - 1.0) < 0.01

    def test_analyze_global(
        self,
        analyzer: BenfordsLawAnalyzer,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test global Benford's Law analysis."""
        d = Decimal
        # Create data with artificial distribution - heavy on digit 5
        data = []
        # Add many values starting with 5 (should exceed Benford's expectation of ~7.9%)
        for i in range(50):
            data.append(
                (
                    f"CLM5_{i:03d}",
                    f"PAT{i:03d}",
                    "PRV001",
                    "99213",
                    date(2024, 1, 15),
                    d(f"5{i:02d}.00"),
                    "CA",
                    "CA",
                )
            )
        # Add some normal distribution values
        for i in range(10):
            data.append(
                (
                    f"CLM1_{i:03d}",
                    f"PAT1{i:02d}",
                    "PRV001",
                    "99213",
                    date(2024, 1, 16),
                    d(f"1{i:02d}.00"),
                    "CA",
                    "CA",
                )
            )

        claims = spark.createDataFrame(data, claims_schema)

        result = analyzer.analyze(claims, "charge_amount", threshold=0.15)

        assert "benfords_anomaly" in result.columns

        # Values starting with 5 should be flagged as anomalous
        anomalies = result.filter(result.benfords_anomaly).count()
        assert anomalies > 0

    def test_analyze_by_group(
        self,
        analyzer: BenfordsLawAnalyzer,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test Benford's Law analysis by group (provider)."""
        d = Decimal
        data = []

        # Provider with suspicious distribution (heavy on digit 9)
        for i in range(30):
            data.append(
                (
                    f"CLM_SUS{i:03d}",
                    f"PAT_SUS{i:03d}",
                    "PRV_SUSPICIOUS",
                    "99213",
                    date(2024, 1, 15),
                    d(f"9{i:02d}.00"),
                    "CA",
                    "CA",
                )
            )

        # Provider with more natural distribution
        amounts = [100, 150, 200, 125, 180, 110, 195, 145, 175, 130, 210, 155, 190, 140, 120]
        for i, amt in enumerate(amounts):
            data.append(
                (
                    f"CLM_NORM{i:03d}",
                    f"PAT_NORM{i:03d}",
                    "PRV_NORMAL",
                    "99213",
                    date(2024, 1, 15),
                    d(f"{amt}.00"),
                    "CA",
                    "CA",
                )
            )

        claims = spark.createDataFrame(data, claims_schema)

        result = analyzer.analyze(claims, "charge_amount", group_by="provider_id", threshold=0.15)

        assert "benfords_anomaly" in result.columns

        # Suspicious provider should have anomalies flagged
        suspicious = result.filter(result.provider_id == "PRV_SUSPICIOUS")
        suspicious_anomalies = suspicious.filter(suspicious.benfords_anomaly).count()
        assert suspicious_anomalies > 0

    def test_analyze_handles_negative_values(
        self,
        analyzer: BenfordsLawAnalyzer,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test that analysis handles negative values correctly (uses absolute value)."""
        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("-100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV001", "99213", date(2024, 1, 16), d("150.00"), "CA", "CA"),
            ("CLM003", "PAT003", "PRV001", "99213", date(2024, 1, 17), d("-200.00"), "CA", "CA"),
            ("CLM004", "PAT004", "PRV001", "99213", date(2024, 1, 18), d("125.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)

        result = analyzer.analyze(claims, "charge_amount")

        # Should complete without error
        assert "benfords_anomaly" in result.columns
        assert result.count() == 4

    def test_analyze_handles_zero_values(
        self,
        analyzer: BenfordsLawAnalyzer,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test that analysis handles zero values correctly."""
        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("0.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV001", "99213", date(2024, 1, 16), d("150.00"), "CA", "CA"),
            ("CLM003", "PAT003", "PRV001", "99213", date(2024, 1, 17), d("200.00"), "CA", "CA"),
            ("CLM004", "PAT004", "PRV001", "99213", date(2024, 1, 18), d("125.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)

        result = analyzer.analyze(claims, "charge_amount")

        # Should complete without error
        assert "benfords_anomaly" in result.columns

    def test_get_distribution_report_global(
        self,
        analyzer: BenfordsLawAnalyzer,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test distribution report generation without grouping."""
        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV001", "99213", date(2024, 1, 16), d("150.00"), "CA", "CA"),
            ("CLM003", "PAT003", "PRV001", "99213", date(2024, 1, 17), d("200.00"), "CA", "CA"),
            ("CLM004", "PAT004", "PRV001", "99213", date(2024, 1, 18), d("125.00"), "CA", "CA"),
            ("CLM005", "PAT005", "PRV001", "99213", date(2024, 1, 19), d("180.00"), "CA", "CA"),
            ("CLM006", "PAT006", "PRV001", "99213", date(2024, 1, 20), d("350.00"), "CA", "CA"),
            ("CLM007", "PAT007", "PRV001", "99213", date(2024, 1, 21), d("475.00"), "CA", "CA"),
            ("CLM008", "PAT008", "PRV001", "99213", date(2024, 1, 22), d("560.00"), "CA", "CA"),
            ("CLM009", "PAT009", "PRV001", "99213", date(2024, 1, 23), d("690.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)

        report = analyzer.get_distribution_report(claims, "charge_amount")

        # Check report columns
        assert "first_digit" in report.columns
        assert "count" in report.columns
        assert "total" in report.columns
        assert "observed_frequency" in report.columns
        assert "expected_frequency" in report.columns
        assert "deviation" in report.columns
        assert "deviation_percentage" in report.columns

        # Report should have entries for digits present in data
        assert report.count() > 0

    def test_get_distribution_report_by_group(
        self,
        analyzer: BenfordsLawAnalyzer,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test distribution report generation with grouping."""
        d = Decimal
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV001", "99213", date(2024, 1, 16), d("150.00"), "CA", "CA"),
            ("CLM003", "PAT003", "PRV001", "99213", date(2024, 1, 17), d("200.00"), "CA", "CA"),
            ("CLM004", "PAT004", "PRV002", "99213", date(2024, 1, 18), d("300.00"), "CA", "CA"),
            ("CLM005", "PAT005", "PRV002", "99213", date(2024, 1, 19), d("450.00"), "CA", "CA"),
            ("CLM006", "PAT006", "PRV002", "99213", date(2024, 1, 20), d("520.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)

        report = analyzer.get_distribution_report(claims, "charge_amount", group_by="provider_id")

        # Check report columns include group column
        assert "provider_id" in report.columns
        assert "first_digit" in report.columns
        assert "observed_frequency" in report.columns
        assert "expected_frequency" in report.columns

        # Should have entries for both providers
        providers = [row["provider_id"] for row in report.select("provider_id").distinct().collect()]
        assert "PRV001" in providers
        assert "PRV002" in providers

    def test_analyze_filters_invalid_first_digits(
        self,
        analyzer: BenfordsLawAnalyzer,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test that analysis correctly filters out invalid first digits."""
        d = Decimal
        data = [
            # Normal values
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV001", "99213", date(2024, 1, 16), d("250.00"), "CA", "CA"),
            # Value that starts with 0 after decimal conversion
            ("CLM003", "PAT003", "PRV001", "99213", date(2024, 1, 17), d("0.50"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)

        result = analyzer.analyze(claims, "charge_amount")

        # Should complete without error
        assert result.count() == 3

    def test_analyze_with_high_threshold(
        self,
        analyzer: BenfordsLawAnalyzer,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test analysis with high threshold (less sensitive)."""
        d = Decimal
        # Create data with slight deviation
        data = []
        for i in range(20):
            data.append(
                (
                    f"CLM{i:03d}",
                    f"PAT{i:03d}",
                    "PRV001",
                    "99213",
                    date(2024, 1, 15),
                    d(f"1{i:02d}.00"),
                    "CA",
                    "CA",
                )
            )

        claims = spark.createDataFrame(data, claims_schema)

        # With very high threshold, nothing should be flagged
        result = analyzer.analyze(claims, "charge_amount", threshold=0.99)

        anomalies = result.filter(result.benfords_anomaly).count()
        assert anomalies == 0

    def test_analyze_group_with_no_anomalies(
        self,
        analyzer: BenfordsLawAnalyzer,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test group analysis returns false when no anomalies detected."""
        d = Decimal
        # Create data with somewhat natural distribution
        amounts = [
            "100.00",
            "125.00",
            "150.00",
            "175.00",
            "200.00",
            "225.00",
            "250.00",
            "300.00",
            "350.00",
            "400.00",
            "125.50",
            "155.00",
            "185.00",
            "210.00",
            "180.00",
        ]
        data = []
        for i, amt in enumerate(amounts):
            data.append(
                (
                    f"CLM{i:03d}",
                    f"PAT{i:03d}",
                    "PRV001",
                    "99213",
                    date(2024, 1, i + 1),
                    d(amt),
                    "CA",
                    "CA",
                )
            )

        claims = spark.createDataFrame(data, claims_schema)

        # With high threshold, no anomalies should be flagged
        result = analyzer.analyze(claims, "charge_amount", group_by="provider_id", threshold=0.99)

        anomalies = result.filter(result.benfords_anomaly).count()
        assert anomalies == 0
