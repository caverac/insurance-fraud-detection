"""Tests for billing pattern rules."""

from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from fraud_detection.detector import DetectionConfig
from fraud_detection.rules.billing_patterns import BillingPatternRules


class TestBillingPatternRules:
    """Tests for BillingPatternRules."""

    @pytest.fixture
    def rules(self, spark: SparkSession) -> BillingPatternRules:
        """Create BillingPatternRules instance."""
        config = DetectionConfig(
            max_daily_procedures_per_provider=50,
            max_claims_per_patient_per_day=5,
        )
        return BillingPatternRules(spark, config)

    def test_daily_procedure_limits_flags_high_volume(
        self,
        rules: BillingPatternRules,
        high_volume_provider_claims: DataFrame,
    ) -> None:
        """Test that providers exceeding daily limits are flagged."""
        result = rules.check_daily_procedure_limits(high_volume_provider_claims)

        # Get flagged claims for high volume provider
        high_vol_flagged = result.filter((result.provider_id == "PRV_HIGH") & (result.daily_procedure_limit_exceeded)).count()

        # All 60 claims from high volume provider should be flagged
        assert high_vol_flagged == 60

        # Normal provider should not be flagged
        normal_flagged = result.filter((result.provider_id == "PRV_NORMAL") & (result.daily_procedure_limit_exceeded)).count()

        assert normal_flagged == 0

    def test_patient_claim_frequency_detects_multiple_daily_claims(
        self,
        rules: BillingPatternRules,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test detection of patients with excessive daily claims."""

        d = Decimal
        data = [
            # Patient with 6 claims on same day (exceeds limit of 5)
            ("CLM001", "PAT_HIGH", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT_HIGH", "PRV002", "99214", date(2024, 1, 15), d("150.00"), "CA", "CA"),
            ("CLM003", "PAT_HIGH", "PRV003", "99215", date(2024, 1, 15), d("200.00"), "CA", "CA"),
            ("CLM004", "PAT_HIGH", "PRV001", "97110", date(2024, 1, 15), d("75.00"), "CA", "CA"),
            ("CLM005", "PAT_HIGH", "PRV002", "97140", date(2024, 1, 15), d("80.00"), "CA", "CA"),
            ("CLM006", "PAT_HIGH", "PRV003", "36415", date(2024, 1, 15), d("25.00"), "CA", "CA"),
            # Normal patient with 2 claims
            ("CLM007", "PAT_NORMAL", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM008", "PAT_NORMAL", "PRV002", "99214", date(2024, 1, 15), d("150.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = rules.check_patient_claim_frequency(claims)

        # High frequency patient should be flagged
        high_freq_flagged = result.filter((result.patient_id == "PAT_HIGH") & (result.patient_frequency_exceeded)).count()

        assert high_freq_flagged == 6

        # Normal patient should not be flagged
        normal_flagged = result.filter((result.patient_id == "PAT_NORMAL") & (result.patient_frequency_exceeded)).count()

        assert normal_flagged == 0

    def test_weekend_billing_flags_high_weekend_ratio(
        self,
        rules: BillingPatternRules,
        weekend_claims: DataFrame,
    ) -> None:
        """Test that providers with high weekend billing are flagged."""
        result = rules.check_weekend_billing(weekend_claims)

        # Weekend provider should have weekend billing flagged
        weekend_flagged = result.filter((result.provider_id == "PRV_WEEKEND") & (result.weekend_billing_flag)).count()

        # All 4 weekend claims should be flagged
        assert weekend_flagged == 4

        # Normal provider (weekdays only) should not have any flagged
        normal_flagged = result.filter((result.provider_id == "PRV_NORMAL") & (result.weekend_billing_flag)).count()

        assert normal_flagged == 0

    def test_round_amounts_detected(
        self,
        rules: BillingPatternRules,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test detection of round amount patterns."""
        from datetime import date
        from decimal import Decimal

        d = Decimal
        data = [
            # Provider with many round amounts (>20% threshold)
            ("CLM001", "PAT001", "PRV_ROUND", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT002", "PRV_ROUND", "99214", date(2024, 1, 16), d("200.00"), "CA", "CA"),
            ("CLM003", "PAT003", "PRV_ROUND", "99215", date(2024, 1, 17), d("300.00"), "CA", "CA"),
            ("CLM004", "PAT004", "PRV_ROUND", "99213", date(2024, 1, 18), d("500.00"), "CA", "CA"),
            # Normal provider with varied amounts
            ("CLM005", "PAT005", "PRV_NORMAL", "99213", date(2024, 1, 15), d("127.50"), "CA", "CA"),
            ("CLM006", "PAT006", "PRV_NORMAL", "99214", date(2024, 1, 16), d("183.25"), "CA", "CA"),
            ("CLM007", "PAT007", "PRV_NORMAL", "99215", date(2024, 1, 17), d("241.99"), "CA", "CA"),
            ("CLM008", "PAT008", "PRV_NORMAL", "99213", date(2024, 1, 18), d("89.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = rules.check_round_amounts(claims)

        # Round amount provider should have flagged claims
        round_flagged = result.filter((result.provider_id == "PRV_ROUND") & (result.round_amount_flag)).count()

        assert round_flagged == 4

        # Normal provider should not have round amount flags
        normal_flagged = result.filter((result.provider_id == "PRV_NORMAL") & (result.round_amount_flag)).count()

        assert normal_flagged == 0

    def test_procedure_unbundling_detected(
        self,
        rules: BillingPatternRules,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test detection of procedure unbundling."""
        from pyspark.sql.types import StringType, StructField
        from pyspark.sql.types import StructType as ST

        d = Decimal
        # Create bundled procedures reference table
        bundled_schema = ST(
            [
                StructField("bundled_code", StringType(), False),
                StructField("unbundled_code_1", StringType(), False),
                StructField("unbundled_code_2", StringType(), False),
            ]
        )
        bundled_data = [
            ("80053", "80048", "80076"),  # Comprehensive metabolic panel components
            ("85025", "85027", "85004"),  # CBC components
        ]
        bundled_procedures = spark.createDataFrame(bundled_data, bundled_schema)

        # Claims data
        data = [
            # Patient with unbundled procedures (80048 + 80076 on same day)
            ("CLM001", "PAT_UNBUNDLE", "PRV001", "80048", date(2024, 1, 15), d("50.00"), "CA", "CA"),
            ("CLM002", "PAT_UNBUNDLE", "PRV001", "80076", date(2024, 1, 15), d("75.00"), "CA", "CA"),
            # Normal patient without unbundling
            ("CLM003", "PAT_NORMAL", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM004", "PAT_NORMAL", "PRV001", "36415", date(2024, 1, 15), d("25.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = rules.check_procedure_unbundling(claims, bundled_procedures)

        # Check columns exist
        assert "procedures_same_day" in result.columns
        assert "unbundling_flag" in result.columns

        # Unbundled claims should be flagged
        unbundle_flagged = result.filter((result.patient_id == "PAT_UNBUNDLE") & (result.unbundling_flag)).count()
        assert unbundle_flagged > 0

        # Normal patient should not be flagged
        normal_flagged = result.filter((result.patient_id == "PAT_NORMAL") & (result.unbundling_flag)).count()
        assert normal_flagged == 0

    def test_procedure_unbundling_no_match(
        self,
        rules: BillingPatternRules,
        spark: SparkSession,
        claims_schema: StructType,
    ) -> None:
        """Test unbundling check with no matching unbundled pairs."""
        from pyspark.sql.types import StringType, StructField
        from pyspark.sql.types import StructType as ST

        d = Decimal
        bundled_schema = ST(
            [
                StructField("bundled_code", StringType(), False),
                StructField("unbundled_code_1", StringType(), False),
                StructField("unbundled_code_2", StringType(), False),
            ]
        )
        bundled_data = [
            ("80053", "80048", "80076"),
        ]
        bundled_procedures = spark.createDataFrame(bundled_data, bundled_schema)

        # Claims without unbundling
        data = [
            ("CLM001", "PAT001", "PRV001", "99213", date(2024, 1, 15), d("100.00"), "CA", "CA"),
            ("CLM002", "PAT001", "PRV001", "99214", date(2024, 1, 15), d("150.00"), "CA", "CA"),
        ]
        claims = spark.createDataFrame(data, claims_schema)  # type: ignore[arg-type]

        result = rules.check_procedure_unbundling(claims, bundled_procedures)

        # No claims should be flagged
        flagged = result.filter(result.unbundling_flag).count()
        assert flagged == 0
