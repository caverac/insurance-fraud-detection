"""
Billing pattern analysis for insurance fraud detection.

This module implements rule-based detection of suspicious billing patterns that
commonly indicate fraudulent activity. It analyzes temporal patterns, claim
frequencies, and amount characteristics to identify anomalies such as:

- Providers billing an implausible number of procedures per day
- Patients receiving an unusual number of services in a short period
- Suspicious weekend billing for non-emergency services
- Claims with suspiciously round dollar amounts
- Procedure unbundling (billing separately for bundled services)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from fraud_detection.detector import DetectionConfig


class BillingPatternRules:
    """
    Detect suspicious billing patterns indicative of fraud.

    This class implements a collection of rule-based checks that identify
    billing anomalies commonly associated with fraudulent claims. Each method
    adds one or more flag columns to the input DataFrame indicating whether
    the claim exhibits the suspicious pattern.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session for distributed processing.
    config : DetectionConfig
        Configuration object containing detection thresholds:

        - ``max_daily_procedures_per_provider``: Maximum procedures a provider
          can reasonably bill in one day.
        - ``max_claims_per_patient_per_day``: Maximum claims expected for a
          single patient per day.

    Examples
    --------
    >>> rules = BillingPatternRules(spark, config)
    >>> claims = rules.check_daily_procedure_limits(claims)
    >>> claims = rules.check_round_amounts(claims)
    >>> suspicious = claims.filter(claims.daily_procedure_limit_exceeded)
    """

    def __init__(self, spark: SparkSession, config: DetectionConfig) -> None:
        self.spark = spark
        self.config = config

    def check_daily_procedure_limits(self, claims: DataFrame) -> DataFrame:
        """
        Flag providers exceeding daily procedure limits.

        Identifies providers billing an implausibly high number of procedures
        on a single day, which may indicate phantom billing (billing for services
        not rendered) or upcoding schemes.

        A provider billing 100+ procedures per day is physically impossible for
        most service types and warrants investigation.

        Parameters
        ----------
        claims : DataFrame
            Input claims with ``provider_id`` and ``service_date`` columns.

        Returns
        -------
        DataFrame
            Claims with added columns:

            - ``daily_procedure_count`` : int - Total procedures by this provider on this date.
            - ``daily_procedure_limit_exceeded`` : bool - True if count exceeds configured limit.
        """
        window = Window.partitionBy("provider_id", "service_date")

        claims = claims.withColumn(
            "daily_procedure_count",
            F.count("*").over(window),
        )

        claims = claims.withColumn(
            "daily_procedure_limit_exceeded",
            F.col("daily_procedure_count") > F.lit(self.config.max_daily_procedures_per_provider),
        )

        return claims

    def check_patient_claim_frequency(self, claims: DataFrame) -> DataFrame:
        """
        Flag patients with abnormally high daily claim frequency.

        Identifies patients receiving an unusual number of services on the same
        day, which may indicate claim splitting (dividing one service into multiple
        claims for higher reimbursement) or duplicate billing.

        Parameters
        ----------
        claims : DataFrame
            Input claims with ``patient_id`` and ``service_date`` columns.

        Returns
        -------
        DataFrame
            Claims with added columns:

            - ``patient_daily_claims`` : int - Number of claims for this patient on this date.
            - ``patient_frequency_exceeded`` : bool - True if count exceeds configured limit.
        """
        window = Window.partitionBy("patient_id", "service_date")

        claims = claims.withColumn(
            "patient_daily_claims",
            F.count("*").over(window),
        )

        claims = claims.withColumn(
            "patient_frequency_exceeded",
            F.col("patient_daily_claims") > F.lit(self.config.max_claims_per_patient_per_day),
        )

        return claims

    def check_weekend_billing(self, claims: DataFrame) -> DataFrame:
        """
        Flag suspicious weekend billing patterns.

        Identifies providers with unusually high weekend billing volumes. Most
        medical practices are closed on weekends, so high weekend billing for
        routine (non-emergency) procedures may indicate fraudulent backdating
        of claims or fabricated services.

        A provider is flagged if they have weekend claims AND their overall
        weekend billing ratio exceeds 30% of total claims.

        Parameters
        ----------
        claims : DataFrame
            Input claims with ``provider_id`` and ``service_date`` columns.

        Returns
        -------
        DataFrame
            Claims with added columns:

            - ``day_of_week`` : int - Day of week (1=Sunday, 7=Saturday).
            - ``is_weekend`` : bool - True if service date falls on weekend.
            - ``provider_weekend_ratio`` : float - Proportion of provider's claims on weekends.
            - ``weekend_billing_flag`` : bool - True if weekend claim from high-weekend provider.
        """
        claims = claims.withColumn(
            "day_of_week",
            F.dayofweek("service_date"),
        )

        claims = claims.withColumn(
            "is_weekend",
            F.col("day_of_week").isin([1, 7]),
        )

        window = Window.partitionBy("provider_id")

        claims = claims.withColumn(
            "provider_weekend_ratio",
            F.avg(F.col("is_weekend").cast("double")).over(window),
        )

        claims = claims.withColumn(
            "weekend_billing_flag",
            (F.col("is_weekend")) & (F.col("provider_weekend_ratio") > 0.30),
        )

        return claims

    def check_round_amounts(self, claims: DataFrame) -> DataFrame:
        """
        Flag claims with suspiciously round charge amounts.

        Legitimate medical charges typically result in non-round amounts due to
        fee schedules, adjustments, and itemized billing. A high proportion of
        perfectly round amounts (e.g., $100, $500, $1000) may indicate estimated
        or fabricated charges rather than actual services rendered.

        Providers are flagged if they have round-hundred charges AND more than
        20% of their claims have round amounts.

        Parameters
        ----------
        claims : DataFrame
            Input claims with ``provider_id`` and ``charge_amount`` columns.

        Returns
        -------
        DataFrame
            Claims with added columns:

            - ``is_round_hundred`` : bool - True if amount is divisible by 100.
            - ``is_round_fifty`` : bool - True if amount is divisible by 50.
            - ``provider_round_ratio`` : float - Proportion of provider's claims with round amounts.
            - ``round_amount_flag`` : bool - True if round amount from high-round-ratio provider.
        """
        claims = claims.withColumn(
            "is_round_hundred",
            (F.col("charge_amount") % 100 == 0) & (F.col("charge_amount") > 0),
        )

        claims = claims.withColumn(
            "is_round_fifty",
            (F.col("charge_amount") % 50 == 0) & (F.col("charge_amount") > 0),
        )

        window = Window.partitionBy("provider_id")

        claims = claims.withColumn(
            "provider_round_ratio",
            F.avg(F.col("is_round_hundred").cast("double")).over(window),
        )

        claims = claims.withColumn(
            "round_amount_flag",
            (F.col("is_round_hundred")) & (F.col("provider_round_ratio") > 0.20),
        )

        return claims

    def check_procedure_unbundling(self, claims: DataFrame, bundled_procedures: DataFrame) -> DataFrame:
        """
        Detect procedure unbundling fraud.

        Unbundling occurs when a provider bills separately for procedures that
        should be billed together as a single comprehensive service at a lower
        combined rate. This is a common fraud scheme to increase reimbursement.

        For example, a complete blood panel should be billed as one procedure,
        not as individual tests for each blood component.

        Parameters
        ----------
        claims : DataFrame
            Input claims with ``patient_id``, ``provider_id``, ``service_date``,
            and ``procedure_code`` columns.
        bundled_procedures : DataFrame
            Reference table defining procedure bundles with columns:

            - ``bundled_code`` : str - The correct bundled procedure code.
            - ``unbundled_code_1`` : str - First component code when unbundled.
            - ``unbundled_code_2`` : str - Second component code when unbundled.

        Returns
        -------
        DataFrame
            Claims with added columns:

            - ``procedures_same_day`` : array<str> - All procedure codes for this
              patient/provider/date combination.
            - ``unbundling_flag`` : bool - True if unbundled procedure pair detected.
        """
        window = Window.partitionBy("patient_id", "service_date", "provider_id")

        claims = claims.withColumn(
            "procedures_same_day",
            F.collect_set("procedure_code").over(window),
        )

        claims = claims.join(
            bundled_procedures,
            F.array_contains(F.col("procedures_same_day"), F.col("unbundled_code_1"))
            & F.array_contains(F.col("procedures_same_day"), F.col("unbundled_code_2")),
            "left",
        )

        claims = claims.withColumn(
            "unbundling_flag",
            F.col("bundled_code").isNotNull(),
        )

        return claims
