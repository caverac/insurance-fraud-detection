"""
Main fraud detection orchestrator for insurance claims analysis.

This module provides the central entry point for running fraud detection on
insurance claims data. It coordinates multiple detection strategies:

1. **Rule-based detection**: Identifies known fraud patterns (billing anomalies,
   geographic impossibilities, suspicious timing).

2. **Statistical detection**: Finds outliers using Z-score, IQR, and Benford's
   Law analysis.

3. **Duplicate detection**: Identifies exact and near-duplicate claims that may
   indicate double-billing or resubmission schemes.

The results are combined into a composite fraud score that prioritizes claims
for investigation based on the number and severity of flags triggered.
"""

from dataclasses import dataclass
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fraud_detection.rules.billing_patterns import BillingPatternRules
from fraud_detection.rules.duplicates import DuplicateDetector
from fraud_detection.rules.geographic import GeographicRules
from fraud_detection.statistics.benfords import BenfordsLawAnalyzer
from fraud_detection.statistics.outliers import OutlierDetector


@dataclass
class DetectionConfig:
    """
    Configuration parameters for fraud detection algorithms.

    Controls thresholds, weights, and limits used by all detection components.
    Default values are tuned for typical healthcare claims data but should be
    adjusted based on your specific data characteristics and risk tolerance.

    Parameters
    ----------
    outlier_zscore_threshold : float, default 3.0
        Number of standard deviations from mean to flag as outlier.
        Lower values catch more anomalies but increase false positives.
    outlier_iqr_multiplier : float, default 1.5
        IQR multiplier for outlier bounds. Standard is 1.5 (Tukey's method);
        use 3.0 for extreme outliers only.
    duplicate_similarity_threshold : float, default 0.9
        Minimum similarity score (0-1) for near-duplicate detection.
        Higher values require closer matches.
    duplicate_time_window_days : int, default 30
        Maximum days between claims to consider them potential duplicates.
    max_provider_patient_distance_miles : float, default 500.0
        Maximum reasonable distance between patient and provider locations.
    max_daily_procedures_per_provider : int, default 50
        Maximum procedures a provider can reasonably bill in one day.
    max_claims_per_patient_per_day : int, default 5
        Maximum claims expected for a single patient per day.
    weight_rule_violation : float, default 0.3
        Weight for rule violations in composite fraud score.
    weight_statistical_anomaly : float, default 0.25
        Weight for statistical anomalies in composite fraud score.
    weight_duplicate : float, default 0.45
        Weight for duplicate detection in composite fraud score.

    Examples
    --------
    >>> # Use stricter thresholds for high-value claims
    >>> config = DetectionConfig(
    ...     outlier_zscore_threshold=2.5,
    ...     duplicate_similarity_threshold=0.85,
    ... )
    >>> detector = FraudDetector(spark, config)
    """

    outlier_zscore_threshold: float = 3.0
    outlier_iqr_multiplier: float = 1.5
    duplicate_similarity_threshold: float = 0.9
    duplicate_time_window_days: int = 30
    max_provider_patient_distance_miles: float = 500.0
    max_daily_procedures_per_provider: int = 50
    max_claims_per_patient_per_day: int = 5
    weight_rule_violation: float = 0.3
    weight_statistical_anomaly: float = 0.25
    weight_duplicate: float = 0.45


class FraudDetector:
    """
    Central orchestrator for insurance fraud detection.

    Coordinates multiple detection strategies (rule-based, statistical, and
    duplicate detection) and combines their outputs into a unified fraud score.
    Designed for distributed processing on large claims datasets using PySpark.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session for distributed processing.
    config : DetectionConfig, optional
        Configuration object with detection thresholds and weights.
        If not provided, uses default values.

    Attributes
    ----------
    billing_rules : BillingPatternRules
        Component for detecting suspicious billing patterns.
    duplicate_detector : DuplicateDetector
        Component for identifying duplicate claims.
    geographic_rules : GeographicRules
        Component for detecting geographic anomalies.
    outlier_detector : OutlierDetector
        Component for statistical outlier detection.
    benfords_analyzer : BenfordsLawAnalyzer
        Component for Benford's Law conformity analysis.

    Examples
    --------
    >>> from pyspark.sql import SparkSession
    >>> spark = SparkSession.builder.appName("FraudDetection").getOrCreate()
    >>> detector = FraudDetector(spark)
    >>> results = detector.detect(claims_df)
    >>> high_risk = results.filter(results.fraud_score > 0.7)
    """

    def __init__(self, spark: SparkSession, config: Optional[DetectionConfig] = None) -> None:
        self.spark = spark
        self.config = config or DetectionConfig()

        self.billing_rules = BillingPatternRules(spark, self.config)
        self.duplicate_detector = DuplicateDetector(spark, self.config)
        self.geographic_rules = GeographicRules(spark, self.config)
        self.outlier_detector = OutlierDetector(spark, self.config)
        self.benfords_analyzer = BenfordsLawAnalyzer(spark)

    def detect(self, claims: DataFrame) -> DataFrame:
        """
        Run the complete fraud detection pipeline on claims data.

        Executes all detection methods in sequence: rule-based checks,
        statistical analysis, duplicate detection, then combines results
        into a weighted fraud score.

        Parameters
        ----------
        claims : DataFrame
            Input claims data with required columns:

            - ``claim_id`` : str - Unique claim identifier.
            - ``patient_id`` : str - Patient identifier.
            - ``provider_id`` : str - Provider identifier.
            - ``procedure_code`` : str - CPT/HCPCS procedure code.
            - ``service_date`` : date - Date of service.
            - ``charge_amount`` : decimal - Billed amount.
            - ``patient_state`` : str, optional - Patient's state.
            - ``provider_state`` : str, optional - Provider's state.

        Returns
        -------
        DataFrame
            Processed claims with fraud analysis results:

            - ``claim_id``, ``patient_id``, ``provider_id``, ``charge_amount`` : Original fields.
            - ``fraud_score`` : float - Composite risk score (0-1).
            - ``fraud_reasons`` : array<str> - List of triggered flags.
            - ``rule_violations`` : array<str> - Rule-based flags triggered.
            - ``statistical_flags`` : array<str> - Statistical anomaly flags.
            - ``is_duplicate`` : bool - Whether claim is a duplicate.
            - ``duplicate_of`` : str - Original claim ID if duplicate.
            - ``processed_at`` : timestamp - When analysis was performed.
        """
        claims_with_rules = self._apply_rules(claims)
        claims_with_stats = self._apply_statistics(claims_with_rules)
        claims_with_duplicates = self._detect_duplicates(claims_with_stats)
        result = self._calculate_fraud_score(claims_with_duplicates)

        return result

    def _apply_rules(self, claims: DataFrame) -> DataFrame:
        """
        Apply rule-based detection methods.

        Runs billing pattern and geographic rule checks to identify claims
        that match known fraud patterns.

        Parameters
        ----------
        claims : DataFrame
            Input claims DataFrame.

        Returns
        -------
        DataFrame
            Claims with rule violation flags added.
        """
        claims = self.billing_rules.check_daily_procedure_limits(claims)
        claims = self.billing_rules.check_patient_claim_frequency(claims)
        claims = self.billing_rules.check_weekend_billing(claims)
        claims = self.billing_rules.check_round_amounts(claims)

        claims = self.geographic_rules.check_provider_patient_distance(claims)
        claims = self.geographic_rules.check_state_mismatch(claims)

        return claims

    def _apply_statistics(self, claims: DataFrame) -> DataFrame:
        """
        Apply statistical anomaly detection methods.

        Runs outlier detection (Z-score and IQR) on charge amounts and
        analyzes conformity to Benford's Law.

        Parameters
        ----------
        claims : DataFrame
            Input claims DataFrame with rule flags.

        Returns
        -------
        DataFrame
            Claims with statistical anomaly flags added.
        """
        claims = self.outlier_detector.detect_zscore_outliers(claims, "charge_amount", "charge_zscore_outlier")
        claims = self.outlier_detector.detect_iqr_outliers(claims, "charge_amount", "charge_iqr_outlier")
        claims = self.benfords_analyzer.analyze(claims, "charge_amount")

        return claims

    def _detect_duplicates(self, claims: DataFrame) -> DataFrame:
        """
        Run duplicate and near-duplicate detection.

        Parameters
        ----------
        claims : DataFrame
            Input claims DataFrame with rule and statistical flags.

        Returns
        -------
        DataFrame
            Claims with duplicate detection flags added.
        """
        return self.duplicate_detector.detect(claims)

    def _calculate_fraud_score(self, claims: DataFrame) -> DataFrame:
        """
        Calculate composite fraud score from all detection signals.

        Combines rule violations, statistical anomalies, and duplicate flags
        into a weighted score. Also aggregates individual flags into summary
        arrays for investigation support.

        The fraud score is calculated as:
            score = (rule_score * weight_rule) + (stat_score * weight_stat) + (dup_score * weight_dup)

        Where each component score is the proportion of flags triggered in that category.

        Parameters
        ----------
        claims : DataFrame
            Input claims with all detection flags.

        Returns
        -------
        DataFrame
            Final output with fraud scores and selected columns for downstream use.
        """
        rule_columns = [
            "daily_procedure_limit_exceeded",
            "patient_frequency_exceeded",
            "weekend_billing_flag",
            "round_amount_flag",
            "distance_exceeded",
            "state_mismatch",
        ]

        stat_columns = [
            "charge_zscore_outlier",
            "charge_iqr_outlier",
            "benfords_anomaly",
        ]

        claims = claims.withColumn(
            "rule_violations",
            F.array(*[F.when(F.col(col), F.lit(col)).otherwise(F.lit(None)) for col in rule_columns if col in claims.columns]),
        )
        claims = claims.withColumn(
            "rule_violations",
            F.expr("filter(rule_violations, x -> x is not null)"),
        )

        claims = claims.withColumn(
            "statistical_flags",
            F.array(*[F.when(F.col(col), F.lit(col)).otherwise(F.lit(None)) for col in stat_columns if col in claims.columns]),
        )
        claims = claims.withColumn(
            "statistical_flags",
            F.expr("filter(statistical_flags, x -> x is not null)"),
        )

        claims = claims.withColumn(
            "rule_score",
            F.size("rule_violations") / F.lit(len(rule_columns)),
        )

        claims = claims.withColumn(
            "stat_score",
            F.size("statistical_flags") / F.lit(len(stat_columns)),
        )

        claims = claims.withColumn(
            "duplicate_score",
            F.when(F.col("is_duplicate"), F.lit(1.0)).otherwise(F.lit(0.0)),
        )

        claims = claims.withColumn(
            "fraud_score",
            (
                F.col("rule_score") * F.lit(self.config.weight_rule_violation)
                + F.col("stat_score") * F.lit(self.config.weight_statistical_anomaly)
                + F.col("duplicate_score") * F.lit(self.config.weight_duplicate)
            ),
        )

        claims = claims.withColumn(
            "fraud_reasons",
            F.concat(F.col("rule_violations"), F.col("statistical_flags")),
        )

        claims = claims.withColumn("processed_at", F.current_timestamp())

        return claims.select(
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
        )
