"""
Duplicate claim detection for insurance fraud analysis.

This module identifies both exact and near-duplicate claims, which are common
indicators of fraudulent billing practices such as double-billing or claim
resubmission with minor modifications to avoid detection.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from fraud_detection.detector import DetectionConfig


class DuplicateDetector:
    """
    Detect duplicate and near-duplicate insurance claims.

    This detector identifies two types of duplicates:

    1. **Exact duplicates**: Claims with identical key fields (patient, provider,
       procedure, date, and amount). These often indicate accidental or intentional
       double-billing.

    2. **Near-duplicates**: Claims that are highly similar but not identical,
       potentially indicating resubmission with minor changes to evade detection.
       Uses configurable similarity thresholds and time windows.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session for distributed processing.
    config : DetectionConfig
        Configuration object containing detection thresholds:
        - ``duplicate_similarity_threshold``: Minimum similarity score (0-1) for
          near-duplicate detection.
        - ``duplicate_time_window_days``: Maximum days between service dates for
          claims to be considered potential near-duplicates.

    Examples
    --------
    >>> detector = DuplicateDetector(spark, config)
    >>> flagged_claims = detector.detect(claims_df)
    >>> duplicates = flagged_claims.filter(flagged_claims.is_duplicate == True)
    """

    def __init__(self, spark: SparkSession, config: DetectionConfig) -> None:
        self.spark = spark
        self.config = config

    def detect(self, claims: DataFrame) -> DataFrame:
        """
        Run full duplicate detection pipeline on claims data.

        Sequentially applies exact and near-duplicate detection, then combines
        results into unified duplicate flags. A claim is marked as duplicate if
        it matches either criterion.

        Parameters
        ----------
        claims : DataFrame
            Input claims with required columns: ``claim_id``, ``patient_id``,
            ``provider_id``, ``procedure_code``, ``service_date``, ``charge_amount``.

        Returns
        -------
        DataFrame
            Original claims with additional columns:

            - ``is_duplicate`` : bool - True if claim is any type of duplicate.
            - ``duplicate_of`` : str - claim_id of the original claim this duplicates.
            - ``is_exact_duplicate`` : bool - True if exact field match.
            - ``is_near_duplicate`` : bool - True if similarity-based match.
        """
        claims = self._detect_exact_duplicates(claims)
        claims = self._detect_near_duplicates(claims)

        claims = claims.withColumn(
            "is_duplicate",
            F.col("is_exact_duplicate") | F.col("is_near_duplicate"),
        )

        claims = claims.withColumn(
            "duplicate_of",
            F.coalesce(
                F.col("exact_duplicate_of"),
                F.col("near_duplicate_of"),
            ),
        )

        return claims

    def _detect_exact_duplicates(self, claims: DataFrame) -> DataFrame:
        """
        Identify claims with identical key fields.

        Creates a composite key from patient, provider, procedure, date, and amount,
        then uses window functions to find and rank duplicates within each key group.
        The first claim (by claim_id order) is considered the original; subsequent
        claims are flagged as duplicates.

        Parameters
        ----------
        claims : DataFrame
            Input claims DataFrame.

        Returns
        -------
        DataFrame
            Claims with added columns:

            - ``is_exact_duplicate`` : bool - True for duplicates (not the first occurrence).
            - ``exact_duplicate_of`` : str - claim_id of the first claim in the duplicate group.
            - ``duplicate_key`` : str - Composite key used for matching.
            - ``duplicate_rank`` : int - Position within duplicate group (1 = original).
        """
        key_fields = [
            "patient_id",
            "provider_id",
            "procedure_code",
            "service_date",
            "charge_amount",
        ]

        claims = claims.withColumn(
            "duplicate_key",
            F.concat_ws("|", *[F.col(c).cast("string") for c in key_fields]),
        )

        window = Window.partitionBy("duplicate_key").orderBy("claim_id")

        claims = claims.withColumn(
            "duplicate_rank",
            F.row_number().over(window),
        )

        claims = claims.withColumn(
            "first_claim_in_group",
            F.first("claim_id").over(window),
        )

        claims = claims.withColumn(
            "is_exact_duplicate",
            F.col("duplicate_rank") > 1,
        )

        claims = claims.withColumn(
            "exact_duplicate_of",
            F.when(
                F.col("is_exact_duplicate"),
                F.col("first_claim_in_group"),
            ).otherwise(F.lit(None)),
        )

        return claims

    def _detect_near_duplicates(self, claims: DataFrame) -> DataFrame:
        """
        Identify highly similar claims that may indicate modified resubmissions.

        Performs a self-join to compare claims from the same patient-provider pair
        within a configurable time window. Calculates a weighted similarity score
        based on procedure code match (60%) and charge amount similarity (40%).
        Claims exceeding the similarity threshold are flagged as near-duplicates.

        This catches fraud patterns where claims are resubmitted with minor changes
        (e.g., slightly different amounts or dates) to avoid exact-match detection.

        Parameters
        ----------
        claims : DataFrame
            Input claims, must already have ``is_exact_duplicate`` column from
            prior exact duplicate detection (exact duplicates are excluded from
            near-duplicate analysis to avoid double-flagging).

        Returns
        -------
        DataFrame
            Claims with added columns:

            - ``is_near_duplicate`` : bool - True if similarity score exceeds threshold.
            - ``near_duplicate_of`` : str - claim_id of the most similar earlier claim.

        Notes
        -----
        Similarity calculation:

        - **Procedure match** (weight 0.6): Binary - 1.0 if codes match, 0.0 otherwise.
        - **Charge similarity** (weight 0.4): ``1 - |amount1 - amount2| / max(amount1, amount2)``

        Only the highest-similarity match is retained per claim to avoid cascading
        duplicate chains.
        """
        time_window_days = self.config.duplicate_time_window_days

        claims_left = claims.alias("left")
        claims_right = claims.alias("right")

        join_condition = (
            (F.col("left.patient_id") == F.col("right.patient_id"))
            & (F.col("left.provider_id") == F.col("right.provider_id"))
            & (F.col("left.claim_id") != F.col("right.claim_id"))
            & (F.col("left.claim_id") > F.col("right.claim_id"))
            & (F.abs(F.datediff(F.col("left.service_date"), F.col("right.service_date"))) <= time_window_days)
            & (~F.col("left.is_exact_duplicate"))
        )

        potential_duplicates = claims_left.join(claims_right, join_condition, "left")

        potential_duplicates = potential_duplicates.withColumn(
            "procedure_match",
            (F.col("left.procedure_code") == F.col("right.procedure_code")).cast("double"),
        )

        potential_duplicates = potential_duplicates.withColumn(
            "charge_similarity",
            F.lit(1.0)
            - F.abs(F.col("left.charge_amount") - F.col("right.charge_amount"))
            / F.greatest(F.col("left.charge_amount"), F.col("right.charge_amount")),
        )

        potential_duplicates = potential_duplicates.withColumn(
            "similarity_score",
            (F.col("procedure_match") * 0.6 + F.col("charge_similarity") * 0.4),
        )

        threshold = self.config.duplicate_similarity_threshold

        near_duplicates = potential_duplicates.filter(F.col("similarity_score") >= threshold).select(
            F.col("left.claim_id").alias("claim_id"),
            F.col("right.claim_id").alias("near_duplicate_of"),
            F.col("similarity_score"),
        )

        window = Window.partitionBy("claim_id").orderBy(F.desc("similarity_score"))

        near_duplicates = near_duplicates.withColumn("rank", F.row_number().over(window)).filter(F.col("rank") == 1).drop("rank", "similarity_score")

        claims = claims.join(near_duplicates, "claim_id", "left")

        claims = claims.withColumn(
            "is_near_duplicate",
            F.col("near_duplicate_of").isNotNull(),
        )

        return claims
