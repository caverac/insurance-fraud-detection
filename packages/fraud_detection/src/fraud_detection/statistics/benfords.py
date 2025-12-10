"""
Benford's Law analysis for insurance fraud detection.

Benford's Law (also known as the First-Digit Law) is a mathematical observation
that in many naturally occurring datasets, the leading digit is more likely to
be small. Specifically:

- Digit 1 appears as the leading digit ~30.1% of the time
- Digit 9 appears as the leading digit ~4.6% of the time

This counterintuitive distribution applies to data spanning multiple orders of
magnitude, such as financial transactions, population figures, and physical
constants.

**Fraud Detection Application**

Financial fraud often violates Benford's Law because:

1. **Fabricated numbers**: Fraudsters tend to create numbers with more uniform
   digit distributions, or subconsciously favor certain digits.

2. **Round number bias**: Fraudulent amounts often cluster around round numbers
   (e.g., $100, $500, $1000), distorting the natural distribution.

3. **Threshold avoidance**: Fraudsters may manipulate amounts to stay below
   review thresholds, creating artificial spikes at certain values.

A significant deviation from Benford's expected distribution in claim amounts
is a strong indicator that warrants further investigation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class BenfordsLawAnalyzer:
    """
    Analyze claim data for conformity to Benford's Law.

    Compares the observed first-digit distribution of numeric data against
    the expected Benford's distribution. Significant deviations may indicate
    fabricated or manipulated data.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session for distributed processing.

    Examples
    --------
    >>> analyzer = BenfordsLawAnalyzer(spark)
    >>> claims = analyzer.analyze(claims, "charge_amount", group_by="provider_id")
    >>> anomalies = claims.filter(claims.benfords_anomaly)

    >>> # Generate detailed report for visualization
    >>> report = analyzer.get_distribution_report(claims, "charge_amount")
    >>> report.show()

    Notes
    -----
    Benford's Law works best with data that:

    - Spans multiple orders of magnitude
    - Is not artificially constrained (e.g., not limited to a narrow range)
    - Has a sufficient sample size (typically 100+ values)

    For small datasets or data with limited range, results may be unreliable.
    """

    BENFORDS_EXPECTED = {
        1: 0.301,
        2: 0.176,
        3: 0.125,
        4: 0.097,
        5: 0.079,
        6: 0.067,
        7: 0.058,
        8: 0.051,
        9: 0.046,
    }
    """Expected first-digit frequencies according to Benford's Law.

    Derived from the formula: P(d) = log10(1 + 1/d) for d = 1, 2, ..., 9
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def analyze(
        self,
        df: DataFrame,
        column: str,
        group_by: str | None = None,
        threshold: float = 0.15,
    ) -> DataFrame:
        """
        Analyze a numeric column for Benford's Law conformity.

        Extracts the first digit from each value, calculates the observed
        distribution, and compares it to the expected Benford's distribution.
        Values whose first digit appears more frequently than expected (by
        more than the threshold) are flagged.

        Parameters
        ----------
        df : DataFrame
            Input DataFrame containing the column to analyze.
        column : str
            Name of the numeric column to analyze (e.g., "charge_amount").
        group_by : str, optional
            Column to group by for per-group analysis (e.g., "provider_id").
            When provided, calculates separate distributions for each group,
            allowing detection of providers with anomalous digit patterns.
        threshold : float, default 0.15
            Maximum allowed deviation from expected frequency before flagging.
            A value of 0.15 means a digit appearing 45% of the time when
            expected to appear 30% would be flagged (0.45 - 0.30 = 0.15).

        Returns
        -------
        DataFrame
            Input DataFrame with added column:

            - ``benfords_anomaly`` : bool - True if the value's first digit
              is over-represented in the dataset, suggesting possible fabrication.
        """
        df = df.withColumn(
            "_first_digit",
            F.substring(F.abs(F.col(column)).cast("string"), 1, 1).cast("int"),
        )

        df = df.withColumn(
            "_first_digit",
            F.when((F.col("_first_digit") > 0) & (F.col("_first_digit") <= 9), F.col("_first_digit")),
        )

        if group_by:
            df = self._analyze_by_group(df, group_by, threshold)
        else:
            df = self._analyze_global(df, threshold)

        df = df.drop("_first_digit")

        return df

    def _analyze_by_group(self, df: DataFrame, group_by: str, threshold: float) -> DataFrame:
        """
        Perform Benford's Law analysis separately for each group.

        Calculates the digit distribution within each group (e.g., per provider)
        and flags groups whose distribution deviates significantly from expected.
        This is more sensitive than global analysis for detecting individual
        bad actors within an otherwise compliant dataset.

        Parameters
        ----------
        df : DataFrame
            Input DataFrame with ``_first_digit`` column already added.
        group_by : str
            Column to partition the analysis by.
        threshold : float
            Maximum deviation threshold for flagging.

        Returns
        -------
        DataFrame
            DataFrame with ``benfords_anomaly`` column added.
        """
        digit_counts = df.filter(F.col("_first_digit").isNotNull()).groupBy(group_by, "_first_digit").count()

        totals = digit_counts.groupBy(group_by).agg(F.sum("count").alias("_total"))

        digit_counts = digit_counts.join(totals, group_by)

        digit_counts = digit_counts.withColumn("_observed_freq", F.col("count") / F.col("_total"))

        expected_df = self.spark.createDataFrame(  # type: ignore[arg-type]
            list(self.BENFORDS_EXPECTED.items()),
            ["_first_digit", "_expected_freq"],
        )

        digit_counts = digit_counts.join(expected_df, "_first_digit")

        digit_counts = digit_counts.withColumn(
            "_deviation",
            F.abs(F.col("_observed_freq") - F.col("_expected_freq")),
        )

        group_deviations = digit_counts.groupBy(group_by).agg(
            F.max("_deviation").alias("_max_benford_deviation"),
            F.avg("_deviation").alias("_avg_benford_deviation"),
        )

        group_deviations = group_deviations.withColumn(
            "benfords_anomaly_group",
            F.col("_max_benford_deviation") > threshold,
        )

        df = df.join(
            group_deviations.select(group_by, "_max_benford_deviation", "benfords_anomaly_group"),
            group_by,
            "left",
        )

        df = df.withColumn(
            "benfords_anomaly",
            F.coalesce(F.col("benfords_anomaly_group"), F.lit(False)),
        )

        df = df.drop("_max_benford_deviation", "benfords_anomaly_group")

        return df

    def _analyze_global(self, df: DataFrame, threshold: float) -> DataFrame:
        """
        Perform Benford's Law analysis across the entire dataset.

        Calculates a single global digit distribution and identifies which
        digits are over-represented. Individual values with those first digits
        are flagged.

        Parameters
        ----------
        df : DataFrame
            Input DataFrame with ``_first_digit`` column already added.
        threshold : float
            Maximum deviation threshold for flagging a digit as anomalous.

        Returns
        -------
        DataFrame
            DataFrame with ``benfords_anomaly`` column added.
        """
        digit_counts = df.filter(F.col("_first_digit").isNotNull()).groupBy("_first_digit").count()

        total_first = digit_counts.agg(F.sum("count")).first()
        if not total_first:
            raise ValueError("No valid data to analyze")
        total = total_first[0]

        digit_counts = digit_counts.withColumn("_observed_freq", F.col("count") / F.lit(total))

        expected_df = self.spark.createDataFrame(  # type: ignore[arg-type]
            list(self.BENFORDS_EXPECTED.items()),
            ["_first_digit", "_expected_freq"],
        )

        digit_counts = digit_counts.join(expected_df, "_first_digit")

        digit_counts = digit_counts.withColumn(
            "_deviation",
            F.abs(F.col("_observed_freq") - F.col("_expected_freq")),
        )

        over_represented = (
            digit_counts.filter((F.col("_observed_freq") > F.col("_expected_freq")) & (F.col("_deviation") > threshold))
            .select("_first_digit")
            .collect()
        )

        flagged_digits = [row["_first_digit"] for row in over_represented]

        df = df.withColumn(
            "benfords_anomaly",
            F.col("_first_digit").isin(flagged_digits) if flagged_digits else F.lit(False),
        )

        return df

    def get_distribution_report(self, df: DataFrame, column: str, group_by: str | None = None) -> DataFrame:
        """
        Generate a detailed Benford's Law distribution report.

        Creates a summary table comparing observed vs. expected digit frequencies,
        useful for visualization, auditing, and deeper analysis of potential
        anomalies.

        Parameters
        ----------
        df : DataFrame
            Input DataFrame containing the column to analyze.
        column : str
            Name of the numeric column to analyze.
        group_by : str, optional
            Column to group by for per-group reports.

        Returns
        -------
        DataFrame
            Report with columns:

            - ``first_digit`` : int - The leading digit (1-9).
            - ``count`` : int - Number of occurrences.
            - ``total`` : int - Total values in group/dataset.
            - ``observed_frequency`` : float - Actual proportion.
            - ``expected_frequency`` : float - Benford's expected proportion.
            - ``deviation`` : float - Difference (observed - expected).
            - ``deviation_percentage`` : float - Deviation as % of expected.

            If ``group_by`` is provided, includes that column as well.

        Examples
        --------
        >>> report = analyzer.get_distribution_report(claims, "charge_amount")
        >>> report.show()
        +-----------+-----+-----+------------------+------------------+---------+--------------------+
        |first_digit|count|total|observed_frequency|expected_frequency|deviation|deviation_percentage|
        +-----------+-----+-----+------------------+------------------+---------+--------------------+
        |          1| 3021|10000|            0.3021|             0.301|   0.0011|                0.37|
        |          2| 1755|10000|            0.1755|             0.176|  -0.0005|               -0.28|
        ...
        """
        analysis_df = df.withColumn(
            "first_digit",
            F.substring(F.abs(F.col(column)).cast("string"), 1, 1).cast("int"),
        ).filter((F.col("first_digit") > 0) & (F.col("first_digit") <= 9))

        if group_by:
            digit_counts = analysis_df.groupBy(group_by, "first_digit").count()
            totals = digit_counts.groupBy(group_by).agg(F.sum("count").alias("total"))
            digit_counts = digit_counts.join(totals, group_by)
        else:
            digit_counts = analysis_df.groupBy("first_digit").count()
            total_first = digit_counts.agg(F.sum("count")).first()
            if not total_first:
                raise ValueError("No valid data to analyze")
            total = total_first[0]
            digit_counts = digit_counts.withColumn("total", F.lit(total))

        digit_counts = digit_counts.withColumn(
            "observed_frequency",
            F.round(F.col("count") / F.col("total"), 4),
        )

        expected_df = self.spark.createDataFrame(  # type: ignore[arg-type]
            [(d, round(f, 4)) for d, f in self.BENFORDS_EXPECTED.items()],
            ["first_digit", "expected_frequency"],
        )

        report = digit_counts.join(expected_df, "first_digit")

        report = report.withColumn(
            "deviation",
            F.round(F.col("observed_frequency") - F.col("expected_frequency"), 4),
        )

        report = report.withColumn(
            "deviation_percentage",
            F.round(F.col("deviation") / F.col("expected_frequency") * 100, 2),
        )

        return report.orderBy(*([group_by] if group_by else []), "first_digit")
