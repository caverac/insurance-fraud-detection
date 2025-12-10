"""
Statistical outlier detection for insurance fraud analysis.

This module provides multiple statistical methods to identify anomalous claim
amounts that deviate significantly from expected patterns. Outlier detection is
fundamental to fraud detection because fraudulent claims often involve:

- **Inflated charges**: Billing significantly more than the typical rate for a procedure.
- **Unusual patterns**: Providers consistently charging above or below market rates.
- **Temporal anomalies**: Sudden unexplained spikes in billing amounts.

Two primary statistical methods are implemented:

1. **Z-score method**: Measures how many standard deviations a value is from the mean.
   Best for normally distributed data.

2. **IQR method**: Uses quartiles to define outlier boundaries. More robust to extreme
   values and non-normal distributions.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, Row, Window
from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    from fraud_detection.detector import DetectionConfig


class OutlierDetector:
    """
    Detect statistical outliers in insurance claims data.

    Provides multiple outlier detection methods optimized for fraud analysis,
    supporting both global analysis and group-based detection (e.g., by procedure
    code). All methods add boolean flag columns indicating outlier status.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session for distributed processing.
    config : DetectionConfig
        Configuration object containing detection thresholds:

        - ``outlier_zscore_threshold``: Number of standard deviations for Z-score method.
        - ``outlier_iqr_multiplier``: IQR multiplier (typically 1.5 for outliers, 3.0 for extreme).

    Examples
    --------
    >>> detector = OutlierDetector(spark, config)
    >>> claims = detector.detect_zscore_outliers(claims, "charge_amount", "is_outlier")
    >>> claims = detector.detect_procedure_outliers(claims)
    >>> outliers = claims.filter(claims.is_outlier)
    """

    def __init__(self, spark: SparkSession, config: DetectionConfig) -> None:
        self.spark = spark
        self.config = config

    def detect_zscore_outliers(
        self,
        df: DataFrame,
        column: str,
        output_column: str,
        group_by: list[str] | None = None,
    ) -> DataFrame:
        """
        Identify outliers using the Z-score (standard score) method.

        The Z-score measures how many standard deviations a value is from the
        mean. Values with absolute Z-scores exceeding the configured threshold
        are flagged as outliers.

        Z-score is most effective when data is approximately normally distributed.
        For skewed distributions (common with financial data), consider using the
        IQR method instead.

        Parameters
        ----------
        df : DataFrame
            Input DataFrame containing the column to analyze.
        column : str
            Name of the numeric column to check for outliers.
        output_column : str
            Name for the boolean flag column to be added.
        group_by : list[str], optional
            Columns to partition by for group-wise statistics. For example,
            passing ``["procedure_code"]`` calculates separate means and
            standard deviations for each procedure, making the detection
            context-aware.

        Returns
        -------
        DataFrame
            Input DataFrame with added boolean column ``output_column`` where
            True indicates the value is an outlier.

        Notes
        -----
        The formula used is: ``z = (x - μ) / σ``

        A claim is flagged if ``|z| > threshold``. When standard deviation is
        zero (all values identical), Z-score is set to 0 to avoid division errors.
        """
        threshold = self.config.outlier_zscore_threshold

        if group_by:
            window = Window.partitionBy(group_by)
        else:
            window = Window.partitionBy(F.lit(1))

        df = df.withColumn(f"_{column}_mean", F.mean(column).over(window))
        df = df.withColumn(f"_{column}_stddev", F.stddev(column).over(window))

        df = df.withColumn(
            f"_{column}_zscore",
            F.when(
                F.col(f"_{column}_stddev") > 0,
                (F.col(column) - F.col(f"_{column}_mean")) / F.col(f"_{column}_stddev"),
            ).otherwise(F.lit(0.0)),
        )

        df = df.withColumn(
            output_column,
            F.abs(F.col(f"_{column}_zscore")) > threshold,
        )

        df = df.drop(f"_{column}_mean", f"_{column}_stddev", f"_{column}_zscore")

        return df

    def detect_iqr_outliers(
        self,
        df: DataFrame,
        column: str,
        output_column: str,
        group_by: list[str] | None = None,
    ) -> DataFrame:
        """
        Identify outliers using the Interquartile Range (IQR) method.

        IQR-based detection is more robust than Z-score for skewed distributions
        and is less sensitive to extreme outliers when calculating bounds. Values
        below ``Q1 - k*IQR`` or above ``Q3 + k*IQR`` are flagged as outliers.

        Parameters
        ----------
        df : DataFrame
            Input DataFrame containing the column to analyze.
        column : str
            Name of the numeric column to check for outliers.
        output_column : str
            Name for the boolean flag column to be added.
        group_by : list[str], optional
            Columns to partition by for group-wise quartile calculation.

        Returns
        -------
        DataFrame
            Input DataFrame with added boolean column ``output_column`` where
            True indicates the value is an outlier.

        Notes
        -----
        IQR = Q3 - Q1 (interquartile range)

        - Lower bound: Q1 - (multiplier * IQR)
        - Upper bound: Q3 + (multiplier * IQR)

        Common multiplier values:

        - 1.5: Standard outliers (Tukey's method)
        - 3.0: Extreme outliers only
        """
        multiplier = self.config.outlier_iqr_multiplier

        if group_by:
            percentiles_df = df.groupBy(group_by).agg(
                F.percentile_approx(column, 0.25).alias("_q1"),
                F.percentile_approx(column, 0.75).alias("_q3"),
            )
            df = df.join(percentiles_df, group_by, "left")
        else:

            select_first: Row | None = df.select(
                F.percentile_approx(column, 0.25),
                F.percentile_approx(column, 0.75),
            ).first()
            if not select_first:
                raise ValueError("No data to calculate percentiles")
            q1: float = select_first[0]
            q3: float = select_first[1]
            df = df.withColumn("_q1", F.lit(q1))
            df = df.withColumn("_q3", F.lit(q3))

        df = df.withColumn("_iqr", F.col("_q3") - F.col("_q1"))
        df = df.withColumn("_lower_bound", F.col("_q1") - (multiplier * F.col("_iqr")))
        df = df.withColumn("_upper_bound", F.col("_q3") + (multiplier * F.col("_iqr")))

        df = df.withColumn(
            output_column,
            (F.col(column) < F.col("_lower_bound")) | (F.col(column) > F.col("_upper_bound")),
        )

        df = df.drop("_q1", "_q3", "_iqr", "_lower_bound", "_upper_bound")

        return df

    def detect_procedure_outliers(
        self,
        df: DataFrame,
        charge_column: str = "charge_amount",
        procedure_column: str = "procedure_code",
    ) -> DataFrame:
        """
        Detect charge outliers within each procedure code.

        Charges for the same procedure should fall within a predictable range.
        A charge that is an outlier globally might be normal for a complex
        procedure, while a seemingly normal charge might be fraudulent for
        a simple procedure. This method contextualizes outlier detection by
        procedure type.

        Parameters
        ----------
        df : DataFrame
            Input claims DataFrame.
        charge_column : str, default "charge_amount"
            Column containing charge amounts.
        procedure_column : str, default "procedure_code"
            Column containing procedure codes for grouping.

        Returns
        -------
        DataFrame
            Claims with added column:

            - ``procedure_charge_outlier`` : bool - True if charge is an outlier for this procedure.
        """
        df = self.detect_zscore_outliers(
            df,
            column=charge_column,
            output_column="procedure_charge_outlier",
            group_by=[procedure_column],
        )

        return df

    def detect_provider_outliers(
        self,
        df: DataFrame,
        charge_column: str = "charge_amount",
        provider_column: str = "provider_id",
        procedure_column: str = "procedure_code",
    ) -> DataFrame:
        """
        Identify providers with systematically unusual billing patterns.

        Compares each provider's average charges per procedure against market
        averages. Providers consistently billing significantly above or below
        market rates may be engaged in upcoding, unbundling, or other schemes.

        Parameters
        ----------
        df : DataFrame
            Input claims DataFrame.
        charge_column : str, default "charge_amount"
            Column containing charge amounts.
        provider_column : str, default "provider_id"
            Column containing provider identifiers.
        procedure_column : str, default "procedure_code"
            Column containing procedure codes.

        Returns
        -------
        DataFrame
            Claims with added columns:

            - ``provider_avg_charge`` : float - Provider's average charge for this procedure.
            - ``charge_deviation_ratio`` : float - Ratio of provider avg to market avg.
            - ``provider_billing_outlier`` : bool - True if ratio >2.0 or <0.5.
        """
        provider_avg = df.groupBy(provider_column, procedure_column).agg(
            F.avg(charge_column).alias("provider_avg_charge"),
            F.count("*").alias("provider_procedure_count"),
        )

        market_avg = df.groupBy(procedure_column).agg(
            F.avg(charge_column).alias("market_avg_charge"),
            F.stddev(charge_column).alias("market_stddev_charge"),
        )

        combined = provider_avg.join(market_avg, procedure_column)

        combined = combined.withColumn(
            "charge_deviation_ratio",
            F.when(F.col("market_avg_charge") > 0, F.col("provider_avg_charge") / F.col("market_avg_charge")).otherwise(F.lit(1.0)),
        )

        combined = combined.withColumn(
            "provider_billing_outlier",
            (F.col("charge_deviation_ratio") > 2.0) | (F.col("charge_deviation_ratio") < 0.5),
        )

        df = df.join(
            combined.select(
                provider_column,
                procedure_column,
                "provider_avg_charge",
                "charge_deviation_ratio",
                "provider_billing_outlier",
            ),
            [provider_column, procedure_column],
            "left",
        )

        return df

    def detect_temporal_outliers(
        self,
        df: DataFrame,
        charge_column: str = "charge_amount",
        date_column: str = "service_date",
    ) -> DataFrame:
        """
        Detect sudden spikes in provider billing patterns over time.

        Identifies claims where the charge amount significantly exceeds the
        provider's recent historical average. Sudden unexplained billing
        increases may indicate the start of a fraud scheme.

        Uses a 4-week rolling average as the baseline and flags charges
        exceeding 3x this average.

        Parameters
        ----------
        df : DataFrame
            Input claims DataFrame.
        charge_column : str, default "charge_amount"
            Column containing charge amounts.
        date_column : str, default "service_date"
            Column containing service dates.

        Returns
        -------
        DataFrame
            Claims with added column:

            - ``temporal_spike_flag`` : bool - True if charge >3x rolling average.

        Notes
        -----
        The rolling window looks at the previous 4 weeks of data for each
        provider. Claims in the first 4 weeks of a provider's history will
        not have a baseline for comparison and will not be flagged.
        """
        df = df.withColumn("_week", F.weekofyear(date_column))
        df = df.withColumn("_year", F.year(date_column))

        window = Window.partitionBy("provider_id").orderBy("_year", "_week")

        df = df.withColumn(
            "_rolling_avg",
            F.avg(charge_column).over(window.rowsBetween(-4, -1)),
        )

        df = df.withColumn(
            "temporal_spike_flag",
            F.when(
                F.col("_rolling_avg").isNotNull() & (F.col("_rolling_avg") > 0),
                F.col(charge_column) > (3 * F.col("_rolling_avg")),
            ).otherwise(F.lit(False)),
        )

        df = df.drop("_week", "_year", "_rolling_avg")

        return df
