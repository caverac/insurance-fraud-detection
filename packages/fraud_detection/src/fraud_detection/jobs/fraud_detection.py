"""Main fraud detection Spark job."""

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from fraud_detection.detector import DetectionConfig, FraudDetector

logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Insurance claims fraud detection job")
    parser.add_argument(
        "--input",
        required=True,
        help="Input path for claims data (S3 or local)",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output path for flagged claims (S3 or local)",
    )
    parser.add_argument(
        "--input-format",
        default="parquet",
        choices=["parquet", "csv", "json"],
        help="Input data format",
    )
    parser.add_argument(
        "--output-format",
        default="parquet",
        choices=["parquet", "csv", "json"],
        help="Output data format",
    )
    parser.add_argument(
        "--zscore-threshold",
        type=float,
        default=3.0,
        help="Z-score threshold for outlier detection",
    )
    parser.add_argument(
        "--iqr-multiplier",
        type=float,
        default=1.5,
        help="IQR multiplier for outlier detection",
    )
    parser.add_argument(
        "--duplicate-threshold",
        type=float,
        default=0.9,
        help="Similarity threshold for near-duplicate detection",
    )
    parser.add_argument(
        "--min-fraud-score",
        type=float,
        default=0.0,
        help="Minimum fraud score to include in output",
    )
    parser.add_argument(
        "--partition-by",
        nargs="*",
        default=["detection_date"],
        help="Columns to partition output by",
    )
    return parser.parse_args()


def create_spark_session(app_name: str = "FraudDetection") -> SparkSession:
    """Create and configure Spark session."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def read_claims(spark: SparkSession, path: str, fmt: str) -> DataFrame:
    """Read claims data from various formats."""
    reader = spark.read

    if fmt == "csv":
        return reader.option("header", "true").option("inferSchema", "true").csv(path)
    if fmt == "json":
        return reader.json(path)

    return reader.parquet(path)


def write_results(df: DataFrame, path: str, fmt: str, partition_by: list[str]) -> None:
    """Write results to storage."""
    writer = df.write.mode("overwrite")

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if fmt == "csv":
        writer.option("header", "true").csv(path)
    elif fmt == "json":
        writer.json(path)
    else:
        writer.parquet(path)


def main() -> None:
    """Define main entry point for fraud detection job."""
    args = parse_args()

    logger.info("Starting fraud detection job")
    logger.info("Input %s", args.input)
    logger.info("Output %s", args.output)

    # Create Spark session
    spark = create_spark_session()

    try:
        # Read claims data
        logger.info("Reading claims data from %s", args.input)
        claims = read_claims(spark, args.input, args.input_format)

        logger.info("Loaded %d claims", claims.count())

        # Configure detection
        config = DetectionConfig(
            outlier_zscore_threshold=args.zscore_threshold,
            outlier_iqr_multiplier=args.iqr_multiplier,
            duplicate_similarity_threshold=args.duplicate_threshold,
        )

        # Run fraud detection
        logger.info("Running fraud detection")
        detector = FraudDetector(spark, config)
        flagged = detector.detect(claims)

        # Filter by minimum fraud score if specified
        if args.min_fraud_score > 0:
            flagged = flagged.filter(F.col("fraud_score") >= args.min_fraud_score)

        # Add detection date for partitioning
        flagged = flagged.withColumn(
            "detection_date",
            F.to_date(F.current_timestamp()),
        )

        # Log statistics
        total_flagged = flagged.count()
        high_risk = flagged.filter(F.col("fraud_score") > 0.7).count()
        duplicates = flagged.filter(F.col("is_duplicate")).count()

        logger.info("Total flagged claims: %d", total_flagged)
        logger.info("High risk (score > 0.7): %d", high_risk)
        logger.info("Duplicates detected: %d", duplicates)

        # Write results
        logger.info("Writing results to %s", args.output)
        write_results(flagged, args.output, args.output_format, args.partition_by)

        logger.info("Fraud detection job completed successfully")

    except Exception as e:
        logger.error("Job failed: %s", e)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
