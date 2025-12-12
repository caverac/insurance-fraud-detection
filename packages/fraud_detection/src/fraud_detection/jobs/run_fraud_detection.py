"""EMR Spark job for fraud detection."""

import argparse
import logging
import sys
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from fraud_detection import FraudDetector

logging.basicConfig(
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


def main() -> int:
    """Run fraud detection job on EMR."""
    parser = argparse.ArgumentParser(description="Fraud Detection Spark Job")
    parser.add_argument("--input", required=True, help="Input path for claims data (S3)")
    parser.add_argument("--output", required=True, help="Output path for results (S3)")
    parser.add_argument(
        "--format",
        default="csv",
        choices=["csv", "parquet", "json"],
        help="Input file format (default: csv)",
    )
    args = parser.parse_args()

    logger.info("Starting fraud detection job")
    logger.info("Input: %s, Output: %s, Format: %s", args.input, args.output, args.format)

    spark = SparkSession.builder.appName("FraudDetection").getOrCreate()

    try:
        logger.info("Reading claims from %s", args.input)
        if args.format == "csv":
            claims = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input)
        elif args.format == "parquet":
            claims = spark.read.parquet(args.input)
        else:
            claims = spark.read.json(args.input)

        claim_count = claims.count()
        logger.info("Loaded %d claims", claim_count)

        logger.info("Running fraud detection...")
        detector = FraudDetector(spark)
        results = detector.detect(claims)

        flagged_count = results.count()
        high_risk = results.filter(results.fraud_score > 0.7).count()
        logger.info("Flagged claims: %d, High risk (>0.7): %d", flagged_count, high_risk)

        # Add detection_date partition column
        today = date.today().isoformat()
        results_partitioned = results.withColumn("detection_date", F.lit(today))

        logger.info("Writing results to %s with partition detection_date=%s", args.output, today)
        results_partitioned.write.mode("overwrite").partitionBy("detection_date").parquet(args.output)
        logger.info("Fraud detection job completed successfully")

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.exception("Error running fraud detection: %s", e)
        return 1

    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
