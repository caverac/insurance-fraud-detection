"""Command-line interface for fraud detection."""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from fraud_detection.detector import FraudDetector
from fraud_detection.utils.sample_data import generate_sample_claims

logger = logging.getLogger(__name__)


def main() -> int:
    """Define main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Insurance claims fraud detection CLI",
        prog="fraud-detect",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Run command
    run_parser = subparsers.add_parser("run", help="Run fraud detection on claims data")
    run_parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Input path for claims data",
    )
    run_parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Output path for results",
    )
    run_parser.add_argument(
        "--format",
        "-f",
        default="parquet",
        choices=["parquet", "csv", "json"],
        help="Data format (default: parquet)",
    )
    run_parser.add_argument(
        "--local",
        action="store_true",
        help="Run in local mode",
    )

    # Analyze command
    analyze_parser = subparsers.add_parser(
        "analyze",
        help="Analyze fraud detection results",
    )
    analyze_parser.add_argument(
        "--results",
        "-r",
        required=True,
        help="Path to fraud detection results",
    )
    analyze_parser.add_argument(
        "--report",
        default="summary",
        choices=["summary", "providers", "benfords"],
        help="Type of analysis report",
    )
    analyze_parser.add_argument(
        "--format",
        "-f",
        default="csv",
        choices=["csv", "parquet", "json"],
        help="Format of results data (default: csv)",
    )

    # Generate sample data command
    sample_parser = subparsers.add_parser(
        "generate-sample",
        help="Generate sample claims data for testing",
    )
    sample_parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Output path for sample data",
    )
    sample_parser.add_argument(
        "--num-claims",
        "-n",
        type=int,
        default=10000,
        help="Number of claims to generate",
    )
    sample_parser.add_argument(
        "--fraud-rate",
        type=float,
        default=0.05,
        help="Approximate rate of fraudulent claims (0-1)",
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 1

    if args.command == "run":
        return run_detection(args)

    if args.command == "analyze":
        return run_analysis(args)

    if args.command == "generate-sample":
        return generate_sample(args)

    return 0


def run_detection(args: argparse.Namespace) -> int:
    """Run fraud detection job."""
    logger.info("Running fraud detection on %s", args.input)

    builder = SparkSession.builder.appName("FraudDetectionCLI")

    if args.local:
        builder = builder.master("local[*]")

    spark = builder.getOrCreate()

    try:
        # Read data
        if args.format == "csv":
            claims = spark.read.option("header", "true").option("inferSchema", "true").csv(args.input)
        elif args.format == "json":
            claims = spark.read.json(args.input)
        else:
            claims = spark.read.parquet(args.input)

        logger.info("Loaded %d claims", claims.count())

        # Run detection
        detector = FraudDetector(spark)
        results = detector.detect(claims)

        # Write results
        if args.format == "csv":
            # Convert array columns to strings for CSV compatibility
            csv_results = results
            for field in results.schema.fields:
                if "ArrayType" in str(field.dataType):
                    csv_results = csv_results.withColumn(field.name, F.concat_ws(", ", F.col(field.name)))
            csv_results.write.mode("overwrite").option("header", "true").csv(args.output)
        elif args.format == "json":
            results.write.mode("overwrite").json(args.output)
        else:
            results.write.mode("overwrite").parquet(args.output)

        logger.info("Results written to %s", args.output)
        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.exception("Error running fraud detection: %s", e)
        return 1

    finally:
        spark.stop()


def run_analysis(args: argparse.Namespace) -> int:
    """Run analysis on fraud detection results."""
    spark = SparkSession.builder.appName("FraudAnalysis").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Read results in the specified format
        if args.format == "csv":
            results = spark.read.option("header", "true").option("inferSchema", "true").csv(args.results)
        elif args.format == "json":
            results = spark.read.json(args.results)
        else:
            results = spark.read.parquet(args.results)

        if args.report == "summary":
            logger.info("Generating fraud detection summary report")

            total = results.count()
            high_risk = results.filter(F.col("fraud_score") > 0.7).count()
            medium_risk = results.filter((F.col("fraud_score") > 0.3) & (F.col("fraud_score") <= 0.7)).count()
            low_risk = results.filter(F.col("fraud_score") <= 0.3).count()

            logger.info("Fraud Detection Summary")
            logger.info("Total claims analyzed: %d", total)
            logger.info("High risk (>0.7):      %d (%.1f%%)", high_risk, high_risk / total * 100)
            logger.info("Medium risk (0.3-0.7): %d (%.1f%%)", medium_risk, medium_risk / total * 100)
            logger.info("Low risk (<=0.3):      %d (%.1f%%)", low_risk, low_risk / total * 100)

            logger.info("Score Distribution")
            results.select(
                F.mean("fraud_score").alias("mean"),
                F.stddev("fraud_score").alias("stddev"),
                F.min("fraud_score").alias("min"),
                F.max("fraud_score").alias("max"),
            ).show()

        elif args.report == "providers":
            logger.info("High Risk Providers")
            results.filter(F.col("fraud_score") > 0.5).groupBy("provider_id").agg(
                F.count("*").alias("flagged_claims"),
                F.avg("fraud_score").alias("avg_score"),
                F.sum("charge_amount").alias("total_charges"),
            ).orderBy(F.desc("avg_score")).show(20, truncate=False)

        elif args.report == "benfords":
            logger.info("Benford's Law Analysis")
            logger.info("Run full detection to get Benford's analysis")

        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.exception("Error running analysis: %s", e)
        return 1

    finally:
        spark.stop()


def generate_sample(args: argparse.Namespace) -> int:
    """Generate sample claims data."""
    logger.info("Generating %d sample claims...", args.num_claims)

    try:
        generate_sample_claims(
            args.output,
            num_claims=args.num_claims,
            fraud_rate=args.fraud_rate,
        )
        logger.info("Sample data written to %s", args.output)
        return 0

    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.exception("Error generating sample data: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
