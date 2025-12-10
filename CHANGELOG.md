# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2024-01-15

### Added

- Initial project structure with Python monorepo
- PySpark fraud detection application
  - Rule-based detection (billing patterns, geographic, temporal)
  - Statistical detection (Z-score, IQR, Benford's Law)
  - Duplicate detection (exact and near-duplicate)
  - Configurable fraud scoring
- AWS CDK infrastructure
  - Data Lake stack (S3, Glue)
  - Processing stack (EMR, Step Functions)
  - Analytics stack (Athena, Glue tables)
- Documentation site with MkDocs Material
- CLI for local development
- Sample data generator
- Comprehensive test suite
