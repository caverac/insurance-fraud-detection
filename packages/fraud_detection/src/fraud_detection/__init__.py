"""Insurance claims fraud detection using PySpark."""

import logging

from fraud_detection.detector import FraudDetector

# Configure logging for the entire package
logging.basicConfig(
    level=logging.INFO,
)

__version__ = "0.1.0"
__all__ = ["FraudDetector"]
