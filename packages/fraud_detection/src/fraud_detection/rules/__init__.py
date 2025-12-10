"""Rule-based fraud detection modules."""

from fraud_detection.rules.billing_patterns import BillingPatternRules
from fraud_detection.rules.duplicates import DuplicateDetector
from fraud_detection.rules.geographic import GeographicRules

__all__ = ["BillingPatternRules", "DuplicateDetector", "GeographicRules"]
