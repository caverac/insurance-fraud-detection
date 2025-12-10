"""
Generate synthetic insurance claims data for testing fraud detection algorithms.

This module creates realistic healthcare claims with configurable fraud patterns,
including inflated charges, round-dollar amounts, duplicate claims, and
geographic anomalies. The generated data follows standard healthcare coding
systems (CPT procedure codes, ICD-10 diagnosis codes, CMS place of service codes).
"""

import logging
import random
import string
from datetime import date, timedelta
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, DecimalType, StringType, StructField, StructType

logger = logging.getLogger(__name__)


# Sample procedure codes (simplified)
PROCEDURE_CODES = [
    "99213",  # Office visit, established patient
    "99214",  # Office visit, established patient (complex)
    "99215",  # Office visit, established patient (high complexity)
    "99203",  # Office visit, new patient
    "99204",  # Office visit, new patient (complex)
    "90834",  # Psychotherapy, 45 minutes
    "90837",  # Psychotherapy, 60 minutes
    "97110",  # Physical therapy
    "97140",  # Manual therapy
    "36415",  # Blood draw
    "80053",  # Comprehensive metabolic panel
    "85025",  # Complete blood count
    "71046",  # Chest X-ray
    "73030",  # Shoulder X-ray
    "99283",  # Emergency visit, moderate
    "99284",  # Emergency visit, high severity
]

# Typical charges by procedure (simplified)
PROCEDURE_CHARGES = {
    "99213": (75, 150),
    "99214": (100, 200),
    "99215": (150, 300),
    "99203": (100, 200),
    "99204": (150, 300),
    "90834": (100, 175),
    "90837": (150, 250),
    "97110": (50, 100),
    "97140": (50, 100),
    "36415": (15, 30),
    "80053": (50, 100),
    "85025": (25, 50),
    "71046": (100, 250),
    "73030": (75, 200),
    "99283": (200, 400),
    "99284": (300, 600),
}

# Diagnosis codes (simplified ICD-10)
DIAGNOSIS_CODES = [
    "J06.9",  # Acute upper respiratory infection
    "M54.5",  # Low back pain
    "E11.9",  # Type 2 diabetes
    "I10",  # Essential hypertension
    "F32.9",  # Major depressive disorder
    "J45.909",  # Asthma, unspecified
    "M25.561",  # Pain in right knee
    "R51.9",  # Headache
    "K21.0",  # GERD with esophagitis
    "N39.0",  # UTI
]

US_STATES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
]

PLACES_OF_SERVICE = [
    "11",  # Office
    "21",  # Inpatient Hospital
    "22",  # Outpatient Hospital
    "23",  # Emergency Room
    "31",  # Skilled Nursing Facility
    "81",  # Independent Laboratory
]


def generate_id(prefix: str, /, *, length: int = 10) -> str:
    """
    Generate a random alphanumeric identifier with a prefix.

    Parameters
    ----------
    prefix : str
        Prefix to prepend to the generated ID (e.g., "CLM", "PAT", "PRV").
    length : int, optional
        Number of random characters to generate, by default 10.

    Returns
    -------
    str
        A string in the format "{prefix}{random_chars}".

    Examples
    --------
    >>> generate_id("CLM", length=6)
    'CLMAB3X9K2'
    """
    chars = string.ascii_uppercase + string.digits
    return f"{prefix}{''.join(random.choices(chars, k=length))}"


def generate_claim(
    claim_id: str,
    patient_id: str,
    provider_id: str,
    provider_state: str,
    base_date: date,
    /,
    *,
    is_fraudulent: bool = False,
) -> dict[str, str | date | Decimal]:
    """
    Generate a single synthetic insurance claim record.

    Creates a claim with realistic procedure codes, charges, dates, and
    geographic data. Fraudulent claims are modified to exhibit detectable
    patterns such as inflated charges, suspiciously round amounts, or
    patient-provider state mismatches.

    Parameters
    ----------
    claim_id : str
        Unique identifier for the claim.
    patient_id : str
        Identifier for the patient associated with the claim.
    provider_id : str
        Identifier for the healthcare provider submitting the claim.
    provider_state : str
        Two-letter US state code where the provider is located.
    base_date : date
        Reference date for generating service and submission dates.
        Service dates are randomly distributed within 365 days before this date.
    is_fraudulent : bool, optional
        If True, applies one of several fraud patterns to the claim,
        by default False.

    Returns
    -------
    dict
        Claim record with keys: claim_id, patient_id, provider_id, provider_name,
        procedure_code, diagnosis_code, service_date, submitted_date,
        charge_amount, paid_amount, patient_state, provider_state, place_of_service.
    """
    procedure = random.choice(PROCEDURE_CODES)
    min_charge, max_charge = PROCEDURE_CHARGES[procedure]

    if is_fraudulent:
        # Fraudulent claim modifications
        fraud_type = random.choice(
            [
                "inflated_charge",
                "round_amount",
                "duplicate_setup",
                "wrong_state",
            ]
        )

        if fraud_type == "inflated_charge":
            charge = random.uniform(max_charge * 2, max_charge * 5)
        elif fraud_type == "round_amount":
            charge = random.choice([100, 200, 500, 1000, 1500, 2000])
        elif fraud_type == "duplicate_setup":
            charge = random.uniform(min_charge, max_charge)
        else:
            charge = random.uniform(min_charge, max_charge)

        # Wrong state fraud
        if fraud_type == "wrong_state":
            patient_state = random.choice([s for s in US_STATES if s != provider_state])
        else:
            patient_state = provider_state if random.random() > 0.1 else random.choice(US_STATES)
    else:
        charge = random.uniform(min_charge, max_charge)
        patient_state = provider_state if random.random() > 0.1 else random.choice(US_STATES)

    service_date = base_date - timedelta(days=random.randint(0, 365))
    submitted_date = service_date + timedelta(days=random.randint(1, 30))

    return {
        "claim_id": claim_id,
        "patient_id": patient_id,
        "provider_id": provider_id,
        "provider_name": f"Provider {provider_id[-4:]}",
        "procedure_code": procedure,
        "diagnosis_code": random.choice(DIAGNOSIS_CODES),
        "service_date": service_date,
        "submitted_date": submitted_date,
        "charge_amount": Decimal(str(round(charge, 2))),
        "paid_amount": Decimal(str(round(charge * random.uniform(0.6, 0.9), 2))),
        "patient_state": patient_state,
        "provider_state": provider_state,
        "place_of_service": random.choice(PLACES_OF_SERVICE),
    }


def generate_sample_claims(
    output_path: str,
    /,
    *,
    num_claims: int = 10000,
    fraud_rate: float = 0.05,
    num_providers: int = 100,
    num_patients: int = 1000,
) -> None:
    """
    Generate a dataset of synthetic insurance claims and write to CSV.

    Creates a pool of providers and patients, then generates claims with a
    controlled proportion of fraudulent records. Fraudulent claims may also
    spawn exact duplicates (with different claim IDs) to simulate duplicate
    billing fraud.

    Parameters
    ----------
    output_path : str
        Directory path where the CSV output will be written. PySpark writes
        to a directory containing part files with a header row.
    num_claims : int, optional
        Target number of claims to generate, by default 10000. Actual count
        may be slightly higher due to duplicate fraud injection.
    fraud_rate : float, optional
        Proportion of claims to mark as fraudulent, by default 0.05 (5%).
        Must be between 0 and 1.
    num_providers : int, optional
        Number of unique healthcare providers in the dataset, by default 100.
    num_patients : int, optional
        Number of unique patients in the dataset, by default 1000.

    Notes
    -----
    This function starts a local Spark session internally and stops it upon
    completion. The output is coalesced to a single partition for convenience.
    """
    spark = SparkSession.builder.master("local[*]").appName("SampleDataGen").getOrCreate()

    try:
        # Generate provider and patient pools
        providers = [(generate_id("PRV"), random.choice(US_STATES)) for _ in range(num_providers)]
        patients = [generate_id("PAT") for _ in range(num_patients)]

        base_date = date.today()
        claims: list[dict[str, str | date | Decimal]] = []

        for _ in range(num_claims):
            claim_id = generate_id("CLM")
            patient_id = random.choice(patients)
            provider_id, provider_state = random.choice(providers)
            is_fraudulent = random.random() < fraud_rate

            claim = generate_claim(
                claim_id,
                patient_id,
                provider_id,
                provider_state,
                base_date,
                is_fraudulent=is_fraudulent,
            )
            claims.append(claim)

            # Add some exact duplicates for fraud
            if is_fraudulent and random.random() < 0.2:
                duplicate = claim.copy()
                duplicate["claim_id"] = generate_id("CLM")
                claims.append(duplicate)

        # Create DataFrame
        schema = StructType(
            [
                StructField("claim_id", StringType(), False),
                StructField("patient_id", StringType(), False),
                StructField("provider_id", StringType(), False),
                StructField("provider_name", StringType(), True),
                StructField("procedure_code", StringType(), False),
                StructField("diagnosis_code", StringType(), True),
                StructField("service_date", DateType(), False),
                StructField("submitted_date", DateType(), True),
                StructField("charge_amount", DecimalType(10, 2), False),
                StructField("paid_amount", DecimalType(10, 2), True),
                StructField("patient_state", StringType(), True),
                StructField("provider_state", StringType(), True),
                StructField("place_of_service", StringType(), True),
            ]
        )

        df = spark.createDataFrame(claims, schema)  # type: ignore[type-var]

        # Write to CSV
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

        logger.info("Generated %d claims", len(claims))
        logger.info("Approximate fraud rate: %.1f%%", fraud_rate * 100)

    finally:
        spark.stop()


if __name__ == "__main__":
    generate_sample_claims("./sample_claims", num_claims=10000)
