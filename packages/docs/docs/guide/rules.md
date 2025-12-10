# Rule-Based Detection

Rule-based detection uses explicit business rules derived from domain expertise to identify suspicious patterns.

## Billing Pattern Rules

### Daily Procedure Limits

**Purpose**: Detect providers billing an unrealistic number of procedures per day.

**Logic**: Count procedures per provider per day, flag if exceeds threshold.

**Configuration**:
```python
max_daily_procedures_per_provider = 50  # Default
```

**Common fraud patterns detected**:

- Phantom billing (billing for services not rendered)
- Upcoding (billing for more expensive procedures)
- Assembly-line medicine (rushing through patients)

### Patient Claim Frequency

**Purpose**: Detect patients with abnormally high claim frequency.

**Logic**: Count claims per patient per day, flag if exceeds threshold.

**Configuration**:
```python
max_claims_per_patient_per_day = 5  # Default
```

**Common fraud patterns detected**:

- Claim splitting (breaking one service into multiple claims)
- Identity theft (multiple providers billing same patient)
- Doctor shopping (seeking multiple prescriptions)

### Weekend Billing

**Purpose**: Detect unusual weekend billing patterns.

**Logic**: Calculate provider's weekend billing ratio, flag if high ratio and weekend claim.

**Threshold**: >30% weekend billing ratio

**Common fraud patterns detected**:

- Backdated claims
- Time sheet fraud
- Billing for closed facilities

### Round Amount Detection

**Purpose**: Detect suspicious patterns of round charge amounts.

**Logic**: Flag charges divisible by $100 when provider has high round-amount ratio.

**Threshold**: >20% round amounts

**Common fraud patterns detected**:

- Estimated charges (not from actual services)
- Fabricated claims
- Template billing

## Geographic Rules

### Provider-Patient Distance

**Purpose**: Detect claims where patient and provider are unusually distant.

**Logic**: Calculate geographic distance, flag if exceeds threshold.

**Configuration**:
```python
max_provider_patient_distance_miles = 500.0  # Default
```

**Common fraud patterns detected**:

- Identity theft (patient's identity used remotely)
- Phantom providers
- Billing for impossible travel

### State Mismatch

**Purpose**: Detect claims with patient and provider in different states.

**Logic**: Compare patient_state and provider_state fields.

**Common fraud patterns detected**:

- Out-of-network billing schemes
- Cross-state fraud rings
- False address claims

### Impossible Travel

**Purpose**: Detect patients with physically impossible travel patterns.

**Logic**: Check for claims from multiple distant locations on same day.

**Threshold**: >3 different provider locations per day

**Common fraud patterns detected**:

- Identity theft
- Billing fraud rings
- False patient information

## Implementing Custom Rules

Add custom rules by extending `BillingPatternRules`:

```python
from fraud_detection.rules.billing_patterns import BillingPatternRules

class CustomRules(BillingPatternRules):
    def check_specialty_mismatch(self, claims: DataFrame) -> DataFrame:
        """Flag when procedure doesn't match provider specialty."""
        # Join with provider specialty reference data
        claims = claims.join(
            self.specialty_procedures,
            "procedure_code",
            "left"
        )

        claims = claims.withColumn(
            "specialty_mismatch",
            F.col("provider_specialty") != F.col("expected_specialty")
        )

        return claims
```

## Rule Weighting

Rules contribute to the composite fraud score based on severity:

| Rule | Weight | Rationale |
|------|--------|-----------|
| Duplicate | 0.45 | Strong indicator |
| Daily limit exceeded | 0.15 | Common fraud pattern |
| Patient frequency | 0.10 | Moderate indicator |
| Weekend billing | 0.05 | Weak indicator alone |
| Round amounts | 0.05 | Weak indicator alone |
| State mismatch | 0.10 | Moderate indicator |
| Distance exceeded | 0.10 | Moderate indicator |

Weights are normalized within the rule category.

## Best Practices

1. **Start conservative**: Begin with high thresholds, tune down based on results
2. **Monitor false positive rate**: Track investigator feedback
3. **Update rules regularly**: Fraud patterns evolve
4. **Combine rules**: Single rules rarely indicate fraud; patterns do
5. **Document exceptions**: Some providers legitimately exceed limits
