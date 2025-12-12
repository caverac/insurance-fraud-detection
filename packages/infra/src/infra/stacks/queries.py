"""Queries for Athena named queries."""

HIGH_RISK_PROVIDERS_QUERY = """
    SELECT
        provider_id,
        COUNT(*) as flagged_claims,
        AVG(fraud_score) as avg_fraud_score,
        SUM(charge_amount) as total_charges
    FROM {database}.flagged_claims
    WHERE fraud_score > 0.7
    GROUP BY provider_id
    ORDER BY avg_fraud_score DESC
    LIMIT 100
"""

DUPLICATE_CLAIMS_QUERY = """
    SELECT
        claim_id,
        duplicate_of,
        charge_amount,
        processed_at
    FROM {database}.flagged_claims
    WHERE is_duplicate = true
    ORDER BY processed_at DESC
"""

FRAUD_BY_RULE_QUERY = """
    SELECT
        rule,
        COUNT(*) as count,
        SUM(charge_amount) as total_amount
    FROM {database}.flagged_claims
    CROSS JOIN UNNEST(rule_violations) AS t(rule)
    GROUP BY rule
    ORDER BY count DESC
"""
