-- =============================================================
-- kpi_queries.sql
-- Core KPI queries for the Healthcare Claims Analytics Pipeline
-- Run against the warehouse after the ETL pipeline completes
-- =============================================================


-- ---------------------------------------------------------------
-- 1. Overall approval and denial rates
-- ---------------------------------------------------------------
SELECT
    COUNT(*)                                                          AS total_claims,
    SUM(CASE WHEN claim_status = 'approved' THEN 1 ELSE 0 END)       AS approved_claims,
    SUM(CASE WHEN claim_status = 'denied'   THEN 1 ELSE 0 END)       AS denied_claims,
    SUM(CASE WHEN claim_status = 'pending'  THEN 1 ELSE 0 END)       AS pending_claims,
    ROUND(100.0 * SUM(CASE WHEN claim_status = 'approved' THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                                      AS approval_rate_pct,
    ROUND(100.0 * SUM(CASE WHEN claim_status = 'denied'   THEN 1 ELSE 0 END) / COUNT(*), 2)
                                                                      AS denial_rate_pct
FROM claims;


-- ---------------------------------------------------------------
-- 2. Average processing time (overall and by claim type)
-- ---------------------------------------------------------------
SELECT
    claim_type,
    COUNT(*)                          AS total_claims,
    ROUND(AVG(processing_days), 1)    AS avg_processing_days,
    ROUND(MEDIAN(processing_days), 1) AS median_processing_days,
    MAX(processing_days)              AS max_processing_days,
    ROUND(100.0 * SUM(CASE WHEN processing_days <= 5 THEN 1 ELSE 0 END) / COUNT(*), 1)
                                      AS pct_processed_under_5_days
FROM claims
WHERE processing_days IS NOT NULL
GROUP BY claim_type
ORDER BY avg_processing_days DESC;


-- ---------------------------------------------------------------
-- 3. Cost summary — billed vs paid variance
-- ---------------------------------------------------------------
SELECT
    ROUND(SUM(billed_amount),  2) AS total_billed,
    ROUND(SUM(allowed_amount), 2) AS total_allowed,
    ROUND(SUM(paid_amount),    2) AS total_paid,
    ROUND(SUM(billed_amount - paid_amount), 2) AS total_variance,
    ROUND(AVG(billed_amount),  2) AS avg_billed_per_claim,
    ROUND(AVG(paid_amount),    2) AS avg_paid_per_claim,
    ROUND(100.0 * SUM(paid_amount) / NULLIF(SUM(billed_amount), 0), 1)
                                  AS payment_ratio_pct
FROM claims
WHERE claim_status = 'approved';


-- ---------------------------------------------------------------
-- 4. Denial breakdown by reason
-- ---------------------------------------------------------------
SELECT
    denial_reason,
    COUNT(*)                                                  AS denial_count,
    ROUND(100.0 * COUNT(*) /
        SUM(COUNT(*)) OVER (), 1)                             AS pct_of_all_denials,
    ROUND(AVG(billed_amount), 2)                              AS avg_billed_amount
FROM claims
WHERE claim_status = 'denied'
  AND denial_reason IS NOT NULL
  AND denial_reason <> 'N/A'
GROUP BY denial_reason
ORDER BY denial_count DESC;


-- ---------------------------------------------------------------
-- 5. Monthly claim volume and KPIs (trend analysis)
-- ---------------------------------------------------------------
SELECT
    TO_CHAR(claim_date, 'YYYY-MM')                                   AS year_month,
    COUNT(*)                                                          AS total_claims,
    SUM(CASE WHEN claim_status = 'approved' THEN 1 ELSE 0 END)       AS approved,
    SUM(CASE WHEN claim_status = 'denied'   THEN 1 ELSE 0 END)       AS denied,
    ROUND(100.0 * SUM(CASE WHEN claim_status = 'approved' THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                      AS approval_rate_pct,
    ROUND(SUM(billed_amount), 2)                                      AS total_billed,
    ROUND(AVG(billed_amount), 2)                                      AS avg_cost_per_claim
FROM claims
GROUP BY TO_CHAR(claim_date, 'YYYY-MM')
ORDER BY year_month;


-- ---------------------------------------------------------------
-- 6. KPIs by region
-- ---------------------------------------------------------------
SELECT
    region,
    COUNT(*)                                                          AS total_claims,
    SUM(CASE WHEN claim_status = 'approved' THEN 1 ELSE 0 END)       AS approved_claims,
    ROUND(100.0 * SUM(CASE WHEN claim_status = 'approved' THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                      AS approval_rate_pct,
    ROUND(SUM(billed_amount),  2)                                     AS total_billed,
    ROUND(AVG(processing_days),1)                                     AS avg_processing_days
FROM claims
GROUP BY region
ORDER BY total_claims DESC;


-- ---------------------------------------------------------------
-- 7. KPIs by plan type
-- ---------------------------------------------------------------
SELECT
    plan_type,
    COUNT(*)                                                          AS total_claims,
    SUM(CASE WHEN claim_status = 'approved' THEN 1 ELSE 0 END)       AS approved,
    SUM(CASE WHEN claim_status = 'denied'   THEN 1 ELSE 0 END)       AS denied,
    ROUND(100.0 * SUM(CASE WHEN claim_status = 'approved' THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                      AS approval_rate_pct,
    ROUND(AVG(paid_amount), 2)                                        AS avg_paid_per_claim
FROM claims
GROUP BY plan_type
ORDER BY total_claims DESC;


-- ---------------------------------------------------------------
-- 8. Top 10 providers by claim volume and approval rate
-- ---------------------------------------------------------------
SELECT
    provider_id,
    COUNT(*)                                                          AS total_claims,
    ROUND(100.0 * SUM(CASE WHEN claim_status = 'approved' THEN 1 ELSE 0 END) / COUNT(*), 1)
                                                                      AS approval_rate_pct,
    ROUND(SUM(paid_amount), 2)                                        AS total_paid,
    ROUND(AVG(processing_days), 1)                                    AS avg_processing_days
FROM claims
GROUP BY provider_id
ORDER BY total_claims DESC
LIMIT 10;
