{{ config(materialized='table', schema='features') }}

WITH base AS (
    select
        loan_id,
        date_trunc('month', event_timestamp) as event_timestamp,
        loan_amount,
        funded_amount,
        annual_income,
        dti,
        int_rate_pct,
        revol_util_pct,
        fico_range_low,
        fico_range_high
    FROM {{ ref('accepted_loans') }}
)

select
    loan_id,
    event_timestamp,
    loan_amount,
    funded_amount,
    annual_income,
    dti,
    {{ clamp_range('int_rate_pct', 0, 100) }} as int_rate_pct,
    {{ clamp_range('revol_util_pct', 0, 200) }} as revol_util_pct,
    ( fico_range_low + fico_range_high )/2.0 as fico_avg,
    {{ bucketize('((fico_range_low + fico_range_high)/2.0)', [0,600,660,700,740,780,900],
                   ['subprime', 'near-prime', 'prime-', 'prime', 'prime+', 'super']) }} as fico_band
from base