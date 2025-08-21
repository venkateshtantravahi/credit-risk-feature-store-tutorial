{{ config(materialized='table', schema='features') }}

with a as (
    select * from {{ ref('base_customer_features') }}
),
b as (
    select * from {{ ref('customer_rolling_12m') }}
),
c as (
    select * from {{ ref('geo_aggregates_monthly') }}
)

select
    a.customer_id,
    a.event_timestamp,
    --base
    a.loan_amount, a.funded_amount, a.annual_income, a.dti,
    a.int_rate_pct, a.revol_util_pct, a.fico_avg, a.fico_band,
    -- rolling
    b.prior_12m_loan_cnt,
    b.prior_12m_loan_amnt,
    b.prior_12m_avg_int_rate,
    b.prior_12m_avg_dti,
from a
left join b
on b.customer_id = a.customer_id
and b.event_timestamp = a.event_timestamp