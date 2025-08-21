{{ config(materialized='table', schema='features') }}

-- Grid of (customer_id, month)
with months as (
    select distinct
        customer_id,
        date_trunc('month', event_timestamp) as event_month
    from {{ ref('accepted_loans') }}
),

-- Historical facts at month
hist as (
    select
        customer_id,
        date_trunc('month', event_timestamp) as event_month,
        loan_amount,
        int_rate_pct,
        dti
    from {{ ref('accepted_loans') }}
),

-- Join each (customer_id, month) to all prior-month facts
grid as (
    select
        m.customer_id,
        m.event_month as event_timestamp,
        -- indicators/values for windowing
        case when h.customer_id is not null then 1 else 0 end as loan_cnt,
        h.loan_amount,
        h.int_rate_pct,
        h.dti
    from months m
    left join hist h
    on h.customer_id = m.customer_id
    and h.event_month < m.event_month
),

-- rolling windows over prior 12 months
rolled as (
    select
        customer_id,
        event_timestamp,

        -- counts
        sum(loan_cnt) over (
        partition by customer_id
        order by event_timestamp
        range between interval '365 days' preceding and interval '1 microsecond' preceding
        ) as prior_12m_loan_cnt,

       -- sums
        sum(loan_amount) over (
          partition by customer_id
          order by event_timestamp
          range between interval '365 days' preceding and interval '1 microsecond' preceding
        ) as prior_12m_loan_amt,

        sum(int_rate_pct) over (
          partition by customer_id
          order by event_timestamp
          range between interval '365 days' preceding and interval '1 microsecond' preceding
        ) as prior_12m_int_rate_sum,

        sum(dti) over (
          partition by customer_id
          order by event_timestamp
          range between interval '365 days' preceding and interval '1 microsecond' preceding
        ) as prior_12m_dti_sum
      from grid
)

select
    customer_id,
  event_timestamp,
  prior_12m_loan_cnt,
  prior_12m_loan_amt,
  case when prior_12m_loan_cnt is null or prior_12m_loan_cnt = 0
       then null
       else prior_12m_int_rate_sum / prior_12m_loan_cnt
  end as prior_12m_avg_int_rate,
  case when prior_12m_loan_cnt is null or prior_12m_loan_cnt = 0
       then null
       else prior_12m_dti_sum / prior_12m_loan_cnt
  end as prior_12m_avg_dti
from rolled