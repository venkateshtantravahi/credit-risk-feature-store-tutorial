{{ config(materialized='table', schema='features') }}

with r as (
    select
        state,
        date_trunc('month', event_timestamp) as event_timestamp,
    from {{ ref('rejected_loans') }}
    where state is not null
)
select
    state,
    event_timestamp,
    count(*) as state_rejects_in_month
from r
group by state, event_timestamp