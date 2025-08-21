-- Fail rows where features are timestamped after the loan issue month
select f.customer_id, f.event_timestamp
from {{ ref('final_customer_features') }} f
join {{ ref('accepted_loans') }} a
    on a.customer_id = f.customer_id
and date_trunc('month', a.event_timestamp) = f.event_timestamp
where f.event_timestamp > a.event_timestamp