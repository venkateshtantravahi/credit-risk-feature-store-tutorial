-- fail if > 5% of rows have all rolling stats NULL while customers
-- had prior loans
with t as (
    select
        count(*) filter (
        where prior_12m_loan_cnt is null
        and prior_12m_loan_amt is null
        and prior_12m_avg_int_rate is null
        ) as null_roll_rows,
        count(*) as total_rows
    from {{ ref('final_customer_features') }}
)
select *
from t
where (t.null_roll_rows::double precision / nullif(total_rows, 0)) > 0.05