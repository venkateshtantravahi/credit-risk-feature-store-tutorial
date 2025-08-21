{{ config(materialized='table') }}

with src as (
    select * from {{ source('raw_src', 'accepted_2007_to_2018q4') }}
),

clean as (
    select
    -- identities
    id::text        as loan_id,
    member_id::text as customer_id,

    -- numeric amounts/ ratios
    nullif(loan_amnt, '')::double precision         as loan_amount,
    nullif(funded_amnt, '')::double precision       as funded_amount,
    nullif(funded_amnt_inv, '')::double precision   as funded_amount_investor,
    nullif(installment, '')::double precision       as installment,
    nullif(annual_inc, '')::double precision        as annual_income,
    nullif(dti, '')::double precision               as dti,

    -- interest & utilization (robust: strip % if present)
    nullif(regexp_replace(int_rate::text, '%', '', 'g'), '')::double precision      as int_rate_pct,
    nullif(regexp_replace(revol_util::text, '%', '', 'g'), '')::double precision    as revol_util_pct,

    -- fico ranges
    nullif(fico_range_low, '')::double precision    as fico_range_low,
    nullif(fico_range_high, '')::double precision   as fico_range_high,


    -- credit metrics
    nullif(delinq_2yrs, ''):: double precision      as delinq_2yrs,
    nullif(inq_last_6mths, '')::double precision    as inq_last_6mths,
    nullif(open_acc, '')::double precision          as open_acc,
    nullif(pub_rec, ''):: double precision          as pub_rec,
    nullif(revol_bal, '')::double precision         as revol_bal,
    nullif(total_acc, '')::double precision         as total_acc,

    -- total pymnts
    nullif(total_pymnt, '')::double precision           as total_pymnt,
    nullif(total_pymnt_inv, '')::double precision       as total_pymnt_inv,
    nullif(total_rec_prncp, '')::double precision       as total_rec_prncp,
    nullif(total_rec_int, '')::double precision         as total_rec_int,
    nullif(total_rec_late_fee, '')::double precision    as total_rec_late_fee,

    -- recoveries
    nullif(recoveries, '')::double precision                    as recoveries,
    nullif(collection_recovery_fee, '')::double precision       as collection_recovery_fee,


    -- dates in 'Mon-YYYY'
    case when issue_d is null or issue_d='' then null
        else to_timestamp(issue_d, 'Mon-YYYY') end                  as issue_ts,
    case when last_pymnt_d is null or last_pymnt_d='' then null
        else to_timestamp(last_pymnt_d, 'Mon-YYYY') end             as last_pymnt_ts,
    case when next_pymnt_d is null or next_pymnt_d='' then null
        else to_timestamp(next_pymnt_d, 'Mon-YYYY') end             as next_pymnt_ts,
    case when earliest_cr_line is null or earliest_cr_line='' then null
        else to_timestamp(earliest_cr_line, 'Mon-YYYY') end         as earliest_cr_line_ts,


    -- categorical
    nullif(grade, '')::text                     as grade,
    nullif(sub_grade, '')::text                 as sub_grade,
    trim(lower(nullif(term, '')))::text         as term,
    nullif(emp_title, '')::text                 as emp_title,
    nullif(emp_length, '')::text                as emp_length_raw,
    nullif(home_ownership, '')::text            as home_ownership,
    nullif(verification_status, '')::text       as verification_status,
    nullif(purpose, '')::text                   as purpose,
    nullif(addr_state, '')::text                as addr_state,
    nullif(loan_status, '')::text               as loan_status,
    nullif(application_type, '')::text          as application_type,


    -- event timestamp
    case when issue_d is null or issue_d='' then null
        else to_timestamp(issue_d, 'Mon-YYYY') end  as event_timestamp

from src
)

select * from clean