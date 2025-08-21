{{ config(materialized = "table") }}

with raw as (
    select *
    from {{ source('raw_src', 'rejected_2007_to_2018q4') }}
),

-- normalize and type
clean as (
        select
              -- numeric
              nullif(amount_requested, '')::double precision as amount_requested,
              nullif(risk_score, '')::double precision       as risk_score,
              nullif(policy_code, '')::double precision      as policy_code,

              -- DTI: strip % and cast
              case
                  when debt_to_income_ratio is null or debt_to_income_ratio = '' then null
                  else nullif(replace(debt_to_income_ratio, '%', ''), '')::double precision
                  end                                          as dti_ratio_pct,

              -- text
              nullif(loan_title, '')::text                   as loan_title,
              nullif(zip_code, '')::text                     as zip_code,
              nullif(state, '')::text                        as state,
              nullif(employment_length, '')::text            as employment_length,

              -- application_date: robust parse from TEXT (supports YYYY-MM-DD or MM/DD/YYYY)
              case
                  when application_date ~ '^\d{4}-\d{2}-\d{2}$'
                      then to_date(application_date, 'YYYY-MM-DD')
                  when application_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                      then to_date(application_date, 'MM/DD/YYYY')
                  else null
                  end                                          as application_date,

              -- event_timestamp for downstream (Feast, etc.)
              case
                  when application_date ~ '^\d{4}-\d{2}-\d{2}$'
                      then to_timestamp(application_date, 'YYYY-MM-DD')
                  when application_date ~ '^\d{1,2}/\d{1,2}/\d{4}$'
                      then to_timestamp(application_date, 'MM/DD/YYYY')
                  else null
                  end                                       as event_timestamp
        from raw
)

select * from clean