{# -----------------------------------------------------------------------------
  Test: feature_unique_on_keys
  Purpose:
    Ensure uniqueness on (entity_keys, timestamp_col).
    Typically used for time-bucketed features.

  Usage (schema.yml):
    - feature_unique_on_keys:
        arguments:
          entity_keys: ['customer_id']
          timestamp_col: 'event_timestamp'
----------------------------------------------------------------------------- #}
{% test feature_unique_on_keys(model, entity_keys, timestamp_col) %}

with d as (
  select
    {{ entity_keys | join(', ') }} , {{ timestamp_col }},
    count(*) as cnt
  from {{ model }}
  group by {{ entity_keys | join(', ') }} , {{ timestamp_col }}
)
select * from d where cnt > 1

{% endtest %}
