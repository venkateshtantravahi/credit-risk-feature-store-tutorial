{# -----------------------------------------------------------------------------
  Test: feature_not_null_keys
  Purpose:
    Ensure entity keys and timestamp are NOT NULL.

  Usage (schema.yml):
    - feature_not_null_keys:
        arguments:
          entity_keys: ['customer_id']
          timestamp_col: 'event_timestamp'
----------------------------------------------------------------------------- #}
{% test feature_not_null_keys(model, entity_keys, timestamp_col) %}

select
  {{ entity_keys | join(', ') }} , {{ timestamp_col }}
from {{ model }}
where
  (
    {%- for k in entity_keys -%}
      {{ k }} is null {%- if not loop.last %} or {% endif -%}
    {%- endfor -%}
  )
  or {{ timestamp_col }} is null

{% endtest %}
