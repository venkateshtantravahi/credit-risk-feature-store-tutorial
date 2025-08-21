{# -----------------------------------------------------------------------------
  Test: non_future_timestamp
  Purpose:
    Fail rows with timestamp in the future.

  Usage (schema.yml):
    - non_future_timestamp:
        arguments:
          column_name: 'event_timestamp'
----------------------------------------------------------------------------- #}
{% test non_future_timestamp(model, column_name) %}

select {{ column_name }}
from {{ model }}
where {{ column_name }} > now()

{% endtest %}
