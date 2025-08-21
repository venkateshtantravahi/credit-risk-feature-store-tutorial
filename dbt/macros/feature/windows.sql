{# -----------------------------------------------------------------------------
  rolling_sum(partition_cols, order_col, value_expr, lookback_interval)
  Example: {{ rolling_sum(['customer_id'], 'event_timestamp', 'loan_amount', "interval '365 days'") }}
  Notes: Postgres RANGE frame works with timestamps + intervals.
----------------------------------------------------------------------------- #}
{% macro rolling_sum(partition_cols, order_col, value_expr, lookback_interval) -%}
  SUM({{ value_expr }}) OVER (
    PARTITION BY {{ partition_cols|join(', ') }}
    ORDER BY {{ order_col }}
    RANGE BETWEEN {{ lookback_interval }} PRECEDING AND INTERVAL '1 microsecond' PRECEDING
  )
{%- endmacro %}

{# -----------------------------------------------------------------------------
  lag1(col, partition_col, order_col): previous value
----------------------------------------------------------------------------- #}
{% macro lag1(col, partition_cols, order_col) -%}
  LAG({{ col }}) OVER (PARTITION BY {{ partition_cols|join(', ') }} ORDER BY {{ order_col }})
{%- endmacro %}

{# -----------------------------------------------------------------------------
  rolling_count(partition_cols, order_col, lookback_interval, value_expr='*', filter_where=None)
  Example:
    {{ rolling_count(['x.customer_id'], 'x.event_timestamp', "interval '365 days'", 'hist.customer_id') }}
    {{ rolling_count(['x.customer_id'], 'x.event_timestamp', "interval '365 days'", '*', "hist.customer_id IS NOT NULL") }}
----------------------------------------------------------------------------- #}
{% macro rolling_count(partition_cols, order_col, lookback_interval, value_expr='*', filter_where=None) -%}
  COUNT({{ value_expr }})
  {%- if filter_where %} FILTER (WHERE {{ filter_where }}){% endif %}
  OVER (
    PARTITION BY {{ partition_cols|join(', ') }}
    ORDER BY {{ order_col }}
    RANGE BETWEEN {{ lookback_interval }} PRECEDING AND INTERVAL '1 microsecond' PRECEDING
  )
{%- endmacro %}
