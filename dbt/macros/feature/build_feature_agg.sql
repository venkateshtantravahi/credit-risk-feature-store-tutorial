{# -----------------------------------------------------------------------------
  build_feature_agg(source_relation, entity_keys:list, timestamp_col, time_bucket, feature_exprs:dict, where_clause?)
  Purpose: produce time-bucketed aggregates with a consistent grain
  Example:
    {% set F = {'loans_issued':'count(*)','avg_int_rate_pct':'avg(int_rate_pct)'} %}
    {{ build_feature_agg(ref('accepted_loans'), ['customer_id'], 'event_timestamp', 'month', F) }}
----------------------------------------------------------------------------- #}
{% macro build_feature_agg(source_relation, entity_keys, timestamp_col, time_bucket, feature_exprs, where_clause=None) -%}

WITH base AS (
  SELECT
    {{ entity_keys | join(', ') }} ,
    date_trunc('{{ time_bucket }}', {{ timestamp_col }}) AS event_timestamp,
    *
  FROM {{ source_relation }}
  {% if where_clause %} WHERE {{ where_clause }} {% endif %}
),

agg AS (
  SELECT
    {{ entity_keys | join(', ') }} ,
    event_timestamp,
    {%- for alias, expr in feature_exprs.items() %}
      {{ expr }} AS {{ alias }}{{ "," if not loop.last }}
    {%- endfor %}
  FROM base
  GROUP BY {{ entity_keys | join(', ') }}, event_timestamp
)

SELECT * FROM agg

{%- endmacro %}
