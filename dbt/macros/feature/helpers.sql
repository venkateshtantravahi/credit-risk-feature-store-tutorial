{# ----------------------------------------------------------------
 Macro: tz_to_utz
 Purpose:
    Force a timestamp expression to UTC (safe default for serving).
Usage:
    {{ tz_to_utc('event_timestamp') }}
-------------------------------------------------------------------- #}
{% macro tz_to_utc(col) -%}
    ({{ col }} AT TIME ZONE 'UTC')
{%- endmacro %}

{# -----------------------------------------------------------------
 Macro: clamp_range
 Purpose:
 If numeric value is outside [min, max], return NULL; else the value.
 Usage:
 {{ clamp_range('revol_util_pct', 0, 200) }}
-------------------------------------------------------------------- #}
{% macro clamp_range(col, min_value, max_value) -%}
CASE
    WHEN {{ col }} IS NULL THEN NULL
    WHEN {{ col }} < {{ min_value }} OR {{ col }} > {{ max_value }} THEN NULL
    ELSE {{ col }}
END
{%- endmacro %}

{# -----------------------------------------------------------------------------
  safe_divide(num, den, default=NULL): avoid div-by-zero
----------------------------------------------------------------------------- #}
{% macro safe_divide(num, den, default='NULL') -%}
  CASE
    WHEN {{ den }} IS NULL OR {{ den }} = 0 THEN {{ default }}
    ELSE ( {{ num }}::double precision / {{ den }}::double precision )
  END
{%- endmacro %}

{# -----------------------------------------------------------------------------
  bucketize(col, edges:list, labels:list?): numeric bins -> label
  Example: {{ bucketize('fico', [0,600,660,700,740,780,850],
                                ['subprime','near','prime-','prime','prime+','super']) }}
----------------------------------------------------------------------------- #}
{% macro bucketize(col, edges, labels=None) -%}
  CASE
  {%- for i in range(0, (edges|length) - 1) %}
    WHEN {{ col }} >= {{ edges[i] }} AND {{ col }} < {{ edges[i+1] }}
      THEN {% if labels %}'{{ labels[i] }}'{% else %}{{ i }}{% endif %}
  {%- endfor %}
    ELSE {% if labels %}'other'{% else %}NULL{% endif %}
  END
{%- endmacro %}