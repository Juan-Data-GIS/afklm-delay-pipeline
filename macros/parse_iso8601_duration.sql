{% macro parse_iso8601_duration_minutes(column_name) %}
-- Parse ISO8601 duration (PT2H25M, PT15M) to minutes
COALESCE(
  (regexp_match({{ column_name }}, '(\d+)H'))[1]::int, 0
) * 60 + COALESCE(
  (regexp_match({{ column_name }}, '(\d+)M'))[1]::int, 0
)
{% endmacro %}
