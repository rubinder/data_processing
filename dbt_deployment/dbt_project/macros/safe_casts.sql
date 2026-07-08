{#
    Postgres has no TRY_CAST, so a single malformed value in a VARCHAR source
    column (e.g. date = 'NOPE') would abort the whole model with a cast error.
    These macros validate before casting and yield NULL on bad input, so one
    bad row degrades to a null instead of failing the batch. Rows that produce
    a NULL key can then be surfaced by a not_null test rather than crashing dbt.
#}

{% macro safe_to_date(col) %}
    case
        when {{ col }} ~ '^\d{4}-\d{2}-\d{2}$'
            then to_date({{ col }}, 'YYYY-MM-DD')
        else null
    end
{% endmacro %}


{% macro safe_event_timestamp(date_col, hour_col, min_col, sec_col) %}
    case
        when {{ safe_to_date(date_col) }} is not null
            and {{ hour_col }} between 0 and 23
            and {{ min_col }} between 0 and 59
            and {{ sec_col }} between 0 and 59
        then make_timestamp(
            extract(year from {{ safe_to_date(date_col) }})::int,
            extract(month from {{ safe_to_date(date_col) }})::int,
            extract(day from {{ safe_to_date(date_col) }})::int,
            {{ hour_col }},
            {{ min_col }},
            {{ sec_col }}
        )
        else null
    end
{% endmacro %}
