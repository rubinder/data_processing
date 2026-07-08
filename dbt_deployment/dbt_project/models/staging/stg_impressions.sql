with source as (
    select * from {{ source('raw', 'impressions') }}
),

cleaned as (
    select
        user_id,
        impression_id,
        page_type,
        -- Safe casts: a malformed date/time yields NULL instead of aborting
        -- the whole model. NULLs are then caught by the not_null tests
        -- rather than crashing the run on a single bad row.
        {{ safe_to_date('date') }} as event_date,
        hour,
        min as minute,
        second,
        event_type,
        {{ safe_event_timestamp('date', 'hour', 'min', 'second') }}
            as event_timestamp
    from source
)

select * from cleaned
