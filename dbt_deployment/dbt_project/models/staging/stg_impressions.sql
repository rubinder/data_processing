with source as (
    select * from {{ source('raw', 'impressions') }}
),

cleaned as (
    select
        user_id,
        impression_id,
        page_type,
        date::date as event_date,
        hour,
        min as minute,
        second,
        event_type,
        make_timestamp(
            extract(year from date::date)::int,
            extract(month from date::date)::int,
            extract(day from date::date)::int,
            hour,
            min,
            second
        ) as event_timestamp
    from source
)

select * from cleaned
