with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                '[{"event_uuid": "varchar",
                   "freeze_frame": "struct(teammate boolean, actor boolean, keeper boolean, location double[])[]"
                   }]'
            )
        ) as json
    from
        (
            select
                *
            from
                read_json($filename)
        )
),
final as (
select
        cast(split(split(url, '/') [-1], '.') [1] as integer) as match_id,
        json.event_uuid,
        unnest(json.freeze_frame).location[1] as x,
        unnest(json.freeze_frame).location[2] as y,
        unnest(json.freeze_frame).teammate,
        unnest(json.freeze_frame).actor,
        unnest(json.freeze_frame).keeper
from raw_json
)
select
    *
from
    final
