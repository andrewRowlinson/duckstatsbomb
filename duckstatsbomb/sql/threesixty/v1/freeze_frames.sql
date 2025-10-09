with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            filename = true,
            format = 'array',
            columns = {"event_uuid": "varchar",
                   "freeze_frame": "struct(teammate boolean, actor boolean, keeper boolean, location double[])[]"
                   }
        )
),
final as (
    select
        cast(
            split(split(filename, '/') [-1], '.') [1] as integer
        ) as match_id,
        event_uuid,
        unnest(freeze_frame).location[1] as x,
        unnest(freeze_frame).location[2] as y,
        unnest(freeze_frame).teammate as teammate,
        unnest(freeze_frame).actor as actor,
        unnest(freeze_frame).keeper as keeper
    from
        raw_json
)
select
    *
from
    final
