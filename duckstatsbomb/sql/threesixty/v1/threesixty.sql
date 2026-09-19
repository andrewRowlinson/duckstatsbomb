with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            filename = true,
            format = 'array',
            columns = {"event_uuid": "varchar",
                   "visible_area": "double[]"
                   }
            )
),
final as (
    select
        cast(
            parse_filename(filename, true, 'both_slash') as integer
        ) as match_id,
        event_uuid,
        visible_area
    from
        raw_json
)
select
    *
from
    final
