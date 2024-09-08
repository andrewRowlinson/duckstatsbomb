with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
               '[{"event_uuid": "varchar",
                   "visible_area": "double[]"
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
)
select
    cast(split(split(filename, '/') [-1], '.') [1] as integer) as match_id,
    json.event_uuid,
    json.visible_area
from
    raw_json
