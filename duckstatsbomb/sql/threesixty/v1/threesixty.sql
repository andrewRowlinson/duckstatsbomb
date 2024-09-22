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
                read_json($filename, maximum_object_size=25000000)
        )
)
select
    cast(split(split(url, '/') [-1], '.') [1] as integer) as match_id,
    json.event_uuid,
    json.visible_area
from
    raw_json
