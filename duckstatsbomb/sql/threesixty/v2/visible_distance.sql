with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                '[{"event_uuid": "varchar",
                   "distances_from_edge_of_visible_area": "struct(point_id ubigint, distance double)[]"
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
    unnest(json.distances_from_edge_of_visible_area).point_id as point_id,
    unnest(json.distances_from_edge_of_visible_area).distance as distance
from
    raw_json
