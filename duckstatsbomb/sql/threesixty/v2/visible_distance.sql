with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            format = 'array',
            columns = {event_uuid: varchar,
            distances_from_edge_of_visible_area: 'struct(point_id ubigint, distance double)[]'},
            filename = true
        )
)
select
    cast(split(split(filename, '/') [-1], '.') [1] as integer) as match_id,
    event_uuid,
    unnest(distances_from_edge_of_visible_area).point_id as point_id,
    unnest(distances_from_edge_of_visible_area).distance as distance
from
    raw_json;