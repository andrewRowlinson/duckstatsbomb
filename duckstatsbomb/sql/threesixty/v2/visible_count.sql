with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                '[{"event_uuid": "varchar",
                   "visible_player_counts": "struct(team_id ubigint, count ubigint)[]"
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
    unnest(json.visible_player_counts).team_id as team_id,
    unnest(json.visible_player_counts).count as visible_player_count,
from
    raw_json
