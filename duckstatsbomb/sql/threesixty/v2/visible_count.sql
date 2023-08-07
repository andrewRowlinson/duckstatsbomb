with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            format = 'array',
            columns = {event_uuid: varchar,
            visible_player_counts: 'struct(team_id ubigint, count ubigint)[]'},
            filename = true
        )
)
select
    cast(split(split(filename, '/') [-1], '.') [1] as integer) as match_id,
    event_uuid,
    unnest(visible_player_counts).team_id as team_id,
    unnest(visible_player_counts).count as visible_player_count,
from
    raw_json;