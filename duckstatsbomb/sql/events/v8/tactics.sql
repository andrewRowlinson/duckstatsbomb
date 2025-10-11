with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            filename = true,
            format = 'array',
            columns = {"id": "varchar",
                   "index": "integer",
                   "period": "integer",
                   "timestamp": "time",
                   "minute": "integer",
                   "second": "integer",
                   "type": "struct(id ubigint, name varchar)",
                   "team": "struct(id ubigint, name varchar)",
                   "tactics": "struct(formation varchar, lineup struct(jersey_number integer, player struct(id integer, name varchar), position struct(id integer, name varchar))[])"
                  }
            )
),
final as (
    select
        cast(
            split(split(filename, '/') [-1], '.') [1] as integer
        ) as match_id,
        id as event_uuid,
        type.name as type_name,
        index,
        period,
        timestamp,
        minute,
        second,
        type.id as type_id,
        team.id as team_id,
        team.name as team_name,
        tactics.formation as formation,
        unnest(tactics.lineup).jersey_number as jersey_number,
        unnest(tactics.lineup).player.id as player_id,
        unnest(tactics.lineup).player.name as player_name,
        unnest(tactics.lineup).position.id as position_id,
        unnest(tactics.lineup).position.name as position_name
    from
        raw_json
    where
        type.name in ('Starting XI', 'Tactical Shift')
)
select
    *
from
    final
