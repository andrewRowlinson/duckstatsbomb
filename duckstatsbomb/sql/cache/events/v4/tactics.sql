with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                '[{"id": "varchar",
                   "index": "integer",
                   "period": "integer",
                   "timestamp": "time",
                   "minute": "integer",
                   "second": "integer",
                   "type": "struct(id ubigint, name varchar)",
                   "team": "struct(id ubigint, name varchar)",
                   "tactics": "struct(formation varchar, lineup struct(jersey_number integer, player struct(id integer, name varchar), position struct(id integer, name varchar))[])"
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
        json.id as event_uuid,
        json.type.name as type_name,
        json.index,
        json.period,
        json.timestamp,
        json.minute,
        json.second,
        json.type.id as type_id,
        json.team.id as team_id,
        json.team.name as team_name,
        json.tactics.formation as formation,
        unnest(json.tactics.lineup).jersey_number as jersey_number,
        unnest(json.tactics.lineup).player.id as player_id,
        unnest(json.tactics.lineup).player.name as player_name,
        unnest(json.tactics.lineup).position.id as position_id,
        unnest(json.tactics.lineup).position.name as position_name
    from
        raw_json
    where
        json.type.name in ('Starting XI', 'Tactical Shift')
)
select
    *
from
    final
