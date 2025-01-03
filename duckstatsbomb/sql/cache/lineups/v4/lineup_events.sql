with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                '[{"team_id": "integer",
                   "team_name": "varchar",
                   "events": "struct(player_id ubigint, player_name varchar, period ubigint, \"timestamp\" time, type varchar, outcome varchar)[]"
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
        json.team_id,
        json.team_name,
        unnest(json.events).player_id as player_id,
        unnest(json.events).player_name as player_name,
        unnest(json.events).period as period,
        unnest(json.events).timestamp as timestamp,
        unnest(json.events).type as type,
        unnest(json.events).outcome as outcome
    from
        raw_json
)
select
    *
from
    final
