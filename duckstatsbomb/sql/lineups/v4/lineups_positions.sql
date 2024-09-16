with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                '[{"team_id": "integer",
                   "team_name": "varchar",
                   "lineup": "struct(player_id ubigint, player_name varchar, positions struct(position_id ubigint, position varchar, \"from\" time, \"to\" time, from_period ubigint, to_period ubigint, start_reason varchar, end_reason varchar)[])[]"
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
        unnest(json.lineup).player_id as player_id,
        unnest(json.lineup).player_name as player_name,
        unnest(json.lineup).positions as positions
    from
        raw_json
)
select
    * exclude positions,
    unnest(positions).position_id as position_id,
    unnest(positions).position as position_name,
    unnest(positions).from as from_timestamp,
    unnest(positions).to as to_timestamp,
    unnest(positions).from_period as from_period,
    unnest(positions).to_period as to_period,
    unnest(positions).start_reason as start_reason,
    unnest(positions).end_reason as end_reason
from
    final
