with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            format = 'array',
            filename = true,
            columns = {
                team_id: integer,
                team_name: varchar,
                lineup: 'struct(player_id ubigint, player_name varchar, player_nickname varchar, positions struct(position_id ubigint, position varchar, "from" time, "to" time, from_period ubigint, to_period ubigint, start_reason varchar, end_reason varchar)[])[]'
            }
        )
),
final as (
    select
        cast(split(split(filename, '/') [-1], '.') [1] as integer) as match_id,
        team_id,
        team_name,
        unnest(lineup).player_id as player_id,
        unnest(lineup).player_name as player_name,
        unnest(lineup).positions as positions
    from
        raw_json
)
select
    * exclude positions,
    unnest(positions).position_id as position_id,
    unnest(positions).position as position_name,
    unnest(positions).from as from,
    unnest(positions).to as to,
    unnest(positions).from_period as from_period,
    unnest(positions).to_period as to_period,
    unnest(positions).start_reason as start_reason,
    unnest(positions).end_reason as end_reason
from
    final;