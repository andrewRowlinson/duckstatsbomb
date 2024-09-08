with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            format = 'array',
            filename = true,
            columns = {team_id: integer,
            team_name: varchar,
            events: 'struct(
                       player_id ubigint,
                       player_name varchar,
                       period ubigint,
                       timestamp time,
                       type varchar,
                       outcome varchar
                       )[]'}
        )
),
final as (
    select
        cast(split(split(filename, '/') [-1], '.') [1] as integer) as match_id,
        team_id,
        team_name,
        unnest(events).player_id as player_id,
        unnest(events).player_name as player_name,
        unnest(events).period as period,
        unnest(events).timestamp as timestamp,
        unnest(events).type as type,
        unnest(events).outcome as outcome
    from
        raw_json
)
select
    *
from
    final;