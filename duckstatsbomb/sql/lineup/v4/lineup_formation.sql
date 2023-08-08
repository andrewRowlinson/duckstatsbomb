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
            formations: 'struct(
                           period ubigint,
                           "timestamp" time,
                           reason varchar,
                           formation varchar
                           )[]'
                           }
        )
),
final as (
    select
        cast(split(split(filename, '/') [-1], '.') [1] as integer) as match_id,
        team_id,
        team_name,
        unnest(formations).period as period,
        unnest(formations).timestamp as timestamp,
        unnest(formations).reason as reason,
        unnest(formations).formation as formation
    from
        raw_json
)
select
    *
from
    final;