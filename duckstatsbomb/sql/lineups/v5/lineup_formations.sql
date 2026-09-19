with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            filename = true,
            format = 'array',
            columns = {"team_id": "integer",
                   "team_name": "varchar",
                   "formations": 'struct(period ubigint, "timestamp" time, reason varchar, formation varchar)[]'
                   }
            )
),
final as (
    select
        cast(parse_filename(filename, true, 'both_slash') as integer) as match_id,
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
    final
