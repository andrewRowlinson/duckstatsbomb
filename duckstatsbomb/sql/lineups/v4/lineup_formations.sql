with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                '[{"team_id": "integer",
                   "team_name": "varchar",
                   "formations": "struct(period ubigint, \"timestamp\" time, reason varchar, formation varchar)[]"
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
        unnest(json.formations).period as period,
        unnest(json.formations).timestamp as timestamp,
        unnest(json.formations).reason as reason,
        unnest(json.formations).formation as formation
    from
        raw_json
)
select
    *
from
    final
