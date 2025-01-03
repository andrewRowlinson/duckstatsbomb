with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                '[{"team_id": "integer",
                   "team_name": "varchar",
                   "lineup": "struct(player_id ubigint, player_name varchar, player_nickname varchar, jersey_number ubigint, country struct(id ubigint, name varchar))[]"
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
        unnest(json.lineup).player_nickname as player_nickname,
        unnest(json.lineup).jersey_number as jersey_number,
        unnest(json.lineup).country.id as country_id,
        unnest(json.lineup).country.name as country_name,
    from
        raw_json
)
select
    *
from
    final
