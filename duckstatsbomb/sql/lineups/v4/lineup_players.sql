with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                '[{"team_id": "integer",
                   "team_name": "varchar",
                   "lineup": "struct(player_id ubigint, player_name varchar, player_nickname varchar, player_gender varchar, player_weight double, player_height double, birth_date date, jersey_number ubigint, country struct(id ubigint, name varchar), \"stats\" json)[]"
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
        unnest(json.lineup).player_gender as player_gender,
        unnest(json.lineup).player_weight as player_weight,
        unnest(json.lineup).player_height as player_height,
        unnest(json.lineup).birth_date as birth_date,
        unnest(json.lineup).jersey_number as jersey_number,
        unnest(json.lineup).country.id as country_id,
        unnest(json.lineup).country.name as country_name,
        -- needed to do this way as sometimes stats is an empty list [] and sometimes a dict {}
        cast(json_extract(unnest(json.lineup).stats, 'goals') as integer) as goals,
        cast(json_extract(unnest(json.lineup).stats, 'own_goals') as integer) as own_goals,
        cast(json_extract(unnest(json.lineup).stats, 'assists') as integer) as assists,
        cast(json_extract(unnest(json.lineup).stats, 'penalties_scored') as integer) as penalties_scored,
        cast(json_extract(unnest(json.lineup).stats, 'penalties_missed') as integer) as penalties_missed,
        cast(json_extract(unnest(json.lineup).stats, 'penalties_saved') as integer) as penalties_saved
    from
        raw_json
)
select
    *
from
    final
