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
            lineup: 'struct(
                        player_id ubigint,
                        player_name varchar,
                        player_nickname varchar,
                        player_gender varchar,
                        player_weight double,
                        player_height double,
                        birth_date date,
                        jersey_number ubigint,
                        country struct(id ubigint, "name" varchar),
                        "stats" json
                        )[]'}
        )
),
final as (
    select
        cast(split(split(filename, '/') [-1], '.') [1] as integer) as match_id,
        team_id,
        team_name,
        unnest(lineup).player_id as player_id,
        unnest(lineup).player_name as player_name,
        unnest(lineup).player_nickname as player_nickname,
        unnest(lineup).player_gender as player_gender,
        unnest(lineup).player_weight as player_weight,
        unnest(lineup).player_height as player_height,
        unnest(lineup).birth_date as birth_date,
        unnest(lineup).jersey_number as jersey_number,
        unnest(lineup).country.id as country_id,
        unnest(lineup).country.name as country_name,
        -- needed to do this way as sometimes stats is an empty list [] and sometimes a dict {}
        cast(json_extract(unnest(lineup).stats, 'goals') as integer) as goals,
        cast(json_extract(unnest(lineup).stats, 'own_goals') as integer) as own_goals,
        cast(json_extract(unnest(lineup).stats, 'assists') as integer) as assists,
        cast(json_extract(unnest(lineup).stats, 'penalties_scored') as integer) as penalties_scored,
        cast(json_extract(unnest(lineup).stats, 'penalties_missed') as integer) as penalties_missed,
        cast(json_extract(unnest(lineup).stats, 'penalties_saved') as integer) as penalties_saved
    from
        raw_json
)
select
    *
from
    final;