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
                   "lineup": "struct(player_id ubigint, player_name varchar, player_nickname varchar, jersey_number ubigint, country struct(id ubigint, name varchar))[]"
                   }
            )
),
final as (
    select
        cast(
            split(split(filename, '/') [-1], '.') [1] as integer
        ) as match_id,
        team_id,
        team_name,
        unnest(lineup).player_id as player_id,
        unnest(lineup).player_name as player_name,
        unnest(lineup).player_nickname as player_nickname,
        unnest(lineup).jersey_number as jersey_number,
        unnest(lineup).country.id as country_id,
        unnest(lineup).country.name as country_name,
    from
        raw_json
)
select
    *
from
    final
