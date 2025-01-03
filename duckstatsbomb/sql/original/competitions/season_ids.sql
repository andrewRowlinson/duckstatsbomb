with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            format = 'array',
            columns = {"competition_id": "integer",
                   "season_id": "integer",
                   }
            )
)
select
    competition_id,
    season_id
from
    raw_json
where
    competition_id = $competition_id
