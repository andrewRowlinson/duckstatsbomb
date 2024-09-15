with raw_json as (
    select
        unnest(
            from_json(
                json(_decoded_content),
                '[{"competition_id": "integer",
                   "season_id": "integer",
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
)
select
    json.competition_id,
    json.season_id
from
    raw_json
where
    json.competition_id = $competition_id
