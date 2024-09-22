with raw_json as (
    select
        unnest(
            from_json(
                json(_decoded_content),
                '[{"match_id": "integer"}]'
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
    json.match_id
from
    raw_json
