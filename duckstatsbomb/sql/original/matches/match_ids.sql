with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            format = 'array',
            columns = {"match_id": "integer"}
            )
)
select
    match_id
from
    raw_json
