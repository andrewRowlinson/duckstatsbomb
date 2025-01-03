with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            format = 'array',
            columns = {"competition_id": "integer",
                   "season_id": "integer",
                   "competition_name": "varchar",
                   "competition_gender": "varchar",
                   "competition_youth": "boolean",
                   "competition_international": "boolean",
                   "country_name": "varchar",
                   "season_name": "varchar",
                   "match_updated": "varchar",
                   "match_updated_360": "varchar",
                   "match_available": "varchar",
                   "match_available_360": "varchar"
                   }
            )
),
parsed_json as (
    select
        competition_id,
        season_id,
        competition_name,
        competition_gender,
        competition_youth,
        competition_international,
        country_name,
        season_name,
        match_updated,
        match_updated_360,
        match_available,
        match_available_360
    from
        raw_json
),
final as (
    select
        * replace(
        case when match_updated is null then null else
        cast(left(concat(replace(match_updated, 'T', ' '), ':00'), 19) as timestamp)
        end as match_updated,
        case when match_available is null then null
        else cast(left(concat(replace(match_available, 'T', ' '), ':00'), 19) as timestamp)
        end as match_available,
        cast(match_available_360 as timestamp) as match_available_360,
        cast(match_updated_360 as timestamp) as match_updated_360
        )
    from
        parsed_json

)
select
    *
from
    final
