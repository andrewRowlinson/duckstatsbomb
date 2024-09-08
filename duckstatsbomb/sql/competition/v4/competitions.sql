with raw_json as (
    select
        unnest(
            from_json(
                json(_decoded_content),
                '[{"competition_id": "integer",
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
parsed_json as (
    select
        json.competition_id,
        json.season_id,
        json.competition_name,
        json.competition_gender,
        json.competition_youth,
        json.competition_international,
        json.country_name,
        json.season_name,
        json.match_updated,
        json.match_updated_360,
        json.match_available,
        json.match_available_360
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
