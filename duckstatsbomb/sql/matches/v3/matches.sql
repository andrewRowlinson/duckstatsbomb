with raw_json as (
    select
        unnest(
            from_json(
                json(_decoded_content),
                '[{"match_id": "integer",
                   "competition": "struct(competition_id integer, country_name varchar, competition_name varchar)",
                   "season": "struct(season_id integer, season_name varchar)",
                   "match_date": "date",
                   "kick_off": "time",
                   "stadium": "struct(id integer, name varchar, country struct(id integer, name varchar))",
                   "referee": "struct(id integer, name varchar, country struct(id integer, name varchar))",
                   "home_team": "struct(home_team_id integer, home_team_name varchar, home_team_gender varchar, home_team_group varchar, country struct(id integer, name varchar), managers struct(id varchar, name varchar, nickname varchar, dob date, country struct(id integer, name varchar))[])",
                   "away_team": "struct(away_team_id integer, away_team_name varchar, away_team_gender varchar, away_team_group varchar, country struct(id integer, name varchar), managers struct(id varchar, name varchar, nickname varchar, dob date, country struct(id integer, name varchar))[])",
                   "home_score": "integer",
                   "away_score": "integer",
                   "match_status": "varchar",
                   "match_status_360": "varchar",
                   "match_week": "integer",
                   "competition_stage": "struct(id integer, name varchar)",
                   "last_updated": "varchar",
                   "last_updated_360": "varchar",
                   "metadata": "struct(data_version varchar, shot_fidelity_version varchar, xy_fidelity_version varchar)",
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
        json.match_id,
        json.competition.competition_id,
        json.competition.competition_name,
        json.competition.country_name as competition_country_name,
        json.season.season_id,
        json.season.season_name,
        json.match_date,
        json.kick_off,
        json.stadium.id as stadium_id,
        json.stadium.name as stadium_name,
        json.stadium.country.id as stadium_country_id,
        json.stadium.country.name as stadium_country_name,
        json.referee.id as referee_id,
        json.referee.name as referee_name,
        json.referee.country.id as referee_country_id,
        json.referee.country.name as referee_country_name,
        json.home_team.home_team_id,
        json.home_team.home_team_name,
        json.home_team.home_team_gender,
        json.home_team.home_team_group,
        json.home_team.country.id as home_team_country_id,
        json.home_team.country.name as home_team_country_name,
        json.home_team.managers[1].id as home_team_manager_id,
        json.home_team.managers[1].name as home_team_manager_name,
        json.home_team.managers[1].nickname as home_team_manager_nickname,
        json.home_team.managers[1].dob as home_team_manager_dob,
        json.home_team.managers[1].country.id as home_team_manager_country_id,
        json.home_team.managers[1].country.name as home_team_manager_country_name,
        json.away_team.away_team_id,
        json.away_team.away_team_name,
        json.away_team.away_team_gender,
        json.away_team.away_team_group,
        json.away_team.country.id as away_team_country_id,
        json.away_team.country.name as away_team_country_name,
        json.away_team.managers[1].id as away_team_manager_id,
        json.away_team.managers[1].name as away_team_manager_name,
        json.away_team.managers[1].nickname as away_team_manager_nickname,
        json.away_team.managers[1].dob as away_team_manager_dob,
        json.away_team.managers[1].country.id as away_team_manager_country_id,
        json.away_team.managers[1].country.name as away_team_manager_country_name,
        json.home_score,
        json.away_score,
        json.match_status,
        json.match_status_360,
        json.match_week,
        json.competition_stage.id as competition_stage_id,
        json.competition_stage.name as competition_stage_name,
        case
            when json.last_updated is null then null
            else cast(
                left(
                    concat(replace(json.last_updated, 'T', ' '), ':00'),
                    19
                ) as timestamp
            )
        end as last_updated,
        case
            when json.last_updated_360 is null then null
            else cast(
                left(
                    concat(replace(json.last_updated_360, 'T', ' '), ':00'),
                    19
                ) as timestamp
            )
        end as last_updated_360,
        json.metadata.data_version as metadata_data_version,
        json.metadata.shot_fidelity_version as metadata_shot_fidelity_version,
        json.metadata.xy_fidelity_version as metadata_xy_fidelity_version
    from
        raw_json
)
select
    *
from
    final
