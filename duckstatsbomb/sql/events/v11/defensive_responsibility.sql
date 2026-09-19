with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            filename = true,
            format = 'array',
            columns = {"id": "varchar",
                   "defensive_responsibility": "struct(players struct(player_id integer, probability double, pressure_like_probability double, interception_like_probability double)[])"
                  }
            )
),
final as (
    select
        cast(
            parse_filename(filename, true, 'both_slash') as integer
        ) as match_id,
        id as event_uuid,
        unnest(defensive_responsibility.players).player_id as player_id,
        unnest(defensive_responsibility.players).probability as probability,
        unnest(defensive_responsibility.players).pressure_like_probability as pressure_like_probability,
        unnest(defensive_responsibility.players).interception_like_probability as interception_like_probability
    from
        raw_json
    where
        defensive_responsibility is not null
)
select
    *
from
    final
