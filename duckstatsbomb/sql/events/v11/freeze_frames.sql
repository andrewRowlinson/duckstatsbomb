with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            filename = true,
            format = 'array',
            columns = {"id": "varchar",
                   "type": "struct(name varchar)",
                   "shot": "struct(freeze_frame struct(location double[], player struct(id integer, name varchar), position struct(id integer, name varchar), teammate boolean)[])"
                  }
            )
),
final as (
    select
        cast(
            split(split(filename, '/') [-1], '.') [1] as integer
        ) as match_id,
        id as event_uuid,
        unnest(shot.freeze_frame).location [1] as x,
        unnest(shot.freeze_frame).location [2] as y,
        unnest(shot.freeze_frame).player.id as player_id,
        unnest(shot.freeze_frame).player.name as player_name,
        unnest(shot.freeze_frame).position.id as position_id,
        unnest(shot.freeze_frame).position.name as position_name,
        unnest(shot.freeze_frame).teammate as teammate
    from
        raw_json
    where
        type.name = 'Shot'
)
select
    *
from
    final
