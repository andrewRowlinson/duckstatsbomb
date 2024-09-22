with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                '[{"id": "varchar",
                   "type": "struct(name varchar)",
                   "shot": "struct(freeze_frame struct(location double[], player struct(id integer, name varchar), position struct(id integer, name varchar), teammate boolean)[])"
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
        cast(split(split(url, '/') [-1], '.') [1] as integer) as match_id,
        json.id as event_uuid,
        unnest(json.shot.freeze_frame).location [1] as x,
        unnest(json.shot.freeze_frame).location [2] as y,
        unnest(json.shot.freeze_frame).player.id as player_id,
        unnest(json.shot.freeze_frame).player.name as player_name,
        unnest(json.shot.freeze_frame).position.id as position_id,
        unnest(json.shot.freeze_frame).position.name as position_name,
        unnest(json.shot.freeze_frame).teammate as teammate
    from
        raw_json
    where
        json.type.name = 'Shot'
)
select
    *
from
    final