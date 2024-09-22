with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
               '[{"event_uuid": "varchar",
                   "visible_area": "double[]",
                   "line_breaking_pass": "boolean",
                   "num_defenders_on_goal_side_of_actor": "ubigint",
                   "distance_to_nearest_defender": "double",
                   "ball_receipt_in_space": "boolean",
                   "ball_receipt_exceeds_distance": "ubigint"
                   }]'
            )
        ) as json
    from
        (
            select
                *
            from
                read_json($filename, maximum_object_size=25000000)
        )
)
select
    cast(split(split(url, '/') [-1], '.') [1] as integer) as match_id,
    json.event_uuid,
    json.visible_area,
    json.line_breaking_pass,
    json.num_defenders_on_goal_side_of_actor,
    json.distance_to_nearest_defender,
    json.ball_receipt_in_space,
    json.ball_receipt_exceeds_distance
from
    raw_json;
