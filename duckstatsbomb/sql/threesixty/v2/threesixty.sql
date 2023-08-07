with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            format = 'array',
            columns = {event_uuid: varchar,
            visible_area: 'double[]',
            line_breaking_pass: boolean,
            num_defenders_on_goal_side_of_actor: ubigint,
            distance_to_nearest_defender: double,
            ball_receipt_in_space: boolean,
            ball_receipt_exceeds_distance: ubigint},
            filename = true
        )
)
select
    cast(split(split(filename, '/') [-1], '.') [1] as integer) as match_id,
    event_uuid,
    visible_area,
    line_breaking_pass,
    num_defenders_on_goal_side_of_actor,
    distance_to_nearest_defender,
    ball_receipt_in_space,
    ball_receipt_exceeds_distance
from
    raw_json;
