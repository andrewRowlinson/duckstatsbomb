with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                -- columns from the StatsBomb docs, but excluding tactics, related_events, and shot.freeze_frame as many to one relationships
                -- these are instead handled seperately to create their own dataframes.
                -- in the docs counterpress is within the event type objects
                -- however, counterpress appears to be outside of these events, i.e. not within 50_50 event type ibject
                -- included this both inside the event type objects and outside just in case
                -- in the open-data there are some boolean columns left_foot, right_foot, head, other for clearance
                -- these are covered by body_part so ignored (columns are not in the docs)
                -- open data also has the following boolean columns for goalkeeper
                -- shot_saved_to_post, shot_saved_off_target, punched_out, lost_out, success_out, lost_in_play, success_in_play, penalty_saved_to_post, saved_to_post
                -- ignored as not in the official spec and covered by type and outcome columns
                -- open data pass columns also has inswinging, outswinging, through_ball, straight
                -- ignored as not in the official spec and covered by technique column
                -- open data shot columns also has saved_off_target, saved_to_post, kick_off
                -- ignored as not in the official spec and covered by other columns (type/ outcome)
                -- added some additional columns not in the docs: goalkeeper end_location,
                -- pass aerial_won, pass no_touch, and shot redirect
                '[{"id": "varchar",
                   "index": "integer",
                   "period": "integer",
                   "timestamp": "time",
                   "minute": "integer",
                   "second": "integer",
                   "type": "struct(id ubigint, name varchar)",
                   "possession": "integer",
                   "possession_team": "struct(id ubigint, name varchar)",
                   "play_pattern": "struct(id ubigint, name varchar)",
                   "team": "struct(id ubigint, name varchar)",
                   "player": "struct(id ubigint, name varchar)",
                   "position": "struct(id ubigint, name varchar)",
                   "location": "double[]",
                   "duration": "double",
                   "under_pressure": "boolean",
                   "off_camera": "boolean",
                   "out": "boolean",
                   "tactics": "struct(formation varchar)",
                   "obv_for_after": "double",
                   "obv_for_before": "double",
                   "obv_for_net": "double",
                   "obv_against_after": "double",
                   "obv_against_before": "double",
                   "obv_against_net": "double",
                   "obv_total_net": "double",
                   "counterpress": "boolean",
                   "50_50": "struct(outcome struct(id ubigint, name varchar), counterpress boolean)",
                   "bad_behaviour": "struct(card struct(id ubigint, name varchar))",
                   "ball_receipt": "struct(outcome struct(id ubigint, name varchar))",
                   "ball_recovery": "struct(offensive boolean, recovery_failure boolean)",
                   "block": "struct(deflection boolean, offensive boolean, save_block boolean, counterpress boolean)",
                   "carry": "struct(end_location double[])",
                   "clearance": "struct(aerial_won boolean, body_part struct(id ubigint, name varchar))",
                   "dribble": "struct(overrun boolean, nutmeg boolean, outcome struct(id ubigint, name varchar), no_touch boolean)",
                   "dribbled_past": "struct(counterpress boolean)",
                   "duel": "struct(counterpress boolean, type struct(id ubigint, name varchar), outcome struct(id ubigint, name varchar))",
                   "foul_committed": "struct(counterpress boolean, offensive boolean, type struct(id ubigint, name varchar), advantage boolean, penalty boolean, card struct(id ubigint, name varchar))",
                   "foul_won": "struct(defensive boolean, advantage boolean, penalty boolean)",
                   "goalkeeper": "struct(position struct(id ubigint, name varchar), technique struct(id ubigint, name varchar), body_part struct(id ubigint, name varchar), type struct(id ubigint, name varchar), outcome struct(id ubigint, name varchar), end_location double[])",
                   "half_end": "struct(early_video_end boolean, match_suspended boolean)",
                   "half_start": "struct(late_video_start boolean)",
                   "injury_stoppage": "struct(in_chain boolean)",
                   "interception": "struct(outcome struct(id ubigint, name varchar))",
                   "miscontrol": "struct(aerial_won boolean)",
                   "pass": "struct(recipient struct(id ubigint, name varchar), length double, angle double, height struct(id ubigint, name varchar), end_location double[], assisted_shot_id varchar, backheel boolean, deflected boolean, miscommunication boolean, \"cross\" boolean, xclaim double, cut_back boolean, switch boolean, shot_assist boolean, goal_assist boolean, body_part struct(id ubigint, name varchar), type struct(id ubigint, name varchar), outcome struct(id ubigint, name varchar), technique struct(id ubigint, name varchar), pass_cluster_id ubigint, pass_cluster_label varchar, pass_cluster_probability double, pass_success_probability double, aerial_won boolean, no_touch boolean)",
                   "player_off": "struct(permanent boolean)",
                   "pressure": "struct(counterpress boolean)",
                   "shot": "struct(key_pass_id varchar, end_location double[], aerial_won boolean, follows_dribble boolean, first_time boolean, open_goal boolean, one_on_one boolean, statsbomb_xg double, gk_save_difficulty_xg double, shot_execution_xg double, shot_execution_xg_uplift double, gk_positioning_xg_suppression double, gk_shot_stopping_xg_suppression double, deflected boolean, technique struct(id ubigint, name varchar), shot_shot_assist boolean, shot_goal_assist boolean, body_part struct(id ubigint, name varchar), type struct(id ubigint, name varchar), outcome struct(id ubigint, name varchar), redirect boolean)",
                   "substitution": "struct(replacement struct(id ubigint, name varchar), outcome struct(id ubigint, name varchar))"
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
        json.index,
        json.period,
        json.timestamp,
        json.minute,
        json.second,
        json.type.id as type_id,
        replace(json.type.name, '*', '') as type_name,
        coalesce(json.duel.type.id, json.foul_committed.type.id, json.goalkeeper.type.id, json.pass.type.id, json.shot.type.id) as sub_type_id,
        coalesce(json.duel.type.name, json.foul_committed.type.name, json.goalkeeper.type.name, json.pass.type.name, json.shot.type.name) as sub_type_name,
        coalesce(json."50_50".outcome.id, json.ball_receipt.outcome.id, json.dribble.outcome.id, json.duel.outcome.id, json.goalkeeper.outcome.id, json.interception.outcome.id, json.pass.outcome.id, json.shot.outcome.id, json.substitution.outcome.id) as outcome_id,
        coalesce(json."50_50".outcome.name, json.ball_receipt.outcome.name, json.dribble.outcome.name, json.duel.outcome.name, json.goalkeeper.outcome.name, json.interception.outcome.name, json.pass.outcome.name, json.shot.outcome.name, json.substitution.outcome.name) as outcome_name,
        json.possession,
        json.possession_team.id as possession_team_id,
        json.possession_team.name as possession_team_name,
        json.play_pattern.id as play_pattern_id,
        json.play_pattern.name as play_pattern_name,
        json.team.id as team_id,
        json.team.name as team_name,
        json.player.id as player_id,
        json.player.name as player_name,
        json.position.id as position_id,
        json.position.name as position_name,
        json.location[1] as x,
        json.location[2] as y,
        json.location[3] as z,
        coalesce(json.carry.end_location[1], json.goalkeeper.end_location[1], json.pass.end_location[1], json.shot.end_location[1]) as end_x,
        coalesce(json.carry.end_location[2], json.goalkeeper.end_location[2], json.pass.end_location[2], json.shot.end_location[2]) as end_y,
        json.shot.end_location [3] as end_z,
        json.duration,
        json.under_pressure,
        json.off_camera,
        json.out,
        json.tactics.formation as tactics_formation,
        json.obv_for_after as obv_for_after,
        json.obv_for_before as obv_for_before,
        json.obv_for_net as obv_for_net,
        json.obv_against_after as obv_against_after,
        json.obv_against_before as obv_against_before,
        json.obv_against_net as obv_against_net,
        json.obv_total_net as obv_total_net,
        coalesce(json.counterpress, json."50_50".counterpress, json.block.counterpress, json.dribbled_past.counterpress, json.duel.counterpress, json.foul_committed.counterpress, json.pressure.counterpress) as counterpress,
        coalesce(json.block.offensive, json.ball_recovery.offensive, json.foul_committed.offensive) as offensive,
        coalesce(json.clearance.aerial_won,  json.miscontrol.aerial_won, json.pass.aerial_won, json.shot.aerial_won) as aerial_won,
        coalesce(json.clearance.body_part.id, json.goalkeeper.body_part.id, json.pass.body_part.id, json.shot.body_part.id) as body_part_id,
        coalesce(json.clearance.body_part.name, json.goalkeeper.body_part.name, json.pass.body_part.name, json.shot.body_part.name) as body_part_name,
        coalesce(json.goalkeeper.technique.id, json.pass.technique.id, json.shot.technique.id) as technique_id,
        coalesce(json.goalkeeper.technique.name, json.pass.technique.name, json.shot.technique.name) as technique_name,
        coalesce(json.dribble.no_touch, json.pass.no_touch) as no_touch,
        coalesce(json.pass.deflected, json.shot.deflected) as deflected,
        json.bad_behaviour.card.id as bad_behaviour_card_id,
        json.bad_behaviour.card.name as bad_behaviour_card_name,
        json.ball_recovery.recovery_failure as ball_recovery_recovery_failure,
        json.block.deflection as block_deflection,
        json.block.save_block as block_save_block,
        json.dribble.overrun as dribble_overrun,
        json.dribble.nutmeg as dribble_nutmeg,
        coalesce(json.foul_committed.advantage, json.foul_won.advantage) as foul_advantage,
        coalesce(json.foul_committed.penalty, json.foul_won.penalty) as foul_penalty,
        json.foul_committed.card.id as foul_card_id,
        json.foul_committed.card.name as foul_card_name,
        json.foul_won.defensive as foul_defensive,
        json.goalkeeper.position.id as goalkeeper_position_id,
        json.goalkeeper.position.name as goalkeeper_position_name,
        json.half_end.early_video_end as half_end_early_video_end,
        json.half_end.match_suspended as half_end_match_suspended,
        json.half_start.late_video_start as half_start_late_video_start,
        json.injury_stoppage.in_chain as injury_stoppage_in_chain,
        json.pass.recipient.id as pass_recipient_id,
        json.pass.recipient.name as pass_recipient_name,
        json.pass.length as pass_length,
        json.pass.angle as pass_angle,
        json.pass.height.id as pass_height_id,
        json.pass.height.name as pass_height_name,
        json.pass.assisted_shot_id as pass_assisted_shot_id,
        json.pass.backheel as pass_backheel,
        json.pass.miscommunication as pass_miscommunication,
        json.pass."cross" as pass_cross,
        json.pass.xclaim as pass_xclaim,
        json.pass.cut_back as pass_cut_back,
        json.pass.switch as pass_switch,
        json.pass.pass_cluster_id as pass_cluster_id,
        json.pass.pass_cluster_label as pass_cluster_label,
        json.pass.pass_cluster_probability as pass_cluster_probability,
        json.pass.pass_success_probability as pass_success_probability,
        json.player_off.permanent as player_off_permanent,
        coalesce(json.pass.shot_assist, json.shot.shot_shot_assist) as shot_assist,
        coalesce(json.pass.goal_assist, json.shot.shot_goal_assist) as goal_assist,
        json.shot.key_pass_id as shot_key_pass_id,
        json.shot.follows_dribble as shot_follows_dribble,
        json.shot.first_time as shot_first_time,
        json.shot.open_goal as shot_open_goal,
        json.shot.one_on_one as shot_one_on_one,
        json.shot.statsbomb_xg as shot_statsbomb_xg,
        json.shot.gk_save_difficulty_xg as shot_gk_save_difficulty_xg,
        json.shot.shot_execution_xg as shot_execution_xg,
        json.shot.shot_execution_xg_uplift as shot_execution_xg_uplift,
        json.shot.gk_positioning_xg_suppression as shot_gk_positioning_xg_suppression,
        json.shot.gk_shot_stopping_xg_suppression as shot_gk_shot_stopping_xg_suppression,
        json.shot.redirect as shot_redirect,
        json.substitution.replacement.id as substitution_replacement_id,
        json.substitution.replacement.name as substitution_replacement_name
    from
        raw_json
)
select
    *
from
    final
