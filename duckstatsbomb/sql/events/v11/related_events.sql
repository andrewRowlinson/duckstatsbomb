with raw_json as (
    select
        *
    from
        read_json(
            $filename,
            filename = true,
            format = 'array',
            columns = {"id": "varchar",
                   "index": "integer",
                   "type": "struct(id ubigint, name varchar)",
                   "related_events": "VARCHAR[]"
                  }
            )
),
events as (
    select
        cast(
            parse_filename(filename, true, 'both_slash') as integer
        ) as match_id,
        id as event_uuid,
        index,
        replace(type.name, '*', '') as type_name,
        related_events
    from
        raw_json
),
-- the links as StatsBomb wrote them, keeping the source event's index and type
links as (
    select
        match_id,
        event_uuid,
        index,
        type_name,
        unnest(related_events) as event_uuid_related
    from
        events
),
-- look up the related event's index and type. This is the only join onto
-- events: with two, the optimizer's low row estimate for read_json led it
-- to pair every event in a match with every other event first
resolved as (
    select
        links.match_id,
        links.event_uuid,
        links.index,
        links.type_name,
        links.event_uuid_related,
        related.index as index_related,
        related.type_name as type_name_related
    from
        links
        join events as related on links.match_id = related.match_id
        and links.event_uuid_related = related.event_uuid
),
-- the reverse of each link, unless StatsBomb recorded it both ways already,
-- e.g. a carry lists the pass before it, but the pass does not list the carry
reverse as (
    select
        backward.match_id,
        backward.event_uuid_related as event_uuid,
        backward.index_related as index,
        backward.type_name_related as type_name,
        backward.event_uuid as event_uuid_related,
        backward.index as index_related,
        backward.type_name as type_name_related
    from
        resolved as backward
        anti join resolved as forward on forward.match_id = backward.match_id
        and forward.index = backward.index_related
        and forward.index_related = backward.index
)
select
    *
from
    resolved
union all
select
    *
from
    reverse
