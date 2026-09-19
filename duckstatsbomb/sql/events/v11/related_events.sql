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
-- the links as StatsBomb wrote them. Some are only recorded one way,
-- e.g. a carry lists the pass before it, but the pass does not list the carry
links as (
    select
        match_id,
        event_uuid,
        unnest(related_events) as event_uuid_related
    from
        events
),
-- add the reverse of every link, so each pair is related both ways
both_ways as (
    select
        match_id,
        event_uuid,
        event_uuid_related
    from
        links
    union
    select
        match_id,
        event_uuid_related as event_uuid,
        event_uuid as event_uuid_related
    from
        links
),
final as (
    select
        both_ways.match_id,
        both_ways.event_uuid,
        events.index,
        events.type_name,
        both_ways.event_uuid_related,
        related.index as index_related,
        related.type_name as type_name_related
    from
        both_ways
        join events on both_ways.match_id = events.match_id
        and both_ways.event_uuid = events.event_uuid
        join events as related on both_ways.match_id = related.match_id
        and both_ways.event_uuid_related = related.event_uuid
)
select
    *
from
    final
