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
related as (
    select
        cast(
            split(split(filename, '/') [-1], '.') [1] as integer
        ) as match_id,
        id as event_uuid,
        index,
        replace(type.name, '*', '') as type_name,
        unnest(related_events) as event_uuid_related
    from
        raw_json
),
events as (
    select
        id as event_uuid_related,
        index as index_related,
        replace(type.name, '*', '') as type_name_related
    from
        raw_json
),
final as (
    select
        related.*,
        events.* exclude event_uuid_related
    from
        related
        join events on related.event_uuid_related = events.event_uuid_related
)
select
    *
from
    final
