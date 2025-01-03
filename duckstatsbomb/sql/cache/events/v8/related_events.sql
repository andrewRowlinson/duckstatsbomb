with raw_json as (
    select
        url,
        unnest(
            from_json(
                json(_decoded_content),
                '[{"id": "varchar",
                   "index": "integer",
                   "type": "struct(id ubigint, name varchar)",
                   "related_events": "VARCHAR[]"
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
related as (
    select
        cast(split(split(url, '/') [-1], '.') [1] as integer) as match_id,
        json.id as event_uuid,
        json.index,
        replace(json.type.name, '*', '') as type_name,
        unnest(json.related_events) as event_uuid_related
    from
        raw_json
),
events as (
    select
        json.id as event_uuid_related,
        json.index as index_related,
        replace(json.type.name, '*', '') as type_name_related
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
