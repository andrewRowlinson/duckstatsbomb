-- One row per file: its url and its JSON as raw text
-- The maximum object size of 128mb is larger than any Hudl StatsBomb file.
select
    filename as url,
    json::varchar as content
from
    read_json_objects(
        $urls,
        format = 'unstructured',
        filename = true,
        maximum_object_size = 128 * 1024 * 1024
    )
