-- Download files in parallel through duckdb as raw JSON.
-- the maximum object size is chosen as 128mb to be larger than the typical Hudl StatsBomb file.
select
    filename,
    json::varchar as content
from
    read_json_objects(
        $urls,
        format = 'unstructured',
        filename = true,
        maximum_object_size = 128 * 1024 * 1024
    )
