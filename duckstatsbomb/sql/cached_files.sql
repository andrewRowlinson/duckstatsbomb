-- Read the cached files metadata only (the file contents are not read)
select
    filename as path,
    size,
    last_modified at time zone 'UTC' as downloaded_at
from
    read_blob($filename)
order by
    path
