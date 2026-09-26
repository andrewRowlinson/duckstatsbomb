"""The cache for downloaded Hudl StatsBomb files.

Currently supports a local cache, but could be expanded for cloud storage.

How a parser with cache_enabled=True loads match data:

1. The parser turns each url into a key, e.g. ``'v4/events/3788741.json'``, and
   asks the cache for the missing keys.
2. The parser looks up the urls of the missing keys and passes them to DuckDB
   in batches of one file per thread, which DuckDB downloads in parallel. Each
   batch returns one row per file, with a url column and a content column
   (JSON as a string), e.g.

   ==================================  ===================================
   url                                 content
   ==================================  ===================================
   https://.../api/v4/events/3788741   [{"id": "...", "index": 1, ...}, ...]
   https://.../api/v4/events/3788742   [{"id": "...", "index": 1, ...}, ...]
   ==================================  ===================================

3. The parser passes each row's content column to the cache, which writes it
   under the key. The DuckDB rows are not kept, so a file is only ever stored
   once.
4. Now every key is cached, the parser asks the cache for the file path of
   each key.
5. DuckDB reads the cached files with SQL.

The cache stores files, identifies missing files, deletes files,
identifies the timestamp of when files were written, and tells DuckDB the
location of the files. To cache files in an object store, e.g. S3,
subclass CacheBase and pass an instance of the new class to
Sbopen(cache=NewClass(...)) or Sbapi(cache=NewClass(...)).
"""

import datetime
import shutil
from abc import ABC, abstractmethod
from pathlib import Path

__all__ = ['CacheBase', 'LocalCache']


class CacheBase(ABC):
    """A cache backend stores files by key, e.g. ``'v4/events/3788741.json'``, and
    gives the path of each key for DuckDB to read, e.g. a local path or an
    object store url.
    """

    @abstractmethod
    def path(self, key):
        """Join a file's key onto the cache location.

        Parameters
        ----------
        key : str
            The file's location within the cache, e.g. ``'v4/events/3788741.json'``.

        Returns
        -------
        str
            The cache file path, e.g. ``'statsbomb_cache/v4/events/3788741.json'``.
        """

    @abstractmethod
    def missing(self, keys):
        """Keep the keys that are not cached.

        Parameters
        ----------
        keys : iterable of str

        Returns
        -------
        set of str
            The keys that are not cached.
        """

    @abstractmethod
    def write(self, key, content):
        """Write a downloaded file to the cache.

        Write the file so that it only exists under its key once it is complete.

        Parameters
        ----------
        key : str
        content : str
            The downloaded JSON text.
        """

    @abstractmethod
    def modified_at(self, keys):
        """The UTC time each cached file was written.

        Parameters
        ----------
        keys : iterable of str

        Returns
        -------
        dict
            Maps each key to a naive datetime.datetime (i.e. tzinfo=None)
            in UTC. It must be naive, as it is compared with the
            naive last_updated timestamps in the match data.
        """

    @abstractmethod
    def delete(self, keys):
        """Delete cached files, ignoring any that are not cached.

        Parameters
        ----------
        keys : iterable of str
        """

    @abstractmethod
    def delete_all(self):
        """Delete every cached file."""

    @abstractmethod
    def glob(self):
        """A glob of the cached files for DuckDB to read.

        Returns
        -------
        str or None
            None when nothing is cached, as DuckDB raises on a glob with no matches.
        """


class LocalCache(CacheBase):
    """Caches files on the local filesystem.

    Parameters
    ----------
    directory : str
        The directory the files are saved in.
    """

    def __init__(self, directory):
        # absolute, as the relations are lazy: a relative path would be read against
        # the working directory when the relation is evaluated, not when it was created
        self.directory = Path(directory).absolute().as_posix()

    def path(self, key):
        return Path(self.directory, key).as_posix()

    def missing(self, keys):
        return {key for key in keys if not Path(self.path(key)).exists()}

    def write(self, key, content):
        # The file is written under a temporary .part name and renamed once
        # complete, so an interrupted write is never counted as cached.
        path = Path(self.path(key))
        path.parent.mkdir(parents=True, exist_ok=True)
        partial = path.with_name(f'{path.name}.part')
        partial.write_bytes(content.encode('utf-8'))
        partial.replace(path)

    def modified_at(self, keys):
        modified = {}
        for key in keys:
            mtime = Path(self.path(key)).stat().st_mtime
            written = datetime.datetime.fromtimestamp(mtime, datetime.UTC)
            modified[key] = written.replace(tzinfo=None)
        return modified

    def delete(self, keys):
        # Any .part file left by an interrupted download is deleted too.
        for key in keys:
            path = Path(self.path(key))
            path.unlink(missing_ok=True)
            path.with_name(f'{path.name}.part').unlink(missing_ok=True)

    def delete_all(self):
        if Path(self.directory).is_dir():
            shutil.rmtree(self.directory)

    def glob(self):
        if any(Path(self.directory).rglob('*.json')):
            return f'{self.directory}/**/*.json'
        return None
