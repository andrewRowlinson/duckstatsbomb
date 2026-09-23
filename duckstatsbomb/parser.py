"""A module for loading Hudl StatsBomb open-data, files, or API data."""

import base64
import collections
import contextlib
import importlib.util
import os
import pkgutil
import string
from abc import ABC, abstractmethod
from pathlib import Path

import duckdb

from .__about__ import __version__
from .cache import LocalCache

__all__ = ['Sbopen', 'Sbapi', 'Sbfiles']

# maps duckstatsbomb output_format arguments to the DuckDB method
# and the pypi package dependencies
OutputFormat = collections.namedtuple('OutputFormat', ['method', 'packages'])
OUTPUT_FORMATS = {
    'relation': OutputFormat(method=None, packages=()),
    'pandas': OutputFormat(method='df', packages=('pandas',)),
    'polars': OutputFormat(method='pl', packages=('polars', 'pyarrow')),
    'arrow': OutputFormat(method='to_arrow_table', packages=('pyarrow',)),
}

SUPPORTED_VERSIONS = {
    'competitions_version': [4],
    'matches_version': [3, 6],
    'events_version': [4, 8, 11],
    'lineup_version': [2, 4, 5],
    'threesixty_version': [1, 2],
}

# Each kind of match data: the StatsBomb file it is parsed from ('events', 'lineups' or
# 'threesixty'), the SQL file that parses it, and the minimum version of the source file
# to support it. A kind is only offered when the parser's version is at least min_version.
Kind = collections.namedtuple('Kind', ['source', 'sql', 'min_version'])
KINDS = {
    'lineup_players': Kind(source='lineups', sql='lineup_players', min_version=1),
    'events': Kind(source='events', sql='events', min_version=1),
    'frames': Kind(source='events', sql='freeze_frames', min_version=1),
    'tactics': Kind(source='events', sql='tactics', min_version=1),
    'related_events': Kind(source='events', sql='related_events', min_version=1),
    'threesixty_frames': Kind(source='threesixty', sql='freeze_frames', min_version=1),
    'threesixty': Kind(source='threesixty', sql='threesixty', min_version=1),
    'lineup_events': Kind(source='lineups', sql='lineup_events', min_version=4),
    'lineup_formations': Kind(source='lineups', sql='lineup_formations', min_version=4),
    'lineup_positions': Kind(source='lineups', sql='lineup_positions', min_version=4),
    'threesixty_visible_count': Kind(source='threesixty', sql='visible_count', min_version=2),
    'threesixty_visible_distance': Kind(source='threesixty', sql='visible_distance', min_version=2),
    'defensive_responsibility': Kind(
        source='events', sql='defensive_responsibility', min_version=11
    ),
}


class SbBase:
    """A base class for parsing Hudl StatsBomb data shared by every parser.

    Parameters
    ----------
    competitions_version, matches_version, events_version, lineup_version, threesixty_version : int
        The Hudl StatsBomb data version.
    database : str, default ':memory:'
        The name of the DuckDB database. The default is in-memory, which
        is not persisted to disk. Pass a file path for a persistent database.
    duckdb_threads : int, default None
        The number of threads used by DuckDB. The default uses the DuckDB default.
    output_format : str, default 'relation'
        The format of data that is returned: 'relation', 'pandas', 'polars' or 'arrow'.
    connection_kws : dict, default None
        Additional keywords are passed to duckdb.connect.
    """

    # the directory of SQL files within the package
    sql_dir = 'sql'

    def __init__(
        self,
        competitions_version,
        matches_version,
        events_version,
        lineup_version,
        threesixty_version,
        database=':memory:',
        duckdb_threads=None,
        output_format='relation',
        connection_kws=None,
    ):
        self.competitions_version = competitions_version
        self.matches_version = matches_version
        self.events_version = events_version
        self.lineup_version = lineup_version
        self.threesixty_version = threesixty_version
        self.output_format = output_format
        self._validation_value_error()
        if connection_kws is None:
            connection_kws = {}
        self.con = duckdb.connect(database=database, **connection_kws)
        if duckdb_threads is not None:
            self.con.execute('set threads to $threads', {'threads': duckdb_threads})
        self.duckdb_threads = self.con.execute("select current_setting('threads')").fetchone()[0]

        self.sql = {
            'competitions': self._get_sql(
                f'{self.sql_dir}/competitions/v{competitions_version}/competitions.sql'
            ),
            'matches': self._get_sql(f'{self.sql_dir}/matches/v{matches_version}/matches.sql'),
            'match_ids': self._get_sql(f'{self.sql_dir}/matches/match_ids.sql'),
            'season_ids': self._get_sql(f'{self.sql_dir}/competitions/season_ids.sql'),
        }

        # the kinds of match data these versions carry, mapped to their source file
        self._source_versions = {
            'events': events_version,
            'lineups': lineup_version,
            'threesixty': threesixty_version,
        }
        self._kinds = {
            kind: spec.source
            for kind, spec in KINDS.items()
            if self._source_versions[spec.source] >= spec.min_version
        }
        for kind, source in self._kinds.items():
            version = self._source_versions[source]
            self.sql[kind] = self._get_sql(
                f'{self.sql_dir}/{source}/v{version}/{KINDS[kind].sql}.sql'
            )

    def _validation_value_error(self):
        """Validates the data version numbers and the output format"""
        for parameter, supported in SUPPORTED_VERSIONS.items():
            if getattr(self, parameter) not in supported:
                raise ValueError(
                    f'Invalid argument: currently supported {parameter} are: {supported}'
                )
        if self.output_format not in OUTPUT_FORMATS:
            raise ValueError(
                f'Invalid argument: currently supported output_formats are: {list(OUTPUT_FORMATS)}'
            )
        missing = [
            package
            for package in OUTPUT_FORMATS[self.output_format].packages
            if importlib.util.find_spec(package) is None
        ]
        if missing:
            raise ImportError(
                f"output_format '{self.output_format}' needs {', '.join(missing)}, "
                f'which is not installed. Either install it with '
                f'`pip install duckstatsbomb[{self.output_format}]` or use the default '
                f"output_format='relation', which only needs DuckDB."
            )

    def _get_sql(self, sql_path):
        """Return a SQL file in the package contents as a string.

        Parameters
        ----------
        sql_path : str
            The path to the SQL file within the duckstatsbomb package.

        Returns
        -------
        str
        """
        return pkgutil.get_data(__package__, sql_path).decode('utf-8')

    @contextlib.contextmanager
    def _http_errors(self):
        """Adds nothing to an HTTP error by default.
        Sbapi overrides this to add a note explaining the error.
        """
        yield

    @staticmethod
    def _literal(value):
        """Write a Python value as it would appear in SQL.

        Parameters
        ----------
        value : str, int or list of str or int

        Returns
        -------
        str
            e.g. ``'events/3788741.json'``, ``2`` or ``['a.json', 'b.json']``.
        """
        # DuckDB does the quoting: strings are single-quoted with any inner quote
        # doubled, integers have no quotes, and lists use square brackets.
        return str(duckdb.ConstantExpression(value))

    def _execute(self, sql, filename, **params):
        """Build a lazy relation from a SQL statement for the given urls or file paths.

        Parameters
        ----------
        sql : str
            The SQL to run, which takes a $filename placeholder.
        filename : str or list of str
            The urls or file paths to read.
        **params
            Any other named placeholders the SQL takes.

        Returns
        -------
        duckdb.DuckDBPyRelation
        """
        # The values are written into the SQL as literals rather than bound as parameters.
        # A parameterized relational query is materialized immediately, so it is not lazy
        # and has significant performance overhead. See
        # https://duckdb.org/docs/stable/clients/python/known_issues.html#parameterized-queries-in-relational-api
        params = {'filename': filename, **params}
        literals = {name: self._literal(value) for name, value in params.items()}
        sql = string.Template(sql).substitute(literals)
        with self._http_errors():
            return self.con.sql(sql)

    def _format_output(self, relation):
        """Convert a DuckDB relation to the parser's output_format.

        Parameters
        ----------
        relation : duckdb.DuckDBPyRelation

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or pyarrow.Table
        """
        if self.output_format == 'relation':
            return relation
        # the relation is lazy, so any HTTP error is raised here rather than in _execute
        with self._http_errors():
            return getattr(relation, OUTPUT_FORMATS[self.output_format].method)()

    @property
    def kinds(self):
        """The kinds of match data that can be passed as ``kind``, and the Hudl StatsBomb
        file each one is parsed from.

        Several kinds come from the same file: the events file also holds the shot
        freeze frames, the tactics and the related events, and the lineups and 360
        files each hold several kinds too. Which kinds are available depends on the
        data versions, as the newer versions carry more.

        Returns
        -------
        dict
            Maps each kind, e.g. 'tactics', to its source file: 'events', 'lineups'
            or 'threesixty'.

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> parser.kinds
        {'lineup_players': 'lineups', 'events': 'events', 'frames': 'events', ...}
        >>> [kind for kind, source in parser.kinds.items() if source == 'events']
        ['events', 'frames', 'tactics', 'related_events']
        """
        return dict(self._kinds)

    def _validate_kind(self, kind):
        """Validate that the kind of data e.g. 'events' is one of
        the valid Hudl StatsBomb data types.

        Parameters
        ----------
        kind : str
        """
        if kind not in self._kinds:
            raise ValueError(f'kind should be one of {list(self._kinds)}')

    def close_connection(self):
        """Close the DuckDB connection."""
        self.con.close()


class SbRemote(SbBase, ABC):
    """A base class for the parsers that download Hudl StatsBomb data from a url,
    and cache what they download.

    Parameters
    ----------
    competitions_version, matches_version, events_version, lineup_version, threesixty_version : int
        The Hudl StatsBomb data version.
    database : str, default ':memory:'
        The name of the DuckDB database. The default is in-memory, which
        is not persisted to disk. Pass a file path for a persistent database.
        The JSON will still be saved to disk if cache_enabled=True.
    duckdb_threads : int, default None
        The number of threads used by DuckDB. The default uses the DuckDB default.
        Also the number of files downloaded at once when filling the cache.
    output_format : str, default 'relation'
        The format of data that is returned: 'relation', 'pandas', 'polars' or 'arrow'.
    cache_enabled : bool, default True
        Save the downloaded events, lineups and threesixty files to cache_path,
        so they are only downloaded once. Does not cache competition or match data.
    cache_path : str, default 'statsbomb_cache'
        The directory the downloaded files are saved in. Ignored if cache is given.
    cache : duckstatsbomb.cache.CacheBase, default None
        The cache backend. The default is a local cache at cache_path. Pass a
        CacheBase subclass to cache elsewhere, e.g. in an object store.
    connection_kws : dict, default None
        Additional keywords are passed to duckdb.connect.
    """

    def __init__(
        self,
        competitions_version,
        matches_version,
        events_version,
        lineup_version,
        threesixty_version,
        database=':memory:',
        duckdb_threads=None,
        output_format='relation',
        cache_enabled=True,
        cache_path='statsbomb_cache',
        cache=None,
        connection_kws=None,
    ):
        super().__init__(
            competitions_version=competitions_version,
            matches_version=matches_version,
            events_version=events_version,
            lineup_version=lineup_version,
            threesixty_version=threesixty_version,
            database=database,
            duckdb_threads=duckdb_threads,
            output_format=output_format,
            connection_kws=connection_kws,
        )
        self.cache_enabled = cache_enabled
        self.cache_path = cache_path
        self.cache = LocalCache(cache_path) if cache is None else cache
        self.sql['download_to_cache'] = self._get_sql(f'{self.sql_dir}/download_to_cache.sql')
        self.sql['cached_files'] = self._get_sql(f'{self.sql_dir}/cached_files.sql')

        # To complete in Sbopen/Sbapi
        self.url = None
        self.url_map = None
        self.url_ending = None

    def _build_url_map(self, slugs):
        """Map each kind of match data to the base url of the file it is parsed from.

        Parameters
        ----------
        slugs : dict
            Maps each source file ('events', 'lineups' and 'threesixty') to its url
            path, e.g. 'v8/events'.
        """
        # Subclasses call this after setting self.url. Several kinds (e.g. events and
        # related_events) come from the same source file, so they share its url rather
        # than each needing one of their own.
        self.url_map = {kind: f'{self.url}/{slugs[source]}' for kind, source in self._kinds.items()}

    @abstractmethod
    def _match_url(self, competition_id, season_id):
        """Implement a method to create a match url from a competition and season identifier."""
        pass

    @abstractmethod
    def _competition_url(self):
        """Implement a method to create a competition url."""
        pass

    def competitions(self):
        """Hudl StatsBomb competition data.

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or
        pyarrow.Table, depending on output_format

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> competitions = parser.competitions()
        """
        url = self._competition_url()
        return self._format_output(self._execute(self.sql['competitions'], url))

    def _multi_match_url(self, competition_id, season_id):
        """Create the matches urls for one or more competition/season pairs.

        Parameters
        ----------
        competition_id, season_id : int or list of int
            Lists must be the same length, and are paired up in order.

        Returns
        -------
        list of str
        """
        if isinstance(competition_id, collections.abc.Iterable):
            if not isinstance(season_id, collections.abc.Iterable):
                raise ValueError('season_id should be a list when competition_id is a list')
            if len(competition_id) != len(season_id):
                raise ValueError(
                    f'competition_id (len = {len(competition_id)}) '
                    f'and season_id (len = {len(season_id)}) should be the same length'
                )
            urls = [
                self._match_url(comp, season_id[idx]) for idx, comp in enumerate(competition_id)
            ]
        else:
            urls = [self._match_url(competition_id, season_id)]
        return urls

    def matches(self, competition_id, season_id):
        """Hudl StatsBomb match data.

        Parameters
        ----------
        competition_id, season_id : int or list of int
            Lists must be the same length, and are paired up in order, so several
            competition/season pairs can be fetched in one call.

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or
        pyarrow.Table, depending on output_format

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> matches = parser.matches(11, 1)
        """
        urls = self._multi_match_url(competition_id, season_id)
        return self._format_output(self._execute(self.sql['matches'], urls))

    def _urls(self, match_id, url_slug):
        """Creates a url for each distinct match identifier from a base url path.

        Parameters
        ----------
        match_id : int or list of int
            The Hudl StatsBomb match identifier
        url_slug : str
            The url base path for the data.

        Returns
        -------
        url : list of str
        """
        if not isinstance(match_id, collections.abc.Iterable):
            match_id = [match_id]
        # dict.fromkeys dedupes like set() but keeps the original order. Duplicates
        # must be removed as duckdb's read_json return rows for each time the file is listed
        return [f'{url_slug}/{matchid}{self.url_ending}' for matchid in dict.fromkeys(match_id)]

    def _cache_key(self, url):
        """The key a url is cached under: its path below the base url.

        Parameters
        ----------
        url : str

        Returns
        -------
        str
            e.g. 'v4/events/3788741.json'
        """
        # The API urls have no extension, so .json is added to make the files easy to
        # recognise and to glob. The keys use forward slashes on every platform.
        key = url.removeprefix(f'{self.url}/')
        if not key.endswith('.json'):
            key = f'{key}.json'
        return key

    def _download(self, urls):
        """Download any urls that are not yet cached, and return their cache keys.

        DuckDB downloads the files and Python writes them. The urls are queried in
        batches of duckdb_threads, so memory is capped at one file per thread and
        each batch is written to the cache before the next batch is downloaded.

        Parameters
        ----------
        urls : list of str

        Returns
        -------
        list of str
            One cache key per url, in the same order.
        """
        keys = {url: self._cache_key(url) for url in urls}
        missing_keys = self.cache.missing(keys.values())
        missing = {url: key for url, key in keys.items() if key in missing_keys}
        missing_urls = list(missing)
        batch_size = self.duckdb_threads
        with self._http_errors():
            for start in range(0, len(missing_urls), batch_size):
                batch = missing_urls[start : start + batch_size]
                self.con.execute(self.sql['download_to_cache'], {'urls': batch})
                # one row at a time, so the batch is not copied into a Python list
                row = self.con.fetchone()
                while row is not None:
                    url, content = row
                    self.cache.write(missing[url], content)
                    row = self.con.fetchone()
        return [keys[url] for url in urls]

    @staticmethod
    def _match_id(path):
        """Get the match identifier from the file name.

        Parameters
        ----------
        path : str
            A url, cache key or file path named after the match_id,
            e.g. '.../events/3788741.json'.

        Returns
        -------
        int
        """
        # must agree with parse_filename in the SQL, as loaded_at is joined on match_id
        return int(Path(path).stem)

    def _with_loaded_at(self, relation, keys=None):
        """Add a loaded_at column, the UTC time the data was fetched.

        Parameters
        ----------
        relation : duckdb.DuckDBPyRelation
            Match data with a match_id column.
        keys : list of str, default None
            The cache keys the relation was read from, or None if it was read
            straight from the urls.

        Returns
        -------
        duckdb.DuckDBPyRelation
        """
        if keys is None:
            # not cached, so the data was fetched just now
            return relation.select("*, now() at time zone 'UTC' as loaded_at")
        # a cached match keeps the loaded_at of its download rather than of this read,
        # as that is the time stale_matches compares with StatsBomb's last_updated
        modified = self.cache.modified_at(dict.fromkeys(keys))
        # binding params materializes this relation, unlike _execute, but it is only
        # one row per match so that is cheap. The match data it joins stays lazy.
        downloaded = self.con.sql(
            'select unnest($match_ids) as match_id, unnest($loaded_at) as loaded_at',
            params={
                'match_ids': [self._match_id(key) for key in modified],
                'loaded_at': list(modified.values()),
            },
        )
        return relation.join(downloaded, 'match_id')

    def _read_match_data(self, urls, kind):
        """Read match files, through the cache if it is enabled, stamped with loaded_at.

        Parameters
        ----------
        urls : list of str
        kind : str
            A data type, e.g. 'events'.

        Returns
        -------
        duckdb.DuckDBPyRelation
        """
        if not self.cache_enabled:
            return self._with_loaded_at(self._execute(self.sql[kind], urls))
        keys = self._download(urls)
        paths = [self.cache.path(key) for key in keys]
        return self._with_loaded_at(self._execute(self.sql[kind], paths), keys)

    def match_data(self, match_id, kind):
        """Hudl StatsBomb match data (e.g. events, lineups) for one or more match ids.

        Parameters
        ----------
        match_id : int or list of int
        kind : str
            A kind of match data, e.g. 'events'. See the kinds property for the values.

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or
        pyarrow.Table, depending on output_format

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> events = parser.match_data([3788741, 3788742], kind='events')
        """
        self._validate_kind(kind)
        urls = self._urls(match_id, url_slug=self.url_map[kind])
        return self._format_output(self._read_match_data(urls, kind))

    def _competition_season_matchids(self, competition_id=None, season_id=None):
        """Return a list of match identifiers for a given competition and season identifier.

        Parameters
        ----------
        competition_id, season_id : int
            A Hudl StatsBomb competition or season identifier.

        Returns
        -------
        matchids
            A list of tuples. The tuples contain a single match identifier integer.
        """
        url = self._match_url(competition_id, season_id)
        return self._execute(self.sql['match_ids'], url).fetchall()

    def _competition_matchids(self, competition_id):
        """Return a list of match identifiers for a given competition identifier.

        Parameters
        ----------
        competition_id : int
            A Hudl StatsBomb competition identifier.

        Returns
        -------
        matchids
            A list of tuples. The tuples contain a single match identifier integer.
        """
        url = self._competition_url()
        seasonids = self._execute(
            self.sql['season_ids'], url, competition_id=competition_id
        ).fetchall()
        urls = [self._match_url(row[0], row[1]) for row in seasonids]
        return self._execute(self.sql['match_ids'], urls).fetchall()

    def competition_data(self, competition_id, season_id=None, kind='events'):
        """Hudl StatsBomb match data for all matches in a competition.

        Parameters
        ----------
        competition_id, season_id : int
            If season_id is None, the method will return matches over multiple seasons
            (if available).
        kind : str
            A kind of match data, e.g. 'events'. See the kinds property for the values.

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or
        pyarrow.Table, depending on output_format

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> events = parser.competition_data(2, 44, kind='events') # the invincibles
        """
        self._validate_kind(kind)
        if season_id is None:
            match_id = self._competition_matchids(competition_id)
        else:
            match_id = self._competition_season_matchids(competition_id, season_id)
        urls = self._urls([matchid[0] for matchid in match_id], url_slug=self.url_map[kind])
        return self._format_output(self._read_match_data(urls, kind))

    def stale_matches(self, competition_id, season_id, kind='events'):
        """Matches whose cached data is out of date with the latest from Hudl StatsBomb.

        Compares the time each match file was downloaded with the match's last_updated
        timestamp in the matches file. Matches not yet cached are included.

        Parameters
        ----------
        competition_id, season_id : int
        kind : str, default 'events'
            A kind of match data, e.g. 'events'. See the kinds property for the values.

        Returns
        -------
        list of int
            Sorted match identifiers. Every match when the cache is disabled or empty.

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> parser.competition_data(2, 44, kind='events')
        >>> stale = parser.stale_matches(2, 44, kind='events')
        >>> parser.clear_match_data(stale, kind='events')  # the next read downloads them again
        """
        self._validate_kind(kind)
        updated = 'last_updated_360' if kind.startswith('threesixty') else 'last_updated'
        index = self._execute(self.sql['matches'], self._match_url(competition_id, season_id))
        last_updated = dict(index.select(f'match_id, {updated}').fetchall())
        urls = self._urls(list(last_updated), url_slug=self.url_map[kind])
        keys = {self._match_id(url): self._cache_key(url) for url in urls}
        if not self.cache_enabled:
            # nothing is kept between reads, so nothing can be known to be current
            return sorted(keys)
        missing = self.cache.missing(keys.values())
        downloaded = self.cache.modified_at(key for key in keys.values() if key not in missing)
        stale = [
            match_id
            for match_id, key in keys.items()
            if key in missing
            or (last_updated[match_id] is not None and last_updated[match_id] > downloaded[key])
        ]
        return sorted(stale)

    def clear_match_data(self, match_id, kind):
        """Delete the cached files for the given match_id, so they are downloaded again.

        Parameters
        ----------
        match_id : int or list of int
        kind : str
            A kind of match data, e.g. 'events'. See the kinds property for the values.

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> parser.clear_match_data([3788741, 3788742], kind='events')
        """
        self._validate_kind(kind)
        urls = self._urls(match_id, url_slug=self.url_map[kind])
        self.cache.delete(self._cache_key(url) for url in urls)

    def cached_files(self):
        """The files in the cache, with their size in bytes and UTC download time.

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or
        pyarrow.Table, depending on output_format

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> parser.match_data(3788741, kind='events')
        >>> parser.cached_files()
        """
        glob = self.cache.glob()
        if glob is None:
            relation = self.con.sql(
                'select null::varchar as path, null::bigint as size, '
                'null::timestamp as downloaded_at where false'
            )
        else:
            relation = self._execute(self.sql['cached_files'], glob)
        return self._format_output(relation)

    def clear_cache(self):
        """Delete the cache and everything in it."""
        self.cache.delete_all()


class Sbopen(SbRemote):
    """A class for loading data from the Hudl StatsBomb open-data.
    The data is available at: https://github.com/statsbomb/open-data under
    a non-commercial license.

    The open-data is published in one format, so the data versions are fixed:
    competitions v4, matches v3, events v4, lineups v2 and 360 v1.

    Parameters
    ----------
    database : str, default ':memory:'
        The name of the DuckDB database. The default is in-memory, which
        is not persisted to disk. Pass a file path for a persistent database.
        The JSON will still be saved to disk if cache_enabled=True.
    duckdb_threads : int, default None
        The number of threads used by DuckDB. The default uses the DuckDB default.
        Also the number of files downloaded at once when filling the cache.
    output_format : str, default 'relation'
        The format of data that is returned: 'relation', 'pandas', 'polars' or 'arrow'.
    cache_enabled : bool, default True
        Save the downloaded events, lineups and threesixty files to cache_path,
        so they are only downloaded once. Does not cache competition or match data.
    cache_path : str, default 'statsbomb_cache'
        The directory the downloaded files are saved in. Ignored if cache is given.
    cache : duckstatsbomb.cache.CacheBase, default None
        The cache backend. The default is a local cache at cache_path. Pass a
        CacheBase subclass to cache elsewhere, e.g. in an object store.
    connection_kws : dict, default None
        Additional keywords are passed to duckdb.connect.
    """

    def __init__(
        self,
        database=':memory:',
        duckdb_threads=None,
        output_format='relation',
        cache_enabled=True,
        cache_path='statsbomb_cache',
        cache=None,
        connection_kws=None,
    ):
        super().__init__(
            competitions_version=4,
            matches_version=3,
            events_version=4,
            lineup_version=2,
            threesixty_version=1,
            database=database,
            duckdb_threads=duckdb_threads,
            output_format=output_format,
            cache_enabled=cache_enabled,
            cache_path=cache_path,
            cache=cache,
            connection_kws=connection_kws,
        )
        self.url_ending = '.json'
        self.url = 'https://raw.githubusercontent.com/statsbomb/open-data/master/data'
        # the open-data paths are not versioned
        self._build_url_map({'events': 'events', 'lineups': 'lineups', 'threesixty': 'three-sixty'})

    def _match_url(self, competition_id, season_id):
        """Creates a matches url string for a given competition and season.

        Parameters
        ----------
        competition_id, season_id : int
            The Hudl StatsBomb competition and season identifiers

        Returns
        -------
        url : str
        """
        return f'{self.url}/matches/{competition_id}/{season_id}{self.url_ending}'

    def _competition_url(self):
        """Creates a competition url string

        Returns
        -------
        url : str
        """
        return f'{self.url}/competitions{self.url_ending}'


class Sbapi(SbRemote):
    """A class for loading data from the Hudl StatsBomb API.
    You can either provide the username and password as arguments or set the SB_USERNAME
    and SB_PASSWORD environment variables.

    Parameters
    ----------
    sb_username, sb_password : str, default None
        Authentication for the Hudl StatsBomb API. Either use the arguments, or
        set the SB_USERNAME and SB_PASSWORD environment variables.
    competitions_version, matches_version, events_version, lineup_version, threesixty_version : int
        The Hudl StatsBomb data version.
    database : str, default ':memory:'
        The name of the DuckDB database. The default is in-memory, which
        is not persisted to disk. Pass a file path for a persistent database.
        The JSON will still be saved to disk if cache_enabled=True.
    duckdb_threads : int, default None
        The number of threads used by DuckDB. The default uses the DuckDB default.
        Also the number of files downloaded at once when filling the cache.
    output_format : str, default 'relation'
        The format of data that is returned: 'relation', 'pandas', 'polars' or 'arrow'.
    cache_enabled : bool, default True
        Save the downloaded events, lineups and threesixty files to cache_path,
        so they are only downloaded once. Does not cache competition or match data.
    cache_path : str, default 'statsbomb_cache'
        The directory the downloaded files are saved in. Ignored if cache is given.
    cache : duckstatsbomb.cache.CacheBase, default None
        The cache backend. The default is a local cache at cache_path. Pass a
        CacheBase subclass to cache elsewhere, e.g. in an object store.
    url : str, default 'https://data.statsbombservices.com/api'
        The base url of the Hudl StatsBomb API.
    connection_kws : dict, default None
        Additional keywords are passed to duckdb.connect.
    """

    def __init__(
        self,
        sb_username=None,
        sb_password=None,
        competitions_version=4,
        matches_version=6,
        events_version=8,
        lineup_version=4,
        threesixty_version=2,
        database=':memory:',
        duckdb_threads=None,
        output_format='relation',
        cache_enabled=True,
        cache_path='statsbomb_cache',
        cache=None,
        url='https://data.statsbombservices.com/api',
        connection_kws=None,
    ):
        super().__init__(
            competitions_version=competitions_version,
            matches_version=matches_version,
            events_version=events_version,
            lineup_version=lineup_version,
            threesixty_version=threesixty_version,
            database=database,
            duckdb_threads=duckdb_threads,
            output_format=output_format,
            cache_enabled=cache_enabled,
            cache_path=cache_path,
            cache=cache,
            connection_kws=connection_kws,
        )
        self.url_ending = ''
        self.url = url
        self.sql['authenticate'] = self._get_sql(f'{self.sql_dir}/authenticate.sql')
        self._authenticate(sb_username, sb_password)

        self._build_url_map(
            {
                'events': f'v{events_version}/events',
                'lineups': f'v{lineup_version}/lineups',
                'threesixty': f'v{threesixty_version}/360-frames',
            }
        )

    def _authenticate(self, sb_username, sb_password):
        """Create a DuckDB http secret for authenticating with the Hudl StatsBomb API.

        The credentials are sent as an HTTP basic Authorization header.

        Parameters
        ----------
        sb_username, sb_password : str
            Authentication for the Hudl StatsBomb API. Falls back to the SB_USERNAME and
            SB_PASSWORD environment variables.
        """
        if sb_username is None:
            sb_username = os.environ.get('SB_USERNAME')
        if sb_password is None:
            sb_password = os.environ.get('SB_PASSWORD')
        if sb_username is None or sb_password is None:
            raise ValueError(
                'Hudl StatsBomb API credentials are required. Either set the SB_USERNAME and '
                'SB_PASSWORD environment variables or use the sb_username and '
                'sb_password arguments.'
            )
        credentials = f'{sb_username}:{sb_password}'.encode()
        token = base64.b64encode(credentials).decode('ascii')
        variables = {
            'sb_scope': self.url,
            'sb_authorization': f'Basic {token}',
            'sb_user_agent': f'duckstatsbomb/{__version__}',
        }
        try:
            for name, value in variables.items():
                self.con.execute(f'set variable {name} = $value', {'value': value})
            self.con.execute(self.sql['authenticate'])
        finally:
            for name in variables:
                self.con.execute(f'reset variable {name}')

    @contextlib.contextmanager
    def _http_errors(self):
        """Add a note to an HTTP error explaining a common failure mode.

        DuckDB reports a request the API rejected as HTTP status 0, which reads as a
        server fault, so a note is added saying what it usually means.
        """
        try:
            yield
        except duckdb.HTTPException as exception:
            if exception.status_code == 0:
                exception.add_note(
                    'The Hudl StatsBomb API rejected the request, which DuckDB reports '
                    'as HTTP 0. This usually means the username or password is wrong, '
                    'or the subscription does not cover this data.'
                )
            raise

    def _match_url(self, competition_id, season_id):
        """Creates a matches url string for a given competition and season.

        Parameters
        ----------
        competition_id, season_id : int
            The Hudl StatsBomb competition and season identifiers

        Returns
        -------
        url : str
        """
        return (
            f'{self.url}/v{self.matches_version}/competitions/{competition_id}'
            f'/seasons/{season_id}/matches'
        )

    def _competition_url(self):
        """Creates a competition url string

        Returns
        -------
        url : str
        """
        return f'{self.url}/v{self.competitions_version}/competitions'


class Sbfiles(SbBase):
    """A class for loading Hudl StatsBomb data from JSON files you already have,
    on disk or in an object store such as S3.

    The events, lineups and 360 JSON do not contain the match identifier, so it is
    taken from the file name. Name the match files {match_id}.json, e.g.
    3788741.json, and the match_id column is filled in. The competitions and matches
    files can keep any name, as their identifiers are in the JSON.

    The methods take a file path, a list of paths, or a glob such as
    ``'events/*.json'``, which DuckDB resolves. DuckDB also reads urls, so
    ``'s3://bucket/events/*.json'`` works. A private bucket needs a DuckDB secret,
    e.g. ``parser.con.sql("CREATE SECRET (TYPE s3, PROVIDER credential_chain)")``.

    Parameters
    ----------
    competitions_version, matches_version, events_version, lineup_version, threesixty_version : int
        The Hudl StatsBomb data version.
    database : str, default ':memory:'
        The name of the DuckDB database. The default is in-memory, which
        is not persisted to disk. Pass a file path for a persistent database.
    duckdb_threads : int, default None
        The number of threads used by DuckDB. The default uses the DuckDB default
    output_format : str, default 'relation'
        The format of data that is returned: 'relation', 'pandas', 'polars' or 'arrow'.
    connection_kws : dict, default None
        Additional keywords are passed to duckdb.connect.
    """

    def __init__(
        self,
        competitions_version=4,
        matches_version=3,
        events_version=4,
        lineup_version=2,
        threesixty_version=1,
        database=':memory:',
        duckdb_threads=None,
        output_format='relation',
        connection_kws=None,
    ):
        super().__init__(
            competitions_version=competitions_version,
            matches_version=matches_version,
            events_version=events_version,
            lineup_version=lineup_version,
            threesixty_version=threesixty_version,
            database=database,
            output_format=output_format,
            duckdb_threads=duckdb_threads,
            connection_kws=connection_kws,
        )

    @staticmethod
    def _unique(filename):
        """Convert a list of paths (or a path) into a unique list of strings.

        Parameters
        ----------
        filename : path or list of paths

        Returns
        -------
        list of str
        """
        if isinstance(filename, str | Path):
            filename = [filename]
        # dict.fromkeys dedupes like set() but keeps the original order
        return list(dict.fromkeys(map(str, filename)))

    def competitions(self, filename):
        """Hudl StatsBomb competition data.

        Parameters
        ----------
        filename : path, list of paths or glob
            The competitions JSON file(s).

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or
        pyarrow.Table, depending on output_format

        Examples
        --------
        >>> from duckstatsbomb import Sbfiles
        >>> parser = Sbfiles()
        >>> competitions = parser.competitions('competitions.json')
        """
        return self._format_output(self._execute(self.sql['competitions'], self._unique(filename)))

    def matches(self, filename):
        """Hudl StatsBomb match data.

        Parameters
        ----------
        filename : path, list of paths or glob
            The matches JSON file(s).

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or
        pyarrow.Table, depending on output_format

        Examples
        --------
        >>> from duckstatsbomb import Sbfiles
        >>> parser = Sbfiles()
        >>> matches = parser.matches('27.json')
        """
        return self._format_output(self._execute(self.sql['matches'], self._unique(filename)))

    def match_data(self, filename, kind):
        """Hudl StatsBomb match data (e.g. events, lineups).

        Parameters
        ----------
        filename : path, list of paths or glob
            The match JSON file(s) should be named {match_id}.json
            as the match_id is taken from the file name(s).
        kind : str
            A kind of match data, e.g. 'events'. See the kinds property for the values.

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or
        pyarrow.Table, depending on output_format

        Examples
        --------
        >>> from duckstatsbomb import Sbfiles
        >>> parser = Sbfiles()
        >>> events = parser.match_data(['3788741.json', '3788742.json'], kind='events')
        >>> events = parser.match_data('events/*.json', kind='events')
        >>> tactics = parser.match_data('events/*.json', kind='tactics')
        >>> lineups = parser.match_data('lineups/*.json', kind='lineup_players')
        >>> frames = parser.match_data('three-sixty/*.json', kind='threesixty_frames')
        """
        self._validate_kind(kind)
        return self._format_output(self._execute(self.sql[kind], self._unique(filename)))
