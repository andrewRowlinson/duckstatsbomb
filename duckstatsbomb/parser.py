"""A module for loading Hudl StatsBomb open-data, local, or API data."""

import base64
import collections
import importlib.util
import os
import pkgutil
import warnings
from abc import ABC, abstractmethod

import duckdb

from .__about__ import __version__

__all__ = ['Sbopen', 'Sbapi', 'Sblocal']

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
    'events_version': [4, 8],
    'lineup_version': [2, 4],
    'threesixty_version': [1, 2],
}


class SbBase(ABC):
    """A base class for parsing Hudl StatsBomb data using DuckDB.

    Parameters
    ----------
    competitions_version, matches_version, events_version, lineup_version, threesixty_version : int
        The Hudl StatsBomb data version.
    database : str, default ':memory:'
        The name of the DuckDB database. The default is in-memory, which
        is not persisted to disk. Pass a file path for a persistent database.
        The JSON will still be saved to disk if cache_enabled=True.
    duckdb_threads : int, default None
        The number of threads used by DuckDB. The default uses the DuckDB default
    output_format : str, default 'relation'
        The format of data that is returned: 'relation', 'pandas', 'polars' or 'arrow'.
    cache_enabled : bool, default True
        Enable caching using the DuckDB QuackStore community extension.
        Caches events, lineups and threesixty data.
        Does not cache competition or match data.
    cache_path : str, default 'statsbomb_cache.bin'
        The cache file path.
    cache_size : int, default None
        The maximum size of the cache in bytes. The default if None is 2GB.
    cache_mutable : bool, default False
        The default assumes the data does not change once cached.
        If True the cache validates the file freshness.
    sql_dir : str, default None
        The directory of SQL files within the package. Set by the subclasses.
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
        cache_path='statsbomb_cache.bin',
        cache_size=None,
        cache_mutable=False,
        sql_dir=None,
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
            self.con.execute(f'set threads to {duckdb_threads}')
        self.cache_enabled = cache_enabled
        self.cache_path = cache_path
        self.cache_size = cache_size
        self.cache_mutable = cache_mutable

        # To complete in Sbopen/Sbapi/Sblocal
        # url is used by competition/ matches (no caching)
        # cache_url is used elsewhere
        self.url = None
        self.cache_url = None
        self.url_map = None
        self.url_ending = None
        self.sql_dir = sql_dir
        self.sql = {
            'competitions': self._get_sql(
                f'{sql_dir}/competitions/v{competitions_version}/competitions.sql'
            ),
            'matches': self._get_sql(f'{sql_dir}/matches/v{matches_version}/matches.sql'),
            'match_ids': self._get_sql(f'{sql_dir}/matches/match_ids.sql'),
            'season_ids': self._get_sql(f'{sql_dir}/competitions/season_ids.sql'),
            'lineup_players': self._get_sql(
                f'{sql_dir}/lineups/v{lineup_version}/lineup_players.sql'
            ),
            'events': self._get_sql(f'{sql_dir}/events/v{events_version}/events.sql'),
            'frames': self._get_sql(f'{sql_dir}/events/v{events_version}/freeze_frames.sql'),
            'tactics': self._get_sql(f'{sql_dir}/events/v{events_version}/tactics.sql'),
            'related_events': self._get_sql(
                f'{sql_dir}/events/v{events_version}/related_events.sql'
            ),
            'threesixty_frames': self._get_sql(
                f'{sql_dir}/threesixty/v{threesixty_version}/freeze_frames.sql'
            ),
            'threesixty': self._get_sql(
                f'{sql_dir}/threesixty/v{threesixty_version}/threesixty.sql'
            ),
        }

        self.valid_match_data = [
            'lineup_players',
            'events',
            'frames',
            'tactics',
            'related_events',
            'threesixty_frames',
            'threesixty',
        ]

        if lineup_version >= 4:
            self.sql['lineup_events'] = self._get_sql(
                f'{sql_dir}/lineups/v{lineup_version}/lineup_events.sql'
            )
            self.sql['lineup_formations'] = self._get_sql(
                f'{sql_dir}/lineups/v{lineup_version}/lineup_formations.sql'
            )
            self.sql['lineup_positions'] = self._get_sql(
                f'{sql_dir}/lineups/v{lineup_version}/lineup_positions.sql'
            )
            self.valid_match_data.extend(['lineup_events', 'lineup_formations', 'lineup_positions'])

        if threesixty_version >= 2:
            self.sql['threesixty_visible_count'] = self._get_sql(
                f'{sql_dir}/threesixty/v{threesixty_version}/visible_count.sql'
            )
            self.sql['threesixty_visible_distance'] = self._get_sql(
                f'{sql_dir}/threesixty/v{threesixty_version}/visible_distance.sql'
            )
            self.valid_match_data.extend(
                ['threesixty_visible_count', 'threesixty_visible_distance']
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

    def _execute(self, sql, filename, **params):
        """Run a SQL statement for the given urls or file paths.

        Parameters
        ----------
        sql : str
            The SQL to run, which takes a $filename parameter.
        filename : str or list of str
            The urls or file paths to read.
        **params
            Any other named parameters the SQL takes.

        Returns
        -------
        duckdb.DuckDBPyRelation
        """
        return self.con.sql(sql, params={'filename': filename, **params})

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
        return getattr(relation, OUTPUT_FORMATS[self.output_format].method)()

    def _with_loaded_at(self, relation):
        """Add a loaded_at column, the UTC time the rows were fetched.

        Parameters
        ----------
        relation : duckdb.DuckDBPyRelation

        Returns
        -------
        duckdb.DuckDBPyRelation
        """
        return relation.select("*, now() at time zone 'UTC' as loaded_at")

    def _build_url_map(self, slugs):
        """Map each valid data type to the base url for that data.

        Called by the subclasses once self.cache_url is known, so that the data types
        added for the newer data versions always get a url.

        Parameters
        ----------
        slugs : dict
            Maps a data type, e.g. 'events', to its url path, e.g. 'v8/events'.
        """
        self.url_map = {kind: f'{self.cache_url}/{slugs[kind]}' for kind in self.valid_match_data}

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

    def _urls(self, match_id, url_slug):
        """Creates a url string from a base url path and a match identifier.

        Parameters
        ----------
        match_id : int
            The Hudl StatsBomb match identifier
        url_slug : str
            The url base path for the data.

        Returns
        -------
        url : list of str
        """
        if isinstance(match_id, collections.abc.Iterable):
            return [f'{url_slug}/{matchid}{self.url_ending}' for matchid in match_id]
        return [f'{url_slug}/{match_id}{self.url_ending}']

    def _validate_kind(self, kind):
        """Validate that the kind of data e.g. 'events' is one of
        the valid Hudl StatsBomb data types.

        Parameters
        ----------
        kind : str
        """
        if kind not in self.valid_match_data:
            raise ValueError(f'kind should be one of {self.valid_match_data}')

    @abstractmethod
    def _match_url(self, competition_id, season_id):
        """Implement a method to create a match url from a competition and season identifier."""
        pass

    @abstractmethod
    def _competition_url(self):
        """Implement a method to create a competition url."""
        pass

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
        competition_id, season_id : int

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

    def valid_data(self):
        """Returns a list of valid data types

        Returns
        -------
        list
        """
        return self.valid_match_data

    def match_data(self, match_id, kind):
        """Hudl StatsBomb match event data for the given match_id.

        Parameters
        ----------
        match_id : int or list of int
        kind : str
            A data type, e.g. 'events'. For a list of valid kind values use the valid_data method.

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
        relation = self._execute(self.sql[kind], urls)
        relation = self._with_loaded_at(relation)
        return self._format_output(relation)

    def clear_match_data(self, match_id, kind):
        """Clear event data from the cache for a given match_id.

        Parameters
        ----------
        match_id : int or list of int
        kind : str
            A data type, e.g. 'events'. For a list of valid kind values use the valid_data method.

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> parser.clear_match_data([3788741, 3788742], kind='events')
        """
        self._validate_kind(kind)
        urls = self._urls(match_id, url_slug=self.url_map[kind])
        self.con.execute('call quackstore_evict_files($urls)', {'urls': urls})

    def competition_data(self, competition_id, season_id=None, kind='events'):
        """Hudl StatsBomb match event for all matches in a competitition.

        Parameters
        ----------
        competition, season_id : int
            If season_id is None, the method will return matches over multiple seasons
            (if available).
        kind : str
            A data type, e.g. 'events'. For a list of valid kind values use the valid_data method.

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
        relation = self._execute(self.sql[kind], urls)
        relation = self._with_loaded_at(relation)
        return self._format_output(relation)

    def stale_matches(self, data, competition_id, season_id, kind='events'):
        """Match identifiers in a competition/season that are missing from, or have been
        updated by Hudl StatsBomb since they were loaded into, some previously loaded data.

        Parameters
        ----------
        data : duckdb.DuckDBPyRelation or str
            Previously loaded data with match_id and loaded_at columns, or the name of
            a table holding it.
        competition_id, season_id : int
        kind : str, default 'events'
            The kind of data, as the 360 data has its own last_updated_360.

        Returns
        -------
        list of int

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen(database='statsbomb.duckdb')
        >>> parser.competition_data(2, 44, kind='events').create('events')
        >>> stale = parser.stale_matches('events', 2, 44)
        >>> parser.clear_match_data(stale, kind='events')
        """
        self._validate_kind(kind)
        if isinstance(data, str):
            data = self.con.table(data)
        updated = 'last_updated_360' if kind.startswith('threesixty') else 'last_updated'
        index = self._execute(self.sql['matches'], self._match_url(competition_id, season_id))
        loaded = data.aggregate('match_id, max(loaded_at) as loaded_at')
        stale = (
            index.set_alias('index')
            .join(loaded.set_alias('loaded'), 'index.match_id = loaded.match_id', how='left')
            .filter(f'loaded.loaded_at is null or index.{updated} > loaded.loaded_at')
            .select('index.match_id')
            .order('match_id')
        )
        return [row[0] for row in stale.fetchall()]

    def _install_quackstore(self):
        """Install and load the quackstore community extension."""
        self.con.execute('install quackstore from community;')
        self.con.execute('load quackstore;')

    def setup_cache(self):
        """Setup the DuckDB community extension quackstore for caching data.

        The extension is built for each DuckDB release in turn, so it is not always
        available for the newest one. Rather than failing to create the parser at all,
        caching is turned off and a warning is raised, and the data is downloaded each
        time instead.
        """
        try:
            self._install_quackstore()
        except duckdb.Error as exception:
            self.cache_enabled = False
            warnings.warn(
                f'QuackStore, which duckstatsbomb uses for caching, is not available '
                f'for DuckDB {duckdb.__version__} ({exception.__class__.__name__}). '
                f'Pass cache_enabled=False to silence this, '
                f'or use a supported version of DuckDB.',
                RuntimeWarning,
                stacklevel=3,
            )
            return
        self.con.execute('set global quackstore_cache_path = $path;', {'path': self.cache_path})
        self.con.execute('set global quackstore_cache_enabled = true;')
        self.con.execute('set quackstore_data_mutable = $mutable;', {'mutable': self.cache_mutable})
        if self.cache_size:
            self.con.execute('set global quackstore_cache_size = $size;', {'size': self.cache_size})

    def _cache_url(self, url):
        """Setup the cache, and return the url to read through.

        The quackstore:// prefix is only added if the cache is actually working, as
        setup_cache turns caching off when the extension is unavailable.

        Parameters
        ----------
        url : str
            The base url, with no cache prefix.

        Returns
        -------
        str
        """
        if self.cache_enabled:
            self.setup_cache()
        return f'quackstore://{url}' if self.cache_enabled else url

    def clear_cache(self):
        """Clear the quackstore community extension cache."""
        self.con.execute('call quackstore_clear_cache();')

    def close_connection(self):
        """Close the DuckDB connection."""
        self.con.close()


class Sbopen(SbBase):
    """A class for loading data from the Hudl StatsBomb open-data.
    The data is available at: https://github.com/statsbomb/open-data under
    a non-commercial license.

    Parameters
    ----------
    competitions_version, matches_version, events_version, lineup_version, threesixty_version : int
        The Hudl StatsBomb data version.
    database : str, default ':memory:'
        The name of the DuckDB database. The default is in-memory, which
        is not persisted to disk. Pass a file path for a persistent database.
        The JSON will still be saved to disk if cache_enabled=True.
    duckdb_threads : int, default None
        The number of threads used by DuckDB. The default uses the DuckDB default
    output_format : str, default 'relation'
        The format of data that is returned: 'relation', 'pandas', 'polars' or 'arrow'.
    cache_enabled : bool, default True
        Enable caching using the DuckDB QuackStore community extension.
        Caches events, lineups and threesixty data.
        Does not cache competition or match data.
    cache_path : str, default 'statsbomb_cache.bin'
        The cache file path.
    cache_size : int, default None
        The maximum size of the cache in bytes. The default if None is 2GB.
    cache_mutable : bool, default False
        The default assumes the data does not change once cached.
        If True the cache validates the file freshness.
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
        cache_enabled=True,
        cache_path='statsbomb_cache.bin',
        cache_size=None,
        cache_mutable=False,
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
            cache_path=cache_path,
            cache_enabled=cache_enabled,
            cache_size=cache_size,
            cache_mutable=cache_mutable,
            sql_dir='sql',
            connection_kws=connection_kws,
        )
        self.url_ending = '.json'
        self.url = 'https://raw.githubusercontent.com/statsbomb/open-data/master/data'
        self.cache_url = self._cache_url(self.url)
        # the open-data paths are not versioned
        self._build_url_map(
            {
                'lineup_players': 'lineups',
                'lineup_events': 'lineups',
                'lineup_formations': 'lineups',
                'lineup_positions': 'lineups',
                'events': 'events',
                'frames': 'events',
                'tactics': 'events',
                'related_events': 'events',
                'threesixty_frames': 'three-sixty',
                'threesixty': 'three-sixty',
                'threesixty_visible_count': 'three-sixty',
                'threesixty_visible_distance': 'three-sixty',
            }
        )

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


class Sbapi(SbBase):
    """A class for loading data from the Hudl StatsBomb API.
    You can either provide the username and password as arguments or set the SB_USERNAME
    and SB_PASSWORD environmental variables.

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
        The number of threads used by DuckDB. The default uses the DuckDB default
    output_format : str, default 'relation'
        The format of data that is returned: 'relation', 'pandas', 'polars' or 'arrow'.
    cache_enabled : bool, default True
        Enable caching using the DuckDB QuackStore community extension.
        Caches events, lineups and threesixty data.
        Does not cache competition or match data.
    cache_path : str, default 'statsbomb_cache.bin'
        The cache file path.
    cache_size : int, default None
        The maximum size of the cache in bytes. The default if None is 2GB.
    cache_mutable : bool, default False
        The default assumes the data does not change once cached.
        If True the cache validates the file freshness.
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
        cache_path='statsbomb_cache.bin',
        cache_size=None,
        cache_mutable=False,
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
            cache_path=cache_path,
            cache_enabled=cache_enabled,
            cache_size=cache_size,
            cache_mutable=cache_mutable,
            sql_dir='sql',
            connection_kws=connection_kws,
        )
        self.url_ending = ''
        self.url = url
        self.sql['authenticate'] = self._get_sql(f'{self.sql_dir}/authenticate.sql')
        self._authenticate(sb_username, sb_password)
        self.cache_url = self._cache_url(self.url)

        self._build_url_map(
            {
                'lineup_players': f'v{lineup_version}/lineups',
                'lineup_events': f'v{lineup_version}/lineups',
                'lineup_formations': f'v{lineup_version}/lineups',
                'lineup_positions': f'v{lineup_version}/lineups',
                'events': f'v{events_version}/events',
                'frames': f'v{events_version}/events',
                'tactics': f'v{events_version}/events',
                'related_events': f'v{events_version}/events',
                'threesixty_frames': f'v{threesixty_version}/360-frames',
                'threesixty': f'v{threesixty_version}/360-frames',
                'threesixty_visible_count': f'v{threesixty_version}/360-frames',
                'threesixty_visible_distance': f'v{threesixty_version}/360-frames',
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

    def _execute(self, sql, filename, **params):
        """Run a SQL statement, explaining a rejected request.

        DuckDB reports a request the API rejected as HTTP status 0, which reads as a
        server fault, so a note is added saying what it usually means.

        Parameters
        ----------
        sql : str
            The SQL to run, which takes a $filename parameter.
        filename : str or list of str
            The urls to read.
        **params
            Any other named parameters the SQL takes.

        Returns
        -------
        duckdb.DuckDBPyRelation
        """
        try:
            return super()._execute(sql, filename, **params)
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


class Sblocal(SbBase):
    """A class for loading local Hudl StatsBomb data

    Parameters
    ----------
    competitions_version, matches_version, events_version, lineup_version, threesixty_version : int
        The Hudl StatsBomb data version.
    database : str, default ':memory:'
        The name of the DuckDB database. The default is in-memory, which
        is not persisted to disk. Pass a file path for a persistent database.
        The JSON will still be saved to disk if cache_enabled=True.
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
            sql_dir='sql',
            connection_kws=connection_kws,
        )

    def competitions(self, filename):
        """Hudl StatsBomb competition data.

        Parameters
        ----------
        filename : path or list of paths

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or
        pyarrow.Table, depending on output_format

        Examples
        --------
        >>> from duckstatsbomb import Sblocal
        >>> parser = Sblocal()
        >>> competitions = parser.competitions('competitions.json')
        """
        return self._format_output(self._execute(self.sql['competitions'], filename))

    def matches(self, filename):
        """Hudl StatsBomb match data.

        Parameters
        ----------
        filename : path or list of paths

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or
        pyarrow.Table, depending on output_format

        Examples
        --------
        >>> from duckstatsbomb import Sblocal
        >>> parser = Sblocal()
        >>> matches = parser.matches('27.json')
        """
        return self._format_output(self._execute(self.sql['matches'], filename))

    def match_data(self, filename, kind):
        """Hudl StatsBomb match event data for the given match_id.

        Parameters
        ----------
        filename : path or list of paths
        kind : str
            A data type, e.g. 'events'. For a list of valid kind values use the valid_data method.

        Returns
        -------
        duckdb.DuckDBPyRelation, pandas.DataFrame, polars.DataFrame or
        pyarrow.Table, depending on output_format

        Examples
        --------
        >>> from duckstatsbomb import Sblocal
        >>> parser = Sblocal()
        >>> events = parser.match_data(['3788741.json', '3788742.json'], kind='events')
        """
        self._validate_kind(kind)
        return self._format_output(self._execute(self.sql[kind], filename))

    def _match_url(self, competition_id, season_id):
        """No URLs for local data."""
        pass

    def _competition_url(self):
        """No URLs for local data."""
        pass

    def competition_data(self, competition_id, season_id=None, kind='events'):
        """Not implemented for Sblocal."""
        raise NotImplementedError('competition_data has not been implemented for Sblocal')

    def stale_matches(self, data, competition_id, season_id, kind='events'):
        """Not implemented for Sblocal, as local files carry no download time."""
        raise NotImplementedError('stale_matches has not been implemented for Sblocal')

    def clear_match_data(self, match_id, kind):
        """Not implemented for Sblocal, as local files are not cached."""
        raise NotImplementedError('clear_match_data has not been implemented for Sblocal')

    def clear_cache(self):
        """Not implemented for Sblocal, as local files are not cached."""
        raise NotImplementedError('clear_cache has not been implemented for Sblocal')
