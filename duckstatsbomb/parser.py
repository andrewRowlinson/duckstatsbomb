"""`duckstatsbomb.parser` is a python module for loading StatsBomb open-data / API data."""

import duckdb
import collections
import pkgutil
import os
from abc import ABC, abstractmethod

__all__ = ['Sbopen', 'Sbapi', 'Sblocal']


class SbBase(ABC):
    """A base class for parsing StatsBomb open-data/ API data using duckdb.

    Parameters
    ----------
    competitions_version, matches_version, events_version, lineup_version, threesixty_version : int
        The StatsBomb data version.
    database : str, default ':default:'
        The name of the duckdb database. By default it creates an unnamed in-memory database that lives
        inside the duckdb module. If the database is a file path, a connection to a persistent database is
        established, which will be created if it doesn't already exist.
    duckdb_threads, int, default None
        The number of threads used by duckdb. The default uses the duckdb default
    output_format : str, default 'pandas'
        The format of data that is returned by the methods: match_data, competition_data, competitions, and match_data.
    cache_path : str, default 'statsbomb_cache.bin'
        The path to store the cache using the DuckDB QuackStore community extension.
    cache_enabled : bool, default True
        Enable caching  using the DuckDB QuackStore community extension.
    cache_size : bool, default None
        The default if None is 2GB. Set in bytes.
    cache_mutable : bool, default True
        If True files are assumed not to change once cached.
        If false, the cache validates the file freshness on access.
    sql_dir : str, default None
        Automatically set to change the SQL parsing depending on whether the data
        has been cached by requests-cache ('sql/cache') or is in the original format ('sql/original')

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
        database=':default:',
        duckdb_threads=None,
        output_format='pandas',
        cache_path='statsbomb_cache.bin',
        cache_enabled=True,
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
        self.cache_path = cache_path
        self.cache_enabled = cache_enabled
        self.cache_size = cache_size
        self.cache_mutable = cache_mutable

        # To complete in Sbopen/Sbapi
        self.url = None
        self.sql = None
        self.url_map = None
        self.valid_match_data = None
        self.url_ending = None
        self.sql = {
            'authenticate': self._get_sql(f'{sql_dir}/authenticate.sql'),
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
            self.url_map['lineup_events'] = f'{self.url}/v{lineup_version}/lineups'
            self.url_map['lineup_formations'] = f'{self.url}/v{lineup_version}/lineups'
            self.url_map['lineup_positions'] = f'{self.url}/v{lineup_version}/lineups'
            self.valid_match_data.extend(
                ['lineup_events', 'lineup_formations', 'lineup_positions']
            )

        if threesixty_version >= 2:
            self.sql['threesixty_visible_count'] = self._get_sql(
                f'{sql_dir}/threesixty/v{threesixty_version}/visible_count.sql'
            )
            self.sql['threesixty_visible_distance'] = self._get_sql(
                f'{sql_dir}/threesixty/v{threesixty_version}/visible_distance.sql'
            )
            self.url_map['threesixty_visible_count'] = (
                f'{self.url}/v{threesixty_version}/360-frames'
            )
            self.url_map['threesixty_visible_distance'] = (
                f'{self.url}/v{threesixty_version}/360-frames'
            )
            self.valid_match_data.extend(
                ['threesixty_visible_count', 'threesixty_visible_distance']
            )

        self.valid_match_data = [
            'lineup_players',
            'events',
            'frames',
            'tactics',
            'related_events',
            'threesixty_frames',
            'threesixty',
        ]

    def _get_sql(self, sql_path):
        """Return a SQL file in the package contents as a string.

        Parameters
        ----------
        sql_path : path to the SQL file in the duckstatsbomb package.
        """
        return pkgutil.get_data(__package__, sql_path).decode('utf-8')

    def _validation_value_error(self):
        """Validates the data version numbers and the output format"""
        if self.competitions_version not in [4]:
            raise ValueError(
                f"Invalid argument: currently supported competitions_version are: [4]"
            )
        if self.matches_version not in [3, 6]:
            raise ValueError(
                f"Invalid argument: currently supported matches_version are: [3, 6]"
            )
        if self.events_version not in [4, 8]:
            raise ValueError(
                f"Invalid argument: currently supported events_version are: [4, 8]"
            )
        if self.lineup_version not in [2, 4]:
            raise ValueError(
                f"Invalid argument: currently supported lineup_version are: [2, 4]"
            )
        if self.threesixty_version not in [1, 2]:
            raise ValueError(
                f"Invalid argument: currently supported threesixty_version are: [1, 2]"
            )
        if self.output_format != 'pandas':
            raise ValueError(
                f"Invalid argument: currently supported output_formats are: 'pandas'"
            )

    def _urls(self, match_id, url_slug):
        """Creates a url string from a base url path and a match identifier.

        Parameters
        ----------
        match_id : int
            The StatsBomb match identifier
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
        """Validate that the kind of data e.g. 'events' is one of the valid StatsBomb data types.

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
            A StatsBomb competition or season identifier.

        Returns
        -------
        matchids
            A list of tuples. The tuples contain a single match identifier integer.
        """
        url = self._match_url(competition_id, season_id)
        return self.con.execute(
            self.sql['match_ids'], {'filename': url}
        ).fetchall()

    def _competition_matchids(self, competition_id):
        """Return a list of match identifiers for a given competition identifier.

        Parameters
        ----------
        competition_id : int
            A StatsBomb competition identifier.

        Returns
        -------
        matchids
            A list of tuples. The tuples contain a single match identifier integer.
        """
        url = self._competition_url()
        seasonids = self.con.execute(
            self.sql['season_ids'],
            {'filename': url, 'competition_id': competition_id},
        ).fetchall()
        urls = [self._match_url(row[0], row[1]) for row in seasonids]
        return self.con.execute(
            self.sql['match_ids'], {'filename': urls}
        ).fetchall()

    def competitions(self):
        """StatsBomb competition data.

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> competitions = parser.competitions()
        """
        url = self._competition_url()
        return self.con.execute(self.sql['competitions'], {'filename': url}).df()

    def clear_competition(self):
        """Clear competition data from the cache.

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> events = parser.clear_competition()
        """
        url = self._competition_url()
        return self.con.execute(f"call quackstore_evict_files(['{url}'])")


    def _multi_match_url(self, competition_id, season_id):
        """ Create match urls in a list if multiple competition or season identifiers."""
        if isinstance(competition_id, collections.abc.Iterable):
            if len(competition_id) != len(season_id):
                raise ValueError(
                    f'competition_id (len = {len(competition_id)}) '
                    f'and season_id (len = {len(season_id)}) should be the same length'
                )
            urls = [
                self._match_url(comp, season_id[idx])
                for idx, comp in enumerate(competition_id)
            ]
        else:
            urls = [self._match_url(competition_id, season_id)]
        return urls

    def matches(self, competition_id, season_id):
        """StatsBomb match data.

        Parameters
        ----------
        competition_id, season_id : int

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> matches = parser.matches(11, 1)
        """
        urls = self._multi_match_url(competition_id, season_id)
        return self.con.execute(self.sql['matches'], {'filename': urls}).df()

    def clear_matches(self, competition_id, season_id):
        """Clear match data from the cache.

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> events = parser.clear_matches(11, 1)
        """
        urls = self._multi_match_url(competition_id, season_id)
        return self.con.execute(f'call quackstore_evict_files({urls})')

    def valid_data(self):
        """Returns a list of valid data types

        Returns
        -------
        list
        """
        return self.valid_match_data

    def match_data(self, match_id, kind):
        """StatsBomb match event data for the given match_id.

        Parameters
        ----------
        match_id : int or list of int
        kind : str
            A data type, e.g. 'events'. For a list of valid kind values use the valid_data method.

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> events = parser.match_data([3788741, 3788742], kind='events')
        """
        self._validate_kind(kind)
        urls = self._urls(match_id, url_slug=self.url_map[kind])
        return self.con.execute(self.sql[kind], {'filename': urls}).df()

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
        >>> events = parser.clear_match_data([3788741, 3788742], kind='events')
        """
        self._validate_kind(kind)
        urls = self._urls(match_id, url_slug=self.url_map[kind])
        return self.con.execute(f'call quackstore_evict_files({urls})')

    def competition_data(self, competition_id, season_id=None, kind='events'):
        """StatsBomb match event for all matches in a competitition.

        Parameters
        ----------
        competition, season_id : int
            If season_id is None, the method will return matches over multiple seasons (if available).
        kind : str
            A data type, e.g. 'events'. For a list of valid kind values use the valid_data method.

        Returns
        -------
        pandas.DataFrame

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
        urls = [
            f'{self.url_map[kind]}/{matchid[0]}{self.url_ending}'
            for matchid in match_id
        ]
        return self.con.execute(self.sql[kind], {'filename': urls}).df()

    def setup_cache(self):
        """ Setup the duckdb community extension quackstore for caching data."""
        self.con.execute('install quackstore from community;')
        self.con.execute('load quackstore;')
        self.con.execute(f"set global quackstore_cache_path = '{self.cache_path}';")
        self.con.execute('set global quackstore_cache_enabled = true;')
        self.con.execute(f'set quackstore_data_mutable = {self.cache_mutable};')
        if self.cache_size:
            self.con.execute(f'set global quackstore_cache_size = {self.cache_size};')

    def clear_cache(self):
        """ Clear the quackstore community extension cache."""
        self.con.execute('call quackstore_clear_cache();')

    def close_connection(self):
        """Close the duckdb connection."""
        self.con.close()


class Sbopen(SbBase):
    """A class for loading data from the StatsBomb open-data.
    The data is available at: https://github.com/statsbomb/open-data under
    a non-commercial license.

    Parameters
    ----------
    competitions_version, matches_version, events_version, lineup_version, threesixty_version : int, defaults 4, 3, 4, 2, 1
        The StatsBomb data version.
    database : str, default ':default:'
        The name of the duckdb database. By default it creates an unnamed in-memory database that lives
        inside the duckdb module. If the database is a file path, a connection to a persistent database is
        established, which will be created if it doesn't already exist.
    duckdb_threads, int, default None
        The number of threads used by duckdb. The default uses the duckdb default
    output_format : str, default 'pandas'
        The format of data that is returned by match_data, competition_data, competitions, and match_data.
    cache_path : str, default 'statsbomb_cache.bin'
        The path to store the cache using the DuckDB QuackStore community extension.
    cache_enabled : bool, default True
        Enable caching  using the DuckDB QuackStore community extension.
    cache_size : bool, default None
        The default if None is 2GB. Set in bytes.
    cache_mutable : bool, default True
        If True files are assumed not to change once cached.
        If false, the cache validates the file freshness on access.
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
        database=':default:',
        duckdb_threads=None,
        output_format='pandas',
        cache_path='statsbomb_cache.bin',
        cache_enabled=True,
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
        if self.cache_enabled:
            self.setup_cache()
            self.url = 'quackstore://https://raw.githubusercontent.com/statsbomb/open-data/master/data'
        else:
            self.url = 'https://raw.githubusercontent.com/statsbomb/open-data/master/data'
        self.url_map = {
            'lineup_players': f'{self.url}/lineups',
            'events': f'{self.url}/events',
            'frames': f'{self.url}/events',
            'tactics': f'{self.url}/events',
            'related_events': f'{self.url}/events',
            'threesixty_frames': f'{self.url}/three-sixty',
            'threesixty': f'{self.url}/three-sixty',
        }

    def _match_url(self, competition_id, season_id):
        """Creates a matches url string for a given competition and season.

        Parameters
        ----------
        competition_id, season_id : int
            The StatsBomb competition and season identifiers

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
    """A class for loading data from the StatsBomb API.
    You can either provide the username and password as arguments or set the SB_USERNAME and SB_PASSWORD
    environmental variables.

    Parameters
    ----------
    sb_username, sb_password, str, default None
        Authentication for the StatsBomb API. The SB_USERNAME and SB_PASSWORD environmental variables are used if available.
        Otherwise the credentials are set from the sb_username and sb_password arguments.
    competitions_version, matches_version, events_version, lineup_version, threesixty_version : int, defaults 4, 6, 8, 4, 2
        The StatsBomb data version.
    database : str, default ':default:'
        The name of the duckdb database. By default it creates an unnamed in-memory database that lives
        inside the duckdb module. If the database is a file path, a connection to a persistent database is
        established, which will be created if it doesn't already exist.
    duckdb_threads, int, default None
        The number of threads used by duckdb. The default uses the duckdb default
    output_format : str, default 'pandas'
        The format of data that is returned by the methods: match_data, competition_data, competitions, and match_data.
    cache_path : str, default 'statsbomb_cache.bin'
        The path to store the cache using the DuckDB QuackStore community extension.
    cache_enabled : bool, default True
        Enable caching  using the DuckDB QuackStore community extension.
    cache_size : bool, default None
        The default if None is 2GB. Set in bytes.
    cache_mutable : bool, default True
        If True files are assumed not to change once cached.
        If false, the cache validates the file freshness on access.
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
        database=':default:',
        duckdb_threads=None,
        output_format='pandas',
        cache_path='statsbomb_cache.bin',
        cache_enabled=True,
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
        self.url_ending = ''
        self.url = 'https://data.statsbombservices.com/api'
        self.con.execute(self.sql['authenticate'],
                         {'url': self.url,
                          'username': os.environ.get('SB_USERNAME', sb_username),
                          'password': os.environ.get('SB_PASSWORD', sb_password)}
                          )
        if self.cache_enabled:
            self.setup_cache()
            self.url = f'quackstore://{self.url}'

        self.url_map = {
            'lineup_players': f'{self.url}/v{lineup_version}/lineups',
            'events': f'{self.url}/v{events_version}/events',
            'frames': f'{self.url}/v{events_version}/events',
            'tactics': f'{self.url}/v{events_version}/events',
            'related_events': f'{self.url}/v{events_version}/events',
            'threesixty_frames': f'{self.url}/v{threesixty_version}/360-frames',
            'threesixty': f'{self.url}/v{threesixty_version}/360-frames',
        }

    def _match_url(self, competition_id, season_id):
        """Creates a matches url string for a given competition and season.

        Parameters
        ----------
        competition_id, season_id : int
            The StatsBomb competition and season identifiers

        Returns
        -------
        url : str
        """
        return f'{self.url}/v{self.matches_version}/competitions/{competition_id}/seasons/{season_id}/matches'

    def _competition_url(self):
        """Creates a competition url string

        Returns
        -------
        url : str
        """
        return f'{self.url}/v{self.competitions_version}/competitions'


class Sblocal(SbBase):
    """A class for loading local StatsBomb data

    Parameters
    ----------
    competitions_version, matches_version, events_version, lineup_version, threesixty_version : int, defaults 4, 3, 4, 2, 1
        The StatsBomb data version.
    database : str, default ':default:'
        The name of the duckdb database. By default it creates an unnamed in-memory database that lives
        inside the duckdb module. If the database is a file path, a connection to a persistent database is
        established, which will be created if it doesn't already exist.
    duckdb_threads, int, default None
        The number of threads used by duckdb. The default uses the duckdb default
    output_format : str, default 'pandas'
        The format of data that is returned by match_data, competition_data, competitions, and match_data.
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
        database=':default:',
        duckdb_threads=None,
        output_format='pandas',
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
        """StatsBomb competition data.

        Parameters
        ----------
        filename : path or list of paths

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sblocal
        >>> parser = Sblocal()
        >>> competitions = parser.competitions('competitions.json')
        """
        return self.con.execute(self.sql['competitions'], {'filename': filename}).df()

    def matches(self, filename):
        """StatsBomb match data.

        Parameters
        ----------
        filename : path or list of paths

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sblocal
        >>> parser = Sblocal()
        >>> matches = parser.matches('27.json')
        """
        return self.con.execute(self.sql['matches'], {'filename': filename}).df()

    def match_data(self, filename, kind):
        """StatsBomb match event data for the given match_id.

        Parameters
        ----------
        filename : path or list of paths
        kind : str
            A data type, e.g. 'events'. For a list of valid kind values use the valid_data method.

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sblocal
        >>> parser = Sblocal()
        >>> events = parser.match_data(['3788741.json', '3788742.json'], kind='events')
        """
        self._validate_kind(kind)
        return self.con.execute(self.sql[kind], {'filename': filename}).df()

    def _match_url(self, competition_id, season_id):
        """No URLs for local data."""
        pass

    def _competition_url(self):
        """No URLs for local data."""
        pass

    def competition_data(self, competition_id, season_id=None, kind='events'):
        """Not implemented for Sblocal."""
        raise NotImplementedError('competition_data has not been implemented for Sblocal')
