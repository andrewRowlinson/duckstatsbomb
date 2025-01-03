"""`duckstatsbomb.parser` is a python module for loading StatsBomb open-data / API data."""

import duckdb
from requests_cache import CachedSession
import collections
import pkgutil
import os
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed

__all__ = ['Sbopen', 'Sbapi', 'Sblocal']


class SbBase(ABC):
    """A base class for parsing StatsBomb open-data/ API data using requests-cache and duckdb.

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
    cache_name : str, default 'statsbomb_cache'
        Base directory for cache files
    cache_backend : str, default 'filesystem'
        The requests-cache backend.
    removed_expired_responses : bool, default True
        If True, removes the expired cached responses when instantiating the class.
    expire_after : int, default 360
        The number of seconds to store cached responses.
    requests_max_workers : default None
        The number of threads to use for requests. The default uses the
        concurrent.futures.ThreadPoolExecutor default.
    sql_dir : str, default None
        Automatically set to change the SQL parsing depending on whether the data
        has been cached by requests-cache ('sql/cache') or is in the original format ('sql/original')
    session_kws : dict, default None
        Additional keywords are passed to requests_cache.CachedSession.
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
        cache_name='statsbomb_cache',
        cache_backend='filesystem',
        remove_expired_responses=True,
        expire_after=360,
        requests_max_workers=None,
        sql_dir=None,
        session_kws=None,
        connection_kws=None,
    ):
        self.competitions_version = competitions_version
        self.matches_version = matches_version
        self.events_version = events_version
        self.lineup_version = lineup_version
        self.threesixty_version = threesixty_version
        self.output_format = output_format
        self._validation_value_error()
        if session_kws is None:
            session_kws = {}
        if connection_kws is None:
            connection_kws = {}
        self.con = duckdb.connect(database=database, **connection_kws)
        if duckdb_threads is not None:
            self.con.execute(f'set threads to {duckdb_threads}')
        self.session = CachedSession(
            cache_name=cache_name,
            backend=cache_backend,
            expire_after=expire_after,
            **session_kws,
        )
        self.requests_max_workers = requests_max_workers
        if remove_expired_responses:
            self.remove_expired_responses()
        # To complete in Sbopen/Sbapi
        self.url = None
        self.sql = None
        self.url_map = None
        self.valid_match_data = None
        self.url_ending = None
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

    def _request(self, url):
        """Request and cache a url via requests-cache and return the file path string.

        Parameters
        ----------
        url : str

        Returns
        -------
        path : str
        """
        resp = self.session.get(url)
        resp.raise_for_status()
        return str(self.session.cache.cache_dir / f'{resp.cache_key}.json')

    def _request_threaded(self, urls):
        """Request and cache multiple urls in parallel using requests-cache and
        ThreadPoolExecutor, and return a list of file path strings.

        Parameters
        ----------
        urls : list of str

        Returns
        -------
        paths : list of str
        """
        with ThreadPoolExecutor(max_workers=self.requests_max_workers) as executor:
            future_list = [executor.submit(self.session.get, url) for url in urls]
            filepaths = []
            for future in as_completed(future_list):
                resp = future.result()
                resp.raise_for_status()
                filepath = str(self.session.cache.cache_dir / f'{resp.cache_key}.json')
                filepaths.append(filepath)
            return filepaths

    def _request_get(self, urls):
        """Request a list of urls with requests-cache.
        Requests are made in parallel using ThreadPoolExecutor if multiple urls are requested.


        Parameters
        ----------
        urls : list of str

        Returns
        -------
        paths : list of str
            File paths of the cached responses.
        """
        if isinstance(urls, str):
            return [self._request(urls)]
        return self._request_threaded(urls)

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
        url : str
        """
        if isinstance(match_id, collections.abc.Iterable):
            return [f'{url_slug}/{matchid}{self.url_ending}' for matchid in match_id]
        return f'{url_slug}/{match_id}{self.url_ending}'

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
        filename = self._request_get(url)
        return self.con.execute(
            self.sql['match_ids'], {'filename': filename}
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
        filename = self._request_get(url)
        seasonids = self.con.execute(
            self.sql['season_ids'],
            {'filename': filename, 'competition_id': competition_id},
        ).fetchall()
        urls = [self._match_url(row[0], row[1]) for row in seasonids]
        filename = self._request_get(urls)
        return self.con.execute(
            self.sql['match_ids'], {'filename': filename}
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
        filename = self._request_get(url)
        return self.con.execute(self.sql['competitions'], {'filename': filename}).df()

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
            urls = self._match_url(competition_id, season_id)
        filename = self._request_get(urls)
        return self.con.execute(self.sql['matches'], {'filename': filename}).df()

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
        filename = self._request_get(urls)
        return self.con.execute(self.sql[kind], {'filename': filename}).df()

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
        filename = self._request_get(urls)
        return self.con.execute(self.sql[kind], {'filename': filename}).df()

    def close_connection(self):
        """Close the duckdb connection."""
        self.con.close()

    def remove_expired_responses(self):
        """Remove expired responses from the cache."""
        self.session.cache.remove_expired_responses()

    def clear_cache(self):
        """Clear the cache."""
        self.session.cache.clear()


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
    cache_name : str, default 'statsbomb_cache'
        Base directory for cache files
    cache_backend : str, default 'filesystem'
        The requests-cache backend.
    removed_expired_responses : bool, default True
        If True, removes the expired cached responses when instantiating the class.
    expire_after : int, default 360
        The number of seconds to store cached responses.
    requests_max_workers : default None
        The number of threads to use for requests. The default uses the
        concurrent.futures.ThreadPoolExecutor default.
    session_kws : dict, default None
        Additional keywords are passed to requests_cache.CachedSession.
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
        cache_name='statsbomb_cache',
        cache_backend='filesystem',
        remove_expired_responses=True,
        expire_after=360,
        requests_max_workers=None,
        session_kws=None,
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
            cache_name=cache_name,
            cache_backend=cache_backend,
            remove_expired_responses=remove_expired_responses,
            expire_after=expire_after,
            requests_max_workers=requests_max_workers,
            duckdb_threads=duckdb_threads,
            sql_dir='sql/cache',
            session_kws=session_kws,
            connection_kws=connection_kws,
        )
        self.url_ending = '.json'
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
    cache_name : str, default 'statsbomb_cache'
        Base directory for cache files
    cache_backend : str, default 'filesystem'
        The requests-cache backend.
    removed_expired_responses : bool, default True
        If True, removes the expired cached responses when instantiating the class.
    expire_after : int, default 360
        The number of seconds to store cached responses.
    requests_max_workers : default None
        The number of threads to use for requests. The default uses the
        concurrent.futures.ThreadPoolExecutor default.
    session_kws : dict, default None
        Additional keywords are passed to requests_cache.CachedSession.
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
        cache_name='statsbomb_cache',
        cache_backend='filesystem',
        remove_expired_responses=True,
        expire_after=360,
        requests_max_workers=None,
        session_kws=None,
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
            cache_name=cache_name,
            cache_backend=cache_backend,
            remove_expired_responses=remove_expired_responses,
            expire_after=expire_after,
            requests_max_workers=requests_max_workers,
            duckdb_threads=duckdb_threads,
            sql_dir='sql/cache',
            session_kws=session_kws,
            connection_kws=connection_kws,
        )
        self.url_ending = ''
        self.session.auth = (
            os.environ.get('SB_USERNAME', sb_username),
            os.environ.get('SB_PASSWORD', sb_password),
        )
        self.url = 'https://data.statsbombservices.com/api'
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
            sql_dir='sql/original',
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
