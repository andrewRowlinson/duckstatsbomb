"""`duckstatsbomb.opendata` is a python module for loading StatsBomb open-data."""

import duckdb
from requests_cache import CachedSession
import collections
import pkgutil
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed

__all__ = ['Sbopen', 'Sbapi']


class SbBase(ABC):

    def __init__(self,
                 database=':default:',
                 output_format='dataframe',
                 cache_name='statsbomb_cache',
                 remove_expired_responses=True,
                 expire_after=360,
                 backend='filesystem',
                 requests_max_workers=None,
                 duckdb_threads=None,
                 session_kws=None,
                 connection_kws=None,
                 ):
        if output_format != 'dataframe':
            raise TypeError(f"Invalid argument: currently supported formats are: 'dataframe'")
        if session_kws is None:
            session_kws = {}
        if connection_kws is None:
            connection_kws = {}
        self.con = duckdb.connect(database=database, **connection_kws)
        if duckdb_threads is not None:
            self.con.execute(f'set threads to {duckdb_threads}')
        self.session = CachedSession(cache_name=cache_name, backend=backend, expire_after=expire_after, **session_kws)
        self.requests_max_workers = requests_max_workers
        if remove_expired_responses:
            self.remove_expired_responses()
        # To complete in Sbopen/Sbapi
        self.url = None
        self.sql = None
        self.url_map = None
        self.valid_match_data = None

    def _get_sql(self, sql_path):
        """ Get SQL string from a file."""
        return pkgutil.get_data(__package__, sql_path).decode('utf-8')       

    def _request(self, url):
        resp = self.session.get(url)
        resp.raise_for_status()
        return str(self.session.cache.cache_dir / f'{resp.cache_key}.json')

    def _request_threaded(self, urls):
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
        """ Request and cache responses and return filepaths to cached responses """
        if isinstance(urls, str):
            return [self._request(urls)]
        return self._request_threaded(urls)

    def _urls(self, match_id, url_slug):
        """ Build urls."""
        if isinstance(match_id, collections.abc.Iterable):
            return [f'{url_slug}/{matchid}.json' for matchid in match_id]
        return f'{url_slug}/{match_id}.json'

    def _validate_file_type(self, file_type):
        if file_type not in self.valid_match_data:
            raise ValueError(f'file_type should be one of {self.valid_match_data}')

    @abstractmethod
    def _match_url(self, competition_id, season_id):
        """ Implement match url helper."""
        pass

    def _competition_season_matchids(self, competition_id=None, season_id=None):    
        url = self._match_url(competition_id, season_id)
        filename = self._request_get(url)
        return self.con.execute(self.sql['match_ids'], {'filename': filename}).fetchall()

    def _competition_matchids(self, competition_id):
        url = f'{self.url}competitions.json'
        filename = self._request_get(url)
        seasonids = self.con.execute(self.sql['season_ids'],
                                     {'filename': filename,
                                      'competition_id': competition_id}).fetchall()
        urls = [self._match_url(row[0], row[1]) for row in seasonids]
        filename = self._request_get(urls)
        return self.con.execute(self.sql['match_ids'], {'filename': filename}).fetchall()

    def competitions(self):
        """ StatsBomb competition open-data.

        Returns
        -------
        competition
            A dataframe or a flattened list of dictionaries.

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> competitions = parser.competitions()
        """
        url = f'{self.url}competitions.json'
        filename = self._request_get(url)
        return self.con.execute(self.sql['competitions'], {'filename': filename}).df()

    def matches(self, competition_id, season_id):
        """ StatsBomb match open-data.

        Parameters
        ----------
        competition_id : int
        season_id : int

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
                raise ValueError(f'competition_id (len = {len(competition_id)}) '
                                 f'and season_id (len = {len(season_id)}) should be the same length')
            urls = [self._match_url(comp, season_id[idx]) for idx, comp in enumerate(competition_id)]
        else:
            urls = self._match_url(competition_id, season_id)
        filename = self._request_get(urls)
        return self.con.execute(self.sql['matches'], {'filename': filename}).df()

    def match_data(self, match_id, file_type):
        """ TO DO"""
        self._validate_file_type(file_type)
        urls = self._urls(match_id, url_slug=self.url_map[file_type])
        filename = self._request_get(urls)
        return self.con.execute(self.sql[file_type], {'filename': filename}).df()

    def competition_data(self, competition_id, season_id=None, file_type='events'):
        """ TO DO"""
        self._validate_file_type(file_type)
        if season_id is None:
            match_id = self._competition_matchids(competition_id)
        else:
            match_id = self._competition_season_matchids(competition_id, season_id)
        urls = [f'{self.url_map[file_type]}/{matchid[0]}.json' for matchid in match_id]
        filename = self._request_get(urls)
        return self.con.execute(self.sql[file_type], {'filename': filename}).df()

    def close_connection(self):
        """ Close the duckdb connection."""
        self.con.close()

    def remove_expired_responses(self):
        """ Remove expired responses from the cache"""
        self.session.cache.remove_expired_responses()

    def clear_cache(self):
        """ Clear the cache."""
        self.session.cache.clear()


class Sbopen(SbBase):
    """ Class for loading data from the StatsBomb open-data.
    The data is available at: https://github.com/statsbomb/open-data under
    a non-commercial license.
    """

    def __init__(self,
                 database=':default:',
                 output_format='dataframe',
                 cache_name='statsbomb_cache',
                 remove_expired_responses=True,
                 expire_after=360,
                 backend='filesystem',
                 requests_max_workers=None,
                 duckdb_threads=None,
                 session_kws=None,
                 connection_kws=None,
                 ):
        super().__init__(database=database,
                 output_format=output_format,
                 cache_name=cache_name,
                 remove_expired_responses=remove_expired_responses,
                 expire_after=expire_after,
                 backend=backend,
                 requests_max_workers=requests_max_workers,
                 duckdb_threads=duckdb_threads,
                 session_kws=session_kws,
                 connection_kws=connection_kws,
                        )
        self.url = 'https://raw.githubusercontent.com/statsbomb/open-data/master/data/'
        self.sql = {'competitions': self._get_sql('sql/competitions/v4/competitions.sql'),
                    'matches': self._get_sql('sql/matches/v3/matches.sql'),
                    'match_ids': self._get_sql('sql/matches/match_ids.sql'),
                    'season_ids': self._get_sql('sql/competitions/season_ids.sql'),
                    'lineups_players': self._get_sql('sql/lineups/v2/lineups_players.sql'),
                    'events': self._get_sql('sql/events/v4/events.sql'),
                    'frames': self._get_sql('sql/events/v4/freeze_frames.sql'),
                    'tactics': self._get_sql('sql/events/v4/tactics.sql'),
                    'related_events': self._get_sql('sql/events/v4/related_events.sql'),
                    'threesixty_frames': self._get_sql('sql/threesixty/v1/freeze_frames.sql'),
                    'threesixty': self._get_sql('sql/threesixty/v1/threesixty.sql'),
                    }
        self.url_map = {'lineups_players': f'{self.url}/lineups',
                        'events': f'{self.url}/events',
                        'frames': f'{self.url}/events',
                        'tactics': f'{self.url}/events',
                        'related_events': f'{self.url}/events',
                        'threesixty_frames': f'{self.url}/three-sixty',
                        'threesixty': f'{self.url}/three-sixty',
                       }
        self.valid_match_data = ['lineups_players', 'events', 'frames', 'tactics',
                                 'related_events', 'threesixty_frames', 'threesixty']

    def _match_url(self, competition_id, season_id):
        return f'{self.url}matches/{competition_id}/{season_id}.json'


class Sbapi(SbBase):
    """ Class for loading data from the StatsBomb API."""

    def __init__(self,
                 database=':default:',
                 output_format='dataframe',
                 cache_name='statsbomb_cache',
                 remove_expired_responses=True,
                 expire_after=360,
                 backend='filesystem',
                 requests_max_workers=None,
                 duckdb_threads=None,
                 session_kws=None,
                 connection_kws=None,
                 sb_username=None,
                 sb_password=None,
                 competition_version=4,
                 events_version=8,
                 lineups_version=4,
                 matches_version=6,
                 threesixty_version=2,                 
                 ):
        super().__init__(database=database,
                 output_format=output_format,
                 cache_name=cache_name,
                 remove_expired_responses=remove_expired_responses,
                 expire_after=expire_after,
                 backend=backend,
                 requests_max_workers=requests_max_workers,
                 duckdb_threads=duckdb_threads,
                 session_kws=session_kws,
                 connection_kws=connection_kws,
                        )
        self.url = 'https://data.statsbombservices.com/api/'
        self.sql = {'competitions': self._get_sql(f'sql/competitions/v{competition_version}/competitions.sql'),
                    'matches': self._get_sql(f'sql/matches/v{matches_version}/matches.sql'),
                    'match_ids': self._get_sql(f'sql/matches/match_ids.sql'),
                    'season_ids': self._get_sql(f'sql/competitions/season_ids.sql'),
                    'lineups_players': self._get_sql(f'sql/lineups/v{lineups_version}/lineups_players.sql'),
                    'lineups_events': self._get_sql(f'sql/lineups/v{lineups_version}/lineups_events.sql'),
                    'lineups_formations': self._get_sql(f'sql/lineups/v{lineups_version}/lineups_formations.sql'),
                    'lineups_positions': self._get_sql(f'sql/lineups/v{lineups_version}/lineups_positions.sql'),
                    'events': self._get_sql(f'sql/events/v{events_version}/events.sql'),
                    'frames': self._get_sql(f'sql/events/v{events_version}/freeze_frames.sql'),
                    'tactics': self._get_sql(f'sql/events/v{events_version}/tactics.sql'),
                    'related_events': self._get_sql(f'sql/events/v{events_version}/related_events.sql'),
                    'threesixty_frames': self._get_sql(f'sql/threesixty/v{threesixty_version}/freeze_frames.sql'),
                    'threesixty': self._get_sql(f'sql/threesixty/v{threesixty_version}/threesixty.sql'),
                    'threesixty_visible_count': self._get_sql(f'sql/threesixty/v{threesixty_version}/visible_count.sql'),
                    'threesixty_visible_distance': self._get_sql(f'sql/threesixty/v{threesixty_version}/visible_distance.sql'),
                    }
        self.url_map = {'lineups_players': f'{self.url}/v{lineups_version}/lineups',
                        'lineups_events': f'{self.url}/v{lineups_version}/lineups',
                        'lineups_formations': f'{self.url}/v{lineups_version}/lineups',
                        'lineups_positions': f'{self.url}/v{lineups_version}/lineups',
                        'events': f'{self.url}/v{events_version}/events',
                        'frames': f'{self.url}/v{events_version}/events',
                        'tactics': f'{self.url}/v{events_version}/events',
                        'related_events': f'{self.url}/v{events_version}/events',
                        'threesixty_frames': f'{self.url}/v{threesixty_version}/360-frames',
                        'threesixty': f'{self.url}/v{threesixty_version}/360-frames',
                        'threesixty_visible_count': f'{self.url}/v{threesixty_version}/360-frames',
                        'threesixty_visible_distance': f'{self.url}/v{threesixty_version}/360-frames',
                       }        
        self.valid_match_data = ['lineups_players', 'lineups_events',
                                 'lineups_formations', 'lineups_positions', 'events', 'frames', 'tactics',
                                 'related_events', 'threesixty_frames', 'threesixty',
                                 'threesixty_visible_count', 'threesixty_visible_distance',
                                ]
        self.matches_version = matches_version

    def _match_url(self, competition_id, season_id):
        return f'{self.url}{self.matches_version}/v6/competitions/{competition_id}/seasons/{season_id}/matches.json'
