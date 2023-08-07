"""`duckstatsbomb.opendata` is a python module for loading StatsBomb open-data."""

import pkgutil
import duckdb

__all__ = ['Sbopen']


class Sbopen:
    """ Class for loading data from the StatsBomb open-data.
    The data is available at: https://github.com/statsbomb/open-data under
    a non-commercial license.
    """

    def __init__(self):
        self.url = 'https://raw.githubusercontent.com/statsbomb/open-data/master/data/'
        self.competition_sql = pkgutil.get_data(__package__, 'sql/competition/v4/competition.sql')
        self.match_sql = pkgutil.get_data(__package__, 'sql/match/v3/match.sql')
        self.lineup_sql = pkgutil.get_data(__package__, 'sql/lineup/v2/lineup.sql')
        self.event_sql = pkgutil.get_data(__package__, 'sql/event/v4/event.sql')
        self.freeze_sql = pkgutil.get_data(__package__, 'sql/event/v4/freeze.sql')
        self.tactic_sql = pkgutil.get_data(__package__, 'sql/event/v4/tactic.sql')
        self.related_sql = pkgutil.get_data(__package__, 'sql/event/v4/related.sql')
        self.threesixty_freeze_sql = pkgutil.get_data(__package__, 'sql/threesixty/v1/freeze_frame.sql')
        self.threesixty_sql = pkgutil.get_data(__package__, 'sql/threesixty/v1/threesixty.sql')
        self.con = duckdb.connect(database=':memory:')

    def event(self, match_id):
        """ StatsBomb event open-data.

        Parameters
        ----------
        match_id : int

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> events = parser.event(3788741)
        """
        url = f'{self.url}events/{match_id}.json'
        return self.con.execute(self.event_sql, {'filename': url}).df()

    def freeze(self, match_id):
        """ StatsBomb event shot freeze frame open-data.

        Parameters
        ----------
        match_id : int

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> freeze_frames = parser.freeze(3788741)
        """
        url = f'{self.url}events/{match_id}.json'
        return self.con.execute(self.freeze_sql, {'filename': url}).df()

    def tactic(self, match_id):
        """ StatsBomb event tactics open-data.

        Parameters
        ----------
        match_id : int

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> tactics = parser.tactic(3788741)
        """
        url = f'{self.url}events/{match_id}.json'
        return self.con.execute(self.tactic_sql, {'filename': url}).df()

    def related(self, match_id):
        """ StatsBomb related events open-data.

        Parameters
        ----------
        match_id : int

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> related_events = parser.related(3788741)
        """
        url = f'{self.url}events/{match_id}.json'
        return self.con.execute(self.related_sql, {'filename': url}).df()

    def lineup(self, match_id):
        """ StatsBomb lineup open-data.

        Parameters
        ----------
        match_id : int

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> lineups = parser.lineup(3788741)
        """
        url = f'{self.url}lineups/{match_id}.json'
        return self.con.execute(self.lineup_sql, {'filename': url}).df()

    def match(self, competition_id, season_id):
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
        >>> matches = parser.match(11, 1)
        """
        url = f'{self.url}matches/{competition_id}/{season_id}.json'
        return self.con.execute(self.match_sql, {'filename': url}).df()

    def competition(self):
        """ StatsBomb competition open-data.

        Returns
        -------
        competition
            A dataframe or a flattened list of dictionaries.

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> competitions = parser.competition()
        """
        url = f'{self.url}competitions.json'
        return self.con.execute(self.competition_sql, {'filename': url}).df()


    def threesixty_frame(self, match_id):
        """ StatsBomb 360 open-data freeze-frames.

        Parameters
        ----------
        match_id : int

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> frames = parser.threesixty_frame(3788741)
        """
        url = f'{self.url}three-sixty/{match_id}.json'
        return self.con.execute(self.threesixty_freeze_sql, {'filename': url}).df()


    def threesixty(self, match_id):
        """ StatsBomb 360 open-data.

        Parameters
        ----------
        match_id : int

        Returns
        -------
        pandas.DataFrame

        Examples
        --------
        >>> from duckstatsbomb import Sbopen
        >>> parser = Sbopen()
        >>> threesixty = parser.threesixty(3788741)
        """
        url = f'{self.url}three-sixty/{match_id}.json'
        return self.con.execute(self.threesixty_sql, {'filename': url}).df()

    def close(self):
        """ Close the duckdb connection."""
        self.con.close()
