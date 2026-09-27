"""
===
API
===

:class:`duckstatsbomb.Sbapi` loads data from the Hudl StatsBomb API, which needs a
subscription. It has the same methods as :class:`duckstatsbomb.Sbopen`.

This page is not run when the documentation is built, as it needs credentials, so
there is no output. Download it as a :download:`Python script <api.py>` or a
:download:`Jupyter notebook <api.ipynb>` and set your credentials to run it. The
exports need the optional dependencies: ``pip install "duckstatsbomb[all]"``.
"""

from pprint import pprint

from duckstatsbomb import Sbapi
from duckstatsbomb.cache import LocalCache

##############################################################################
# Credentials
# -----------
# Set the SB_USERNAME and SB_PASSWORD environment variables and create the parser with
# no arguments, or pass them as arguments:
# ``Sbapi(sb_username='your_username', sb_password='your_password')``.

parser = Sbapi()

##############################################################################
# Output formats
# --------------
# By default the library outputs a
# `DuckDBPyRelation <https://duckdb.org/docs/lts/clients/python/relational_api>`_
# which you can filter, aggregate, or query.

events = parser.match_data(3837609, kind='events')
shots = events.filter("type_name = 'Shot'")
shots.aggregate('player_name, count(*) as shots, round(sum(shot_statsbomb_xg), 2) as xg').order(
    'xg desc'
).limit(5).show()

##############################################################################
# You can also export to other formats. pandas, Polars and PyArrow need
# the optional dependencies.

df_pandas = shots.df()  # pip install "duckstatsbomb[pandas]"
df_polars = shots.pl()  # pip install "duckstatsbomb[polars]"
arrow_table = shots.to_arrow_table()  # pip install "duckstatsbomb[arrow]"
shots.to_parquet('shots.parquet')
shots.to_csv('shots.csv')

##############################################################################
# Or set the output format when creating the parser.

pandas_parser = Sbapi(output_format='pandas')  # 'relation', 'pandas', 'polars' or 'arrow'
df_events = pandas_parser.match_data(3837609, kind='events')

##############################################################################
# Competitions
# ------------
# :meth:`~duckstatsbomb.Sbapi.competitions` returns one row per competition/season
# your subscription covers.

competitions = parser.competitions()
competitions.select('competition_id, season_id, competition_name, season_name').show()

##############################################################################
# Matches
# -------
# :meth:`~duckstatsbomb.Sbapi.matches` returns one row for each match
# within a competition's season.

matches = parser.matches(competition_id=2, season_id=235)
matches.select('match_id, match_date, home_team_name, away_team_name').limit(5).show()

##############################################################################
# You can get matches from multiple competitions and seasons by providing
# lists of competition/season identifiers. The lists are paired in order,
# so here 2 goes with 235, and 11 with 235.

matches = parser.matches(competition_id=[2, 11], season_id=[235, 235])
matches.aggregate('competition_name, season_name, count(*) as matches').show()

##############################################################################
# Match data
# ----------
# :meth:`~duckstatsbomb.Sbapi.match_data` downloads the file for each match_id
# and returns a flat table with many rows per match. Every row has the match_id.

events = parser.match_data(3837609, kind='events')
events.select('match_id, minute, second, type_name, player_name').limit(5).show()

##############################################################################
# Pass a list of match_id to read several matches into one table.

tactics = parser.match_data([3837609, 3837610], kind='tactics')
tactics.aggregate('match_id, count(*) as players').show()

##############################################################################
# Or use :meth:`~duckstatsbomb.Sbapi.competition_data` to read every
# match within a competition's season. Like matches, it also takes lists
# of competition/season identifiers, paired in order.

lineups = parser.competition_data(competition_id=2, season_id=235, kind='lineup_players')
lineups.aggregate('count(distinct match_id) as matches, count(*) as players').show()

##############################################################################
# duckstatsbomb splits the nested JSON structure into several flat tables.
# You can set the ``kind`` argument in match_data and competition_data
# to return different tables. The :attr:`~duckstatsbomb.Sbapi.kinds`
# property lists the available tables. The API data versions are newer than
# the open-data, so there are more kinds.

pprint(list(parser.kinds))
# ['lineup_players',
#  'events',
#  'frames',
#  'tactics',
#  'related_events',
#  'threesixty_frames',
#  'threesixty',
#  'lineup_events',
#  'lineup_formations',
#  'lineup_positions',
#  'threesixty_visible_count',
#  'threesixty_visible_distance',
#  'defensive_responsibility']

##############################################################################
# Cache
# -----
# The downloaded match files are cached as raw JSON in ``cache_path``.
# The competitions and matches files are not cached, as they change
# when Hudl StatsBomb release or reprocess data.

##############################################################################
# :meth:`~duckstatsbomb.Sbapi.cached_files` lists the path, size and UTC download
# time of each cached file.

parser.cached_files().aggregate('count(*) as files, sum(size) as bytes').show()

##############################################################################
# :meth:`~duckstatsbomb.Sbapi.stale_matches` finds the cached matches Hudl StatsBomb
# have updated, and :meth:`~duckstatsbomb.Sbapi.clear_match_data` deletes them so
# the next read downloads them again. The ``source`` argument sets the type
# of source file: ``'events'``, ``'lineups'`` or ``'threesixty'``.
# Clearing the events file clears every table parsed from it, e.g. the tactics too.
# Turn the cache off with ``Sbapi(cache_enabled=False)``
# or change the cache location with ``cache_path``.

stale = parser.stale_matches(competition_id=2, season_id=235, source='events')
parser.clear_match_data(stale, source='events')
# parser.clear_cache()  # or delete the whole cache

##############################################################################
# Parser options
# --------------
# :class:`~duckstatsbomb.Sbapi` takes a few more arguments.

parser = Sbapi(
    database='statsbomb.duckdb',  # use a persistent DuckDB database, not in-memory
    duckdb_threads=8,  # set the number of DuckDB threads
    cache=LocalCache('statsbomb_cache'),  # or your own duckstatsbomb.cache.CacheBase subclass
    connection_kws={'config': {'memory_limit': '4GB'}},  # passed to duckdb.connect
    url='https://data.statsbombservices.com/api',  # the API base url
    competitions_version=4,  # 4 supported
    matches_version=6,  # 3 or 6 supported
    events_version=11,  # 4, 8 or 11 supported
    lineup_version=5,  # 2, 4 or 5 supported
    threesixty_version=2,  # 1 or 2 supported
)
parser.close_connection()  # close the DuckDB connection when you are done
