"""
=========
Open-data
=========

:class:`duckstatsbomb.Sbopen` loads the free
`Hudl StatsBomb open-data <https://github.com/statsbomb/open-data>`_.
The open-data is available under a non-commercial license.

Download this page as a :download:`Python script <open_data.py>` or a
:download:`Jupyter notebook <open_data.ipynb>` to run it. The exports need the
optional dependencies: ``pip install "duckstatsbomb[all]"``.
"""

from pprint import pprint

from duckstatsbomb import Sbopen
from duckstatsbomb.cache import LocalCache

parser = Sbopen()

##############################################################################
# Output formats
# --------------
# By default the library outputs a
# `DuckDBPyRelation <https://duckdb.org/docs/lts/clients/python/relational_api>`_
# which you can filter, aggregate, or query.

events = parser.match_data(3749052, kind='events')
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

pandas_parser = Sbopen(output_format='pandas')  # 'relation', 'pandas', 'polars' or 'arrow'
df_events = pandas_parser.match_data(3749052, kind='events')

##############################################################################
# Competitions
# ------------
# :meth:`~duckstatsbomb.Sbopen.competitions` returns one row for
# each season of a competition.

competitions = parser.competitions()
competitions.select('competition_id, season_id, competition_name, season_name').limit(5).show()

##############################################################################
# Matches
# -------
# :meth:`~duckstatsbomb.Sbopen.matches` returns one row for each match
# within a competition's season.

parser.matches(competition_id=2, season_id=44).select(
    'match_id, home_team_name, away_team_name, home_score, away_score'
).limit(5).show()

##############################################################################
# You can get matches from multiple competitions and seasons by providing
# lists of competition/season identifiers. The lists are paired in order,
# so here 2 goes with 44, and 43 with 106.

matches = parser.matches(competition_id=[2, 43], season_id=[44, 106])
matches.aggregate('competition_name, season_name, count(*) as matches').show()

##############################################################################
# Match data
# ----------
# :meth:`~duckstatsbomb.Sbopen.match_data` downloads the file for each match_id
# and returns a flat table with many rows per match. Every row has the match_id.

events = parser.match_data(3749052, kind='events')
events.select('match_id, minute, second, type_name, player_name').limit(5).show()

##############################################################################
# Pass a list of match_id to read several matches into one table.

tactics = parser.match_data([3749052, 3749522], kind='tactics')
tactics.aggregate('match_id, count(*) as players').show()

##############################################################################
# Or use :meth:`~duckstatsbomb.Sbopen.competition_data` to read every
# match within a competition's season. Like matches, it also takes lists
# of competition/season identifiers, paired in order.

lineups = parser.competition_data(competition_id=44, season_id=107, kind='lineup_players')
lineups.aggregate('count(distinct match_id) as matches, count(*) as players').show()

##############################################################################
# duckstatsbomb splits the nested JSON structure into several flat tables.
# The ``kind`` argument in match_data and competition_data chooses which
# table to return. The :attr:`~duckstatsbomb.Sbopen.kinds` property lists
# the available tables.

pprint(list(parser.kinds))

##############################################################################
# 360 data
# --------
# ``threesixty`` and ``threesixty_frames`` tables are only available for
# some matches. This query returns the seasons with 360 data in the open-data:

seasons = (
    parser.competitions()
    .filter('match_available_360 is not null')
    .select('competition_id, season_id')
    .fetchall()
)
matches = parser.matches(
    competition_id=[competition_id for competition_id, _ in seasons],
    season_id=[season_id for _, season_id in seasons],
)
matches.aggregate(
    """competition_id, season_id,
    count(*) filter (match_status_360 = 'available') as matches_360,
    count(*) as matches"""
).order('competition_id, season_id').show()

##############################################################################
# And here is an example table of threesixty_frames.

parser.match_data(3857254, kind='threesixty_frames').limit(5).show()

##############################################################################
# Cache
# -----
# The downloaded match files are cached as raw JSON in ``cache_path``.
# The competitions and matches files are not cached, as they change
# when Hudl StatsBomb release or reprocess data.

##############################################################################
# :meth:`~duckstatsbomb.Sbopen.cached_files` lists the path, size and UTC download
# time of each cached file.

parser.cached_files().aggregate('count(*) as files, sum(size) as bytes').show()

##############################################################################
# :meth:`~duckstatsbomb.Sbopen.stale_matches` finds the cached matches Hudl StatsBomb
# have updated, and :meth:`~duckstatsbomb.Sbopen.clear_match_data` deletes them so
# the next read downloads them again. The ``source`` argument sets the type
# of source file: ``'events'``, ``'lineups'`` or ``'threesixty'``.
# Clearing the events file clears every table parsed from it, e.g. the tactics too.
# Turn the cache off with ``Sbopen(cache_enabled=False)``
# or change the cache location with ``cache_path``.

stale = parser.stale_matches(competition_id=2, season_id=44, source='events')
parser.clear_match_data(stale, source='events')
# parser.clear_cache()  # or delete the whole cache

##############################################################################
# Parser options
# --------------
# :class:`~duckstatsbomb.Sbopen` takes a few more arguments.

parser = Sbopen(
    database='statsbomb.duckdb',  # use a persistent DuckDB database, not in-memory
    duckdb_threads=8,  # set the number of DuckDB threads
    cache=LocalCache('statsbomb_cache'),  # or your own duckstatsbomb.cache.CacheBase subclass
    connection_kws={'config': {'memory_limit': '4GB'}},  # passed to duckdb.connect
)
parser.close_connection()  # close the DuckDB connection when you are done
