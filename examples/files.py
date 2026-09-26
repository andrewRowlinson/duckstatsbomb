"""
=====
Files
=====

:class:`duckstatsbomb.Sbfiles` reads Hudl StatsBomb JSON files you already have, on
disk, in an object store such as S3, or at a url. The examples here read the
open-data from GitHub.

Download this page as a :download:`Python script <files.py>` or a
:download:`Jupyter notebook <files.ipynb>` to run it. The exports need the
optional dependencies: ``pip install "duckstatsbomb[all]"``.
"""

from pprint import pprint

from duckstatsbomb import Sbfiles

parser = Sbfiles()
# your own folder, e.g. 'statsbomb/data', or an s3:// url. This page reads the
# open-data on GitHub so it can run when the docs are built.
data_dir = 'https://raw.githubusercontent.com/statsbomb/open-data/master/data'

##############################################################################
# Output formats
# --------------
# By default the library outputs a
# `DuckDBPyRelation <https://duckdb.org/docs/lts/clients/python/relational_api>`_
# which you can filter, aggregate, or query.

events = parser.match_data(f'{data_dir}/events/3749052.json', kind='events')
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

pandas_parser = Sbfiles(output_format='pandas')  # 'relation', 'pandas', 'polars' or 'arrow'
df_events = pandas_parser.match_data(f'{data_dir}/events/3749052.json', kind='events')

##############################################################################
# Competitions
# ------------
# :meth:`~duckstatsbomb.Sbfiles.competitions` returns one row for
# each season of a competition.

competitions = parser.competitions(f'{data_dir}/competitions.json')
competitions.select('competition_id, season_id, competition_name, season_name').limit(5).show()

##############################################################################
# Matches
# -------
# :meth:`~duckstatsbomb.Sbfiles.matches` returns one row for each match in the
# matches file. The competitions and matches files can have any name, as the
# identifiers are in the JSON.

parser.matches(f'{data_dir}/matches/2/44.json').select(
    'match_id, home_team_name, away_team_name, home_score, away_score'
).limit(5).show()

##############################################################################
# You can get matches from several files by passing a list of files, or a glob
# such as ``'matches/*/*.json'`` for files on disk.

matches = parser.matches([f'{data_dir}/matches/2/44.json', f'{data_dir}/matches/43/106.json'])
matches.aggregate('competition_name, season_name, count(*) as matches').show()

##############################################################################
# Match data
# ----------
# :meth:`~duckstatsbomb.Sbfiles.match_data` reads the match files and returns a flat
# table with many rows per match. The match JSON does not contain the match_id, so
# name the files {match_id}.json and every row gets the match_id from the file name.

events = parser.match_data(f'{data_dir}/events/3749052.json', kind='events')
events.select('match_id, minute, second, type_name, player_name').limit(5).show()

##############################################################################
# Pass a list of files, or a glob such as ``'events/*.json'`` for files on disk,
# to read several matches into one table.

tactics = parser.match_data(
    [f'{data_dir}/events/3749052.json', f'{data_dir}/events/3749522.json'], kind='tactics'
)
tactics.aggregate('match_id, count(*) as players').show()

##############################################################################
# There is no ``competition_data``, as Sbfiles does not know where the match files
# are. For files on disk, a glob such as ``'lineups/*.json'`` reads them all.
# Otherwise build the paths from the match_id in the matches file.

match_ids = [
    match_id
    for (match_id,) in parser.matches(f'{data_dir}/matches/44/107.json')
    .select('match_id')
    .fetchall()
]
lineups = parser.match_data(
    [f'{data_dir}/lineups/{match_id}.json' for match_id in match_ids], kind='lineup_players'
)
lineups.aggregate('count(distinct match_id) as matches, count(*) as players').show()

##############################################################################
# duckstatsbomb splits the nested JSON structure into several flat tables.
# You can set the ``kind`` argument in match_data to return different tables.
# The :attr:`~duckstatsbomb.Sbfiles.kinds` property maps each table to the file
# it is parsed from, so you know which files to pass, e.g. ``kind='tactics'``
# reads the events files.

pprint(parser.kinds)

##############################################################################
# 360 data
# --------
# ``threesixty`` and ``threesixty_frames`` tables are read from the 360 files,
# which only some matches have.

parser.match_data(f'{data_dir}/three-sixty/3857254.json', kind='threesixty_frames').limit(5).show()

##############################################################################
# Parser options
# --------------
# :class:`~duckstatsbomb.Sbfiles` takes a few more arguments. The data versions
# default to the open-data versions, so set them to match your files, e.g. files
# saved from the API.

parser = Sbfiles(
    database='statsbomb.duckdb',  # use a persistent DuckDB database, not in-memory
    duckdb_threads=8,  # set the number of DuckDB threads
    connection_kws={'config': {'memory_limit': '4GB'}},  # passed to duckdb.connect
    competitions_version=4,  # 4 supported
    matches_version=6,  # 3 or 6 supported
    events_version=11,  # 4, 8 or 11 supported
    lineup_version=5,  # 2, 4 or 5 supported
    threesixty_version=2,  # 1 or 2 supported
)
parser.close_connection()  # close the DuckDB connection when you are done
