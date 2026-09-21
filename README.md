# duckstatsbomb
Get flat tables from Hudl StatsBomb data, in multiple formats, in seconds once cached.

DuckDB does the parsing in parallel. You can parse a whole Premier League season of
1.3 million events from JSON into pandas, Polars or Arrow tables in a few seconds.
Or use the default output, a DuckDB relation, and filter to the shots and sum the xG
before building a DataFrame, so you can query the whole open-data set on your laptop.

Docs available at: https://duckstatsbomb.readthedocs.io

# Installation
The duckstatsbomb default has one dependency, DuckDB.
You can install optional dependencies to export to pandas, Polars, or Arrow:

```bash
pip install duckstatsbomb
pip install "duckstatsbomb[pandas]"  # pandas
pip install "duckstatsbomb[polars]"  # Polars & PyArrow
pip install "duckstatsbomb[arrow]"  # PyArrow
pip install "duckstatsbomb[all]"  # pandas, Polars & PyArrow
```

# Output formats

By default the library outputs a DuckDBPyRelation,
which you can filter, aggregate, or query.

```python
from duckstatsbomb import Sbopen
parser = Sbopen()
events = parser.competition_data(competition_id=43, season_id=106)
shots = events.filter("type_name = 'Shot'")
# top 4 goal scorers at the 2022 World Cup
top = (
    shots.aggregate(
        'player_name, team_name, count(*) as shots, '
        'round(sum(shot_statsbomb_xg), 2) as xg, '
        "count(*) filter (outcome_name = 'Goal') as goals",
        'player_name, team_name',
    )
    .order('goals desc, xg desc')
    .limit(4)
)
top.show()
```

You can also export to different formats:
```python
df_pandas = shots.df()
df_polars = shots.pl()
arrow_table = shots.to_arrow_table()
shots.to_csv('world_cup_2022_shots.csv')
shots.to_parquet('world_cup_2022_shots.parquet')
```

Or set the output format when creating the parser:
```python
parser = Sbopen(output_format='pandas')  # 'relation', 'pandas', 'polars' or 'arrow'
```

# Three parsers

There are three parsers: Sbopen, Sbapi, and Sbfiles.

All three have common methods: `competitions` (competition info),
`matches` (match info), and `match_data` (event, lineup, and three-sixty info).
Sbopen and Sbapi also include `competition_data` for getting whole season
data (event, lineups, and three-sixty).

## Competitions data
```python
from duckstatsbomb import Sbopen  # or Sbapi/Sbfiles
parser = Sbopen()  # or Sbapi() / Sbfiles()
competitions = parser.competitions()  # filenames for Sbfiles
```

## Matches data
```python
from duckstatsbomb import Sbopen  # or Sbapi/Sbfiles
parser = Sbopen()  # or Sbapi() / Sbfiles()
matches = parser.matches(2, 44)  # filenames for Sbfiles
```

## Event data
```python
from duckstatsbomb import Sbopen  # or Sbapi/Sbfiles
parser = Sbopen()  # or Sbapi() / Sbfiles()
# see parser.kinds for valid kind
events = parser.match_data(3857254, kind='events')  # filenames for Sbfiles
```

## Competition/season data
```python
from duckstatsbomb import Sbopen  # or Sbapi
parser = Sbopen()  # or Sbapi()
# see parser.kinds for valid kind
# no method for Sbfiles
lineup_players = parser.competition_data(competition_id=43, season_id=106,
                                         kind='lineup_players')
```

Sbapi needs a Hudl StatsBomb API subscription.
Pass the username and password as class arguments,
or set the SB_USERNAME and SB_PASSWORD environment variables.

# Cache

Sbopen and Sbapi cache the downloaded match files as raw JSON in the
`cache_path` directory. You can list the cache, delete the files for
particular matches, or delete the whole directory.

```python
from duckstatsbomb import Sbopen
parser = Sbopen()
parser.cached_files()  # path, size and UTC download time of each file
parser.stale_matches(43, 106, kind='events')  # identify stale match IDs
parser.clear_match_data([3857254, 3857255], kind='events')
parser.clear_cache()
```

The competitions and matches files are never cached, as they may change
often when Hudl StatsBomb release or reprocess data.

You can turn off the cache with the class argument `cache_enabled`,
change the `cache_path` or design your own cache backend and pass it to `cache`.
