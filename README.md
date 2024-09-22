# duckstatsbomb
A data parser for StatsBomb soccer data using duckdb

# StatsBomb open-data

### Competitions data
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_competitions = parser.competitions()
```

### Matches data
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_matches = parser.matches(2, 44)
```

### List valid data types
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
parser.valid_data()
```

### Data from one match
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_lineup_players = parser.match_data(3749052, kind='lineup_players')
```

### Data from multiple matches
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_events = parser.match_data([3749052, 3749522], kind='events')
```

### Data from one competition
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_tactics = parser.competition_data(competition_id=16, kind='tactics')
```

### Data from one competition/season
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_frames = parser.competition_data(competition_id=16, season_id=37, kind='frames')
```
