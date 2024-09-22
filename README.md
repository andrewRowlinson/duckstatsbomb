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
### Player lineups data
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_lineup_players = parser.match_data([3749052, 3749522], kind='lineup_players')
```

### Events data
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_events = parser.match_data([3749052, 3749522], kind='events')
```

### Shot freeze frames data
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_frames = parser.match_data([3749052, 3749522], kind='frames')
```

### Tactics data
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_tactics = parser.match_data([3749052, 3749522], kind='tactics')
```

### Related events data
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_related = parser.match_data([3749052, 3749522], kind='related_events')
```

### Three-sixty frames data
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_frames_360 = parser.match_data([3788741, 3788742], kind='threesixty_frames')
```

### Three-sixty data
```python
from duckstatsbomb import Sbopen
parser = Sbopen()
df_360 = parser.match_data([3788741, 3788742], kind='threesixty')
```