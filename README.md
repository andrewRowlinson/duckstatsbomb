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
print(parser.valid_data())
```
The valid kind values are:
* 'lineup_players'
* 'events'
* 'frames'
* 'tactics',
* 'related_events'
* 'threesixty_frames'
* 'threesixty'

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

# StatsBomb API

You can either provide the username and password as arguments (sb_username/ sb_password),
or set the SB_USERNAME and SB_PASSWORD environmental variables.

### Competitions data
```python
from duckstatsbomb import Sbapi
parser = Sbapi()
df_competitions = parser.competitions()
```

### Matches data
```python
from duckstatsbomb import Sbapi
parser = Sbapi()
df_matches = parser.matches(2, 44)
```

### List valid data types
```python
from duckstatsbomb import Sbapi
parser = Sbapi()
print(parser.valid_data())
```
The valid kind values are:
* 'lineup_players'
* 'events'
* 'frames'
* 'tactics',
* 'related_events'
* 'threesixty_frames'
* 'threesixty',
* 'lineup_events'
* 'lineup_formations'
* 'lineup_positions',
* 'threesixty_visible_count'
* 'threesixty_visible_distance'

### Data from one match
```python
from duckstatsbomb import Sbapi
parser = Sbapi()
df_lineup_players = parser.match_data(3749052, kind='lineup_players')
```

### Data from multiple matches
```python
from duckstatsbomb import Sbapi
parser = Sbapi()
df_events = parser.match_data([3749052, 3749522], kind='events')
```

### Data from one competition
```python
from duckstatsbomb import Sbapi
parser = Sbapi()
df_tactics = parser.competition_data(competition_id=16, kind='tactics')
```

### Data from one competition/season
```python
from duckstatsbomb import Sbapi
parser = Sbapi()
df_frames = parser.competition_data(competition_id=16, season_id=37, kind='frames')
```
