"""Generate the made-up StatsBomb test data in tests/data.

StatsBomb data cannot be redistributed, so the tests run on made-up files. This script
reads nothing: the shape of each file and StatsBomb's vocabulary (event types,
outcomes, body parts and so on) are written out below from StatsBomb's API
specifications, and every team, player, official, id, time, location and model output
is made up.

A match has one event of each type at a random time, plus the Starting XI and the Half
Start and Half End of both periods in their proper places. Every attribute an event
type can carry is filled in, so the test data exercises every column the SQL reads.

Run it from the repository root:

    uv run python tests/make_test_data.py

The output is seeded, so rerunning it gives the same files.
"""

import datetime
import json
import random
import shutil
import uuid
from pathlib import Path

OUT_DIR = Path(__file__).parent / 'data'

# the data versions to write, which should match duckstatsbomb's SUPPORTED_VERSIONS
EVENTS_VERSIONS = [4, 8, 11]
LINEUP_VERSIONS = [2, 4, 5]
THREESIXTY_VERSIONS = [1, 2]
MATCHES_VERSIONS = [3, 6]
COMPETITIONS_VERSIONS = [4]

MATCH_ID = 1001
COMPETITION = {'competition_id': 1, 'country_name': 'Pondland', 'competition_name': 'Duck League'}
SEASON = {'season_id': 1, 'season_name': '2025/2026'}
COUNTRY = {'id': 1, 'name': 'Pondland'}
# StatsBomb sends its update times to the microsecond, the millisecond or the minute,
# and null when there is nothing to update, e.g. a match with no 360 data. Each field
# only uses the formats seen for it in real data:
#   last_updated                    microseconds, milliseconds, minutes
#   last_updated_360                microseconds, milliseconds, minutes, null
#   match_updated, match_available  microseconds
#   match_updated_360               microseconds, milliseconds, null
#   match_available_360             microseconds, null
MICROSECONDS, MILLISECONDS, MINUTES = (
    '2025-06-01T12:00:00.123456',
    '2025-06-01T12:00:00.123',
    '2025-06-01T12:00',
)
# last_updated and last_updated_360 for each match
MATCH_UPDATED = [
    (MICROSECONDS, MILLISECONDS),
    (MILLISECONDS, MINUTES),
    (MINUTES, MICROSECONDS),
    (MICROSECONDS, None),
]

TEAMS = {
    101: 'Mallard Town',
    102: 'Drake United',
    103: 'Teal Rovers',
    104: 'Wigeon Athletic',
}
# the matches: the first is the one with events, lineups and 360 data
MATCHES = [(1001, 101, 102), (1002, 103, 104), (1003, 102, 103), (1004, 104, 101)]
HOME, AWAY = MATCHES[0][1:]

FIRST_NAMES = ['Ada', 'Bram', 'Cora', 'Dex', 'Edie', 'Finn', 'Gus', 'Hana', 'Ivo', 'Juno']
LAST_NAMES = ['Puddle', 'Reed', 'Quill', 'Waddle', 'Paddle', 'Feather', 'Marsh', 'Webb']

# a 4-2-3-1 in StatsBomb's position ids, then the substitutes
FORMATION = 4231
POSITIONS = [
    (1, 'Goalkeeper'),
    (2, 'Right Back'),
    (3, 'Right Center Back'),
    (5, 'Left Center Back'),
    (6, 'Left Back'),
    (9, 'Right Defensive Midfield'),
    (11, 'Left Defensive Midfield'),
    (17, 'Right Wing'),
    (19, 'Center Attacking Midfield'),
    (21, 'Left Wing'),
    (23, 'Striker'),
]
N_SUBSTITUTES = 3
HALF_LENGTH = 45 * 60

# --- StatsBomb's vocabulary, from the events specification ---

PLAY_PATTERNS = [
    (1, 'Regular Play'),
    (2, 'From Corner'),
    (3, 'From Free Kick'),
    (4, 'From Throw In'),
    (5, 'Other'),
    (6, 'From Counter'),
    (7, 'From Goal Kick'),
    (8, 'From Keeper'),
    (9, 'From Kick Off'),
]
CARDS = [(7, 'Yellow Card'), (6, 'Second Yellow'), (5, 'Red Card')]
BODY_PARTS = [(37, 'Head'), (38, 'Left Foot'), (40, 'Right Foot'), (70, 'Other')]
FOUL_TYPES = [
    (19, '6 Seconds'),
    (20, 'Backpass Pick'),
    (21, 'Dangerous Play'),
    (22, 'Dive'),
    (23, 'Foul Out'),
    (24, 'Handball'),
]
TACKLE_OUTCOMES = [
    (4, 'Won'),
    (13, 'Lost In Play'),
    (14, 'Lost Out'),
    (15, 'Success'),
    (16, 'Success In Play'),
    (17, 'Success Out'),
]
CLUSTER_LABEL_PARTS = [
    ['Defensive third', 'Midfield third', 'Attacking third'],
    ['Left', 'Center', 'Right'],
    ['To left', 'To right', 'Forwards', 'Backwards'],
    ['Short', 'Long'],
    ['Ground Pass', 'High Pass'],
]


class Field:
    """How to make one attribute's value, and the data versions that carry it.

    Parameters
    ----------
    make : callable
        Called with the EventMaker and the event so far, returning the value.
    since, until : int
        The first and last events version with the attribute.
    """

    def __init__(self, make, since=0, until=99):
        self.make = make
        self.since = since
        self.until = until


def since(version, make):
    return Field(make, since=version)


def until(version, make):
    return Field(make, until=version)


def true(maker, event):
    """The flags are only present when set, so a present flag is true."""
    return True


def probability(maker, event):
    return maker.number(0, 1)


def number(low, high):
    return lambda maker, event: maker.number(low, high)


def vocab(options):
    return lambda maker, event: maker.pick(options)


def location(maker, event):
    return maker.location(3 if event['type']['name'] == 'Shot' else 2)


# the linked events, filled in once every event exists
class Link:
    def __init__(self, type_name):
        self.type_name = type_name


def link(type_name):
    return lambda maker, event: Link(type_name)


# counterpress was nested in the defensive event types before v11
COUNTERPRESS = until(8, true)
DUEL_WIN_PROBABILITY = since(11, probability)

# event type id, name, the attribute object named after the type, and its attributes
EVENT_TYPES = [
    (
        33,
        '50/50',
        '50_50',
        {
            'outcome': vocab(
                [
                    (108, 'Won'),
                    (109, 'Lost'),
                    (147, 'Success To Team'),
                    (148, 'Success To Opposition'),
                ]
            ),
            'counterpress': COUNTERPRESS,
        },
    ),
    (24, 'Bad Behaviour', 'bad_behaviour', {'card': vocab(CARDS)}),
    (42, 'Ball Receipt*', 'ball_receipt', {'outcome': vocab([(9, 'Incomplete')])}),
    (2, 'Ball Recovery', 'ball_recovery', {'offensive': true, 'recovery_failure': true}),
    (
        6,
        'Block',
        'block',
        {
            'deflection': true,
            'offensive': true,
            'save_block': true,
            'counterpress': COUNTERPRESS,
        },
    ),
    (43, 'Carry', 'carry', {'end_location': location}),
    (
        9,
        'Clearance',
        'clearance',
        {
            'aerial_won': true,
            'duel_win_probability': DUEL_WIN_PROBABILITY,
            'body_part': vocab(BODY_PARTS),
        },
    ),
    (3, 'Dispossessed', None, {}),
    (
        14,
        'Dribble',
        'dribble',
        {
            'overrun': true,
            'nutmeg': true,
            'outcome': vocab([(8, 'Complete'), (9, 'Incomplete')]),
            'no_touch': true,
        },
    ),
    (39, 'Dribbled Past', 'dribbled_past', {'counterpress': COUNTERPRESS}),
    (
        4,
        'Duel',
        'duel',
        {
            'counterpress': COUNTERPRESS,
            'type': vocab([(10, 'Aerial Lost'), (11, 'Tackle')]),
            'outcome': vocab(TACKLE_OUTCOMES),
            'duel_win_probability': DUEL_WIN_PROBABILITY,
            'hops_raw_rating': since(11, number(300, 600)),
            'hops_rating': since(11, probability),
            'hops_opponent_raw_rating': since(11, number(300, 600)),
            'hops_opponent_rating': since(11, probability),
        },
    ),
    (37, 'Error', None, {}),
    (
        22,
        'Foul Committed',
        'foul_committed',
        {
            'counterpress': COUNTERPRESS,
            'offensive': true,
            'type': lambda maker, event: maker.pick(
                FOUL_TYPES + ([(25, '8 Seconds')] if maker.version >= 11 else [])
            ),
            'advantage': true,
            'penalty': true,
            'card': vocab(CARDS[:1]),
        },
    ),
    (21, 'Foul Won', 'foul_won', {'defensive': true, 'advantage': true, 'penalty': true}),
    (
        23,
        'Goal Keeper',
        'goalkeeper',
        {
            'position': vocab([(42, 'Moving'), (43, 'Prone'), (44, 'Set')]),
            'technique': vocab([(45, 'Diving'), (46, 'Standing')]),
            'body_part': vocab(
                [
                    (35, 'Both Hands'),
                    (36, 'Chest'),
                    (37, 'Head'),
                    (38, 'Left Foot'),
                    (39, 'Left Hand'),
                    (40, 'Right Foot'),
                    (41, 'Right Hand'),
                ]
            ),
            'type': vocab(
                [
                    (25, 'Collected'),
                    (26, 'Goal Conceded'),
                    (27, 'Keeper Sweeper'),
                    (30, 'Punch'),
                    (31, 'Save'),
                    (32, 'Shot Faced'),
                    (33, 'Shot Saved'),
                    (34, 'Smother'),
                ]
            ),
            'outcome': vocab(
                [
                    (47, 'Claim'),
                    (48, 'Clear'),
                    (51, 'In Play'),
                    (52, 'In Play Danger'),
                    (53, 'In Play Safe'),
                    (55, 'No Touch'),
                    (58, 'Touched In'),
                    (59, 'Touched Out'),
                ]
            ),
            'end_location': location,
        },
    ),
    (34, 'Half End', 'half_end', {'early_video_end': true, 'match_suspended': true}),
    (18, 'Half Start', 'half_start', {'late_video_start': true}),
    (40, 'Injury Stoppage', 'injury_stoppage', {'in_chain': true}),
    (10, 'Interception', 'interception', {'outcome': vocab([(1, 'Lost')] + TACKLE_OUTCOMES)}),
    (
        38,
        'Miscontrol',
        'miscontrol',
        {'aerial_won': true, 'duel_win_probability': DUEL_WIN_PROBABILITY},
    ),
    (8, 'Offside', 'offside', {'advantage': since(11, true)}),
    (20, 'Own Goal Against', None, {}),
    (25, 'Own Goal For', None, {}),
    (
        30,
        'Pass',
        'pass',
        {
            'recipient': lambda maker, event: maker.teammate(event),
            'length': number(1, 60),
            'angle': number(-3.14, 3.14),
            'aerial_won': true,
            'duel_win_probability': DUEL_WIN_PROBABILITY,
            'height': vocab([(1, 'Ground Pass'), (2, 'Low Pass'), (3, 'High Pass')]),
            'end_location': location,
            'assisted_shot_id': link('Shot'),
            'backheel': true,
            'deflected': true,
            'miscommunication': true,
            'cross': true,
            'cut_back': true,
            'switch': true,
            'shot_assist': true,
            'goal_assist': true,
            'body_part': vocab(
                BODY_PARTS + [(68, 'Drop Kick'), (69, 'Keeper Arm'), (106, 'No Touch')]
            ),
            'type': vocab(
                [
                    (61, 'Corner'),
                    (62, 'Free Kick'),
                    (63, 'Goal Kick'),
                    (64, 'Interception'),
                    (65, 'Kick Off'),
                    (66, 'Recovery'),
                    (67, 'Throw-in'),
                ]
            ),
            'outcome': vocab(
                [
                    (9, 'Incomplete'),
                    (74, 'Injury Clearance'),
                    (75, 'Out'),
                    (76, 'Pass Offside'),
                    (77, 'Unknown'),
                ]
            ),
            'technique': vocab(
                [
                    (104, 'Inswinging'),
                    (105, 'Outswinging'),
                    (107, 'Straight'),
                    (108, 'Through Ball'),
                ]
            ),
            'pass_cluster_id': since(8, lambda maker, event: maker.rng.randint(0, 59)),
            'pass_cluster_label': since(
                8,
                lambda maker, event: ' - '.join(
                    maker.rng.choice(part) for part in CLUSTER_LABEL_PARTS
                ),
            ),
            'pass_cluster_probability': since(8, probability),
            'pass_success_probability': since(8, probability),
            'no_touch': true,
            'xclaim': since(8, probability),
        },
    ),
    (26, 'Player On', None, {}),
    (27, 'Player Off', 'player_off', {'permanent': true}),
    (17, 'Pressure', 'pressure', {'counterpress': COUNTERPRESS}),
    (41, 'Referee Ball-Drop', None, {}),
    (28, 'Shield', None, {}),
    (
        16,
        'Shot',
        'shot',
        {
            'key_pass_id': link('Pass'),
            'end_location': location,
            'aerial_won': true,
            'duel_win_probability': DUEL_WIN_PROBABILITY,
            'follows_dribble': true,
            'first_time': true,
            'freeze_frame': lambda maker, event: maker.shot_freeze_frame(event),
            'open_goal': true,
            'one_on_one': true,
            'statsbomb_xg': probability,
            'gk_save_difficulty_xg': since(8, probability),
            'shot_execution_xg': since(8, probability),
            'shot_execution_xg_uplift': since(8, number(-0.2, 0.2)),
            'gk_positioning_xg_suppression': since(8, number(-0.2, 0.2)),
            'gk_shot_stopping_xg_suppression': since(8, number(-0.2, 0.2)),
            'deflected': true,
            'technique': vocab(
                [
                    (89, 'Backheel'),
                    (90, 'Diving Header'),
                    (91, 'Half Volley'),
                    (92, 'Lob'),
                    (93, 'Normal'),
                    (94, 'Overhead Kick'),
                    (95, 'Volley'),
                ]
            ),
            'shot_shot_assist': since(8, true),
            'shot_goal_assist': since(8, true),
            'body_part': vocab(BODY_PARTS),
            'type': vocab(
                [
                    (61, 'Corner'),
                    (62, 'Free Kick'),
                    (87, 'Open Play'),
                    (88, 'Penalty'),
                    (65, 'Kick Off'),
                ]
            ),
            'outcome': vocab(
                [
                    (96, 'Blocked'),
                    (97, 'Goal'),
                    (98, 'Off T'),
                    (99, 'Post'),
                    (100, 'Saved'),
                    (101, 'Wayward'),
                    (115, 'Saved Off T'),
                    (116, 'Saved To Post'),
                ]
            ),
            'redirect': true,
        },
    ),
    (35, 'Starting XI', None, {}),
    (
        19,
        'Substitution',
        'substitution',
        {
            'replacement': lambda maker, event: maker.player_ref(
                maker.world.substitutes(event['team']['id'])[0]
            ),
            'outcome': vocab([(102, 'Injury'), (103, 'Tactical')]),
        },
    ),
    (36, 'Tactical Shift', None, {}),
]
EVENT_TYPES = {name: (type_id, key, fields) for type_id, name, key, fields in EVENT_TYPES}

# the events that open the match and each half, which keep their place in the timeline
STRUCTURAL = {'Starting XI', 'Half Start', 'Half End'}
# the events that carry the tactics object
TACTICS = {'Starting XI', 'Tactical Shift'}
# the events with no player, and those with no location
NO_PLAYER = STRUCTURAL | {'Tactical Shift', 'Referee Ball-Drop'}
NO_LOCATION = NO_PLAYER - {'Referee Ball-Drop'} | {
    'Substitution',
    'Player On',
    'Player Off',
    'Bad Behaviour',
    'Injury Stoppage',
}
# the events given each of the attributes that are only sometimes present
FLAGS = {
    'under_pressure': 'Pass',
    'counterpress': 'Pressure',
    'out': 'Clearance',
    'off_camera': 'Foul Won',
}
# the attacking events given v11's defensive responsibility and set piece phase
ATTACKING = {'Pass', 'Carry', 'Dribble', 'Shot'}
OBV = [
    'obv_for_after',
    'obv_for_before',
    'obv_for_net',
    'obv_against_after',
    'obv_against_before',
    'obv_against_net',
    'obv_total_net',
]


# the links between events. StatsBomb records most both ways, but some only one way,
# e.g. a carry lists the pass before it, but the pass does not list the carry
RELATED = [
    ('Pass', 'Ball Receipt*', True),
    ('Carry', 'Pass', False),
    ('Pressure', 'Carry', False),
    ('Shot', 'Goal Keeper', True),
    ('Shot', 'Block', True),
    ('Foul Committed', 'Foul Won', True),
    ('Dribble', 'Dribbled Past', True),
    ('Duel', 'Dribble', True),
]


def clock(seconds, short):
    """Format a time in seconds as StatsBomb does: 'MM:SS' or 'HH:MM:SS.fff'."""
    if short:
        return f'{int(seconds) // 60:02}:{int(seconds) % 60:02}'
    return f'{int(seconds) // 3600:02}:{int(seconds) % 3600 // 60:02}:{seconds % 60:06.3f}'


def write(data, *parts):
    """Write a file below OUT_DIR, with no trailing newline, like the API."""
    path = OUT_DIR.joinpath(*parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))


class World:
    """The made-up teams and players, which every data version shares."""

    def __init__(self):
        rng = random.Random('players')
        names = [f'{first} {last}' for first in FIRST_NAMES for last in LAST_NAMES]
        rng.shuffle(names)
        self.players = {}
        for team_id in TEAMS:
            squad = []
            for number, position in enumerate(POSITIONS + [None] * N_SUBSTITUTES, start=1):
                name = names.pop()
                birth_date = datetime.date(1990, 1, 1) + datetime.timedelta(rng.randint(0, 5000))
                squad.append(
                    {
                        'id': team_id * 100 + number,
                        'name': name,
                        'nickname': name.split()[0] if rng.random() < 0.3 else None,
                        'jersey_number': number,
                        'position': position,
                        'birth_date': birth_date.isoformat(),
                        'height': float(rng.randint(165, 200)),
                        'weight': float(rng.randint(60, 95)),
                        'hops': (round(rng.uniform(0, 1), 4), round(rng.uniform(300, 600), 2)),
                    }
                )
            self.players[team_id] = squad

    def starters(self, team_id):
        return [player for player in self.players[team_id] if player['position']]

    def substitutes(self, team_id):
        return [player for player in self.players[team_id] if not player['position']]


def team_ref(team_id):
    return {'id': team_id, 'name': TEAMS[team_id]}


def position_ref(player):
    position_id, name = player['position']
    return {'id': position_id, 'name': name}


def other_team(team_id):
    return AWAY if team_id == HOME else HOME


class EventMaker:
    """Makes the events of a match at one events version."""

    def __init__(self, world, version):
        self.world = world
        self.version = version
        self.rng = random.Random(f'events{version}')

    def pick(self, options):
        option_id, name = self.rng.choice(options)
        return {'id': option_id, 'name': name}

    def number(self, low, high):
        return round(self.rng.uniform(low, high), 6)

    def location(self, dimensions=2):
        point = [round(self.rng.uniform(0, 120), 1), round(self.rng.uniform(0, 80), 1)]
        if dimensions == 3:
            point.append(round(self.rng.uniform(0, 2.5), 1))
        return point

    def player_ref(self, player):
        return {'id': player['id'], 'name': player['name']}

    def teammate(self, event):
        return self.player_ref(self.rng.choice(self.world.starters(event['team']['id'])))

    def shot_freeze_frame(self, event):
        frame = []
        for team_id in [event['team']['id'], other_team(event['team']['id'])] * 3:
            player = self.rng.choice(self.world.starters(team_id))
            frame.append(
                {
                    'location': self.location(),
                    'player': self.player_ref(player),
                    'position': position_ref(player),
                    'teammate': team_id == event['team']['id'],
                }
            )
        return frame

    def tactics(self, team_id):
        lineup = [
            {
                'player': self.player_ref(player),
                'position': position_ref(player),
                'jersey_number': player['jersey_number'],
            }
            for player in self.world.starters(team_id)
        ]
        return {'formation': FORMATION, 'lineup': lineup}

    def defensive_responsibility(self, team_id):
        players = []
        for player in self.rng.sample(self.world.starters(other_team(team_id)), 3):
            pressure, interception = self.number(0, 0.5), self.number(0, 0.5)
            players.append(
                {
                    'player_id': player['id'],
                    'probability': round(pressure + interception, 6),
                    'pressure_like_probability': pressure,
                    'interception_like_probability': interception,
                }
            )
        players.sort(key=lambda player: -player['probability'])
        categories = self.rng.choice(
            [['pressure_like'], ['interception_like'], ['pressure_like', 'interception_like']]
        )
        return {
            'categories': categories,
            'probability_sum': round(sum(player['probability'] for player in players), 6),
            'players': players,
        }

    def attributes(self, fields, event):
        attributes = {}
        for key, field in fields.items():
            field = field if isinstance(field, Field) else Field(field)
            if field.since <= self.version <= field.until:
                attributes[key] = field.make(self, event)
        return attributes

    def event(self, type_name, team_id, period, seconds):
        """Make an event of a type, in the key order StatsBomb uses."""
        type_id, key, fields = EVENT_TYPES[type_name]
        match_seconds = seconds + (period - 1) * HALF_LENGTH
        # one event per type, team and period, so the id is stable across data versions
        event_id = uuid.uuid5(uuid.NAMESPACE_URL, f'{MATCH_ID}/{type_name}/{team_id}/{period}')
        event = {
            'id': str(event_id),
            'index': None,
            'period': period,
            'timestamp': clock(seconds, short=False),
            'minute': int(match_seconds) // 60,
            'second': int(match_seconds) % 60,
            'type': {'id': type_id, 'name': type_name},
            'possession': None,
            'possession_team': team_ref(team_id),
            'play_pattern': self.pick(PLAY_PATTERNS),
        }
        has_location = type_name not in NO_LOCATION
        if self.version >= 8:
            for name in OBV:
                event[name] = self.number(-0.05, 0.05) if has_location else None
        event['team'] = team_ref(team_id)
        if type_name not in NO_PLAYER:
            if type_name == 'Substitution':
                player = self.world.starters(team_id)[-1]
            else:
                player = self.rng.choice(self.world.starters(team_id))
            event['player'] = self.player_ref(player)
            event['position'] = position_ref(player)
        if has_location:
            # a shot's height at impact was added in data version 5, i.e. from events v8
            shot_height = type_name == 'Shot' and self.version >= 8
            event['location'] = self.location(3 if shot_height else 2)
        event['duration'] = 0.0 if type_name in STRUCTURAL else self.number(0, 3)
        for flag, flagged in FLAGS.items():
            if type_name == flagged:
                event[flag] = True
        if self.version >= 11:
            event['attacking_direction'] = self.rng.choice(['left_to_right', 'right_to_left'])
            if has_location:
                event.update(
                    control_degree=self.number(0, 1),
                    is_transition=self.rng.random() < 0.5,
                    is_controlled_possession=self.rng.random() < 0.5,
                    possession_directness=self.number(0, 1),
                    player_possession_directness=self.number(-2, 2),
                    location_bucket=['defensive_third', 'midfield_third', 'final_third'][
                        int(event['location'][0] // 40)
                    ],
                )
            if type_name in ATTACKING:
                event['defensive_responsibility'] = self.defensive_responsibility(team_id)
            if type_name == 'Pass':
                event['set_piece_phase'] = self.rng.choice([1, 2])
        if type_name not in NO_PLAYER or type_name in {'Half Start', 'Half End'}:
            event['related_events'] = []
        if type_name in TACTICS:
            event['tactics'] = self.tactics(team_id)
        attributes = self.attributes(fields, event)
        if key and attributes:
            event[key] = attributes
        return event

    def match(self):
        """Make the events of a match, in order, with their links filled in."""
        periods = {1: [], 2: []}
        for type_name in sorted(set(EVENT_TYPES) - STRUCTURAL):
            team_id = HOME if type_name == 'Substitution' else self.rng.choice([HOME, AWAY])
            period = self.rng.choice([1, 2])
            periods[period].append((round(self.rng.uniform(1, HALF_LENGTH), 3), type_name, team_id))

        events = [self.event('Starting XI', team_id, 1, 0.0) for team_id in (HOME, AWAY)]
        for period, timed in periods.items():
            events += [self.event('Half Start', team_id, period, 0.0) for team_id in (HOME, AWAY)]
            for seconds, type_name, team_id in sorted(timed):
                events.append(self.event(type_name, team_id, period, seconds))
            end = round(HALF_LENGTH + self.rng.uniform(60, 300), 3)
            events += [self.event('Half End', team_id, period, end) for team_id in (HOME, AWAY)]

        ids = {event['type']['name']: event['id'] for event in events}
        for index, event in enumerate(events, start=1):
            event['index'] = index
            event['possession'] = index // 3 + 1
            key = EVENT_TYPES[event['type']['name']][1]
            for name, value in event.get(key, {}).items():
                if isinstance(value, Link):
                    event[key][name] = ids[value.type_name]

        by_type = {event['type']['name']: event for event in events}
        pairs = [(by_type[source], by_type[target], both) for source, target, both in RELATED]
        # each half's start and end are linked to the other team's
        for name in ('Half Start', 'Half End'):
            for period in (1, 2):
                home, away = [
                    e for e in events if e['type']['name'] == name and e['period'] == period
                ]
                pairs.append((home, away, True))
        for source, target, both_ways in pairs:
            source['related_events'].append(target['id'])
            if both_ways:
                target['related_events'].append(source['id'])
        for event in events:
            # the key is left out, rather than empty, when an event has no links
            if event.get('related_events') == []:
                del event['related_events']
        return events


def match_clock(event):
    """An event's time on the match clock, in seconds."""
    return event['minute'] * 60 + event['second'] + float(event['timestamp'][-4:])


def make_lineups(world, version, events):
    """The lineups, which describe the events at the latest events version."""
    rng = random.Random(f'lineups{version}')
    substitution = next(event for event in events if event['type']['name'] == 'Substitution')
    off, on = substitution['player'], substitution['substitution']['replacement']
    sub_time, sub_period = match_clock(substitution), substitution['period']

    def spell(
        position, start, end, start_reason, end_reason, from_period, to_period, counterpart=None
    ):
        spell = {
            'position_id': position[0],
            'position': position[1],
            'from': clock(start, short=False),
            'to': None if end is None else clock(end, short=False),
            'from_period': from_period,
            'to_period': to_period,
            'start_reason': start_reason,
            'end_reason': end_reason,
        }
        if counterpart:
            spell.update(counterpart_id=counterpart['id'], counterpart_name=counterpart['name'])
        return spell

    teams = []
    for team_id in (HOME, AWAY):
        lineup = []
        for player in world.players[team_id]:
            record = {
                'player_id': player['id'],
                'player_name': player['name'],
                'player_nickname': player['nickname'],
            }
            if version >= 4:
                record.update(
                    birth_date=player['birth_date'],
                    player_gender='male',
                    player_height=player['height'],
                    player_weight=player['weight'],
                )
            record.update(jersey_number=player['jersey_number'], country=COUNTRY)
            if version < 4:
                lineup.append(record)
                continue
            if player['id'] == off['id']:
                positions = [
                    spell(
                        player['position'],
                        0,
                        sub_time,
                        'Starting XI',
                        'Substitution - Off (Tactical)',
                        1,
                        sub_period,
                        on,
                    )
                ]
            elif player['id'] == on['id']:
                positions = [
                    spell(
                        world.starters(team_id)[-1]['position'],
                        sub_time,
                        None,
                        'Substitution - On (Tactical)',
                        'Final Whistle',
                        sub_period,
                        None,
                        off,
                    )
                ]
            elif player['position']:
                positions = [
                    spell(player['position'], 0, None, 'Starting XI', 'Final Whistle', 1, None)
                ]
            else:
                positions = []
            record['positions'] = positions
            # stats and skills are an empty list rather than an object for an unused substitute
            played = bool(positions)
            record['stats'] = (
                {
                    'own_goals': 0,
                    'goals': rng.randint(0, 1),
                    'assists': rng.randint(0, 1),
                    'penalties_scored': 0,
                    'penalties_missed': 0,
                    'penalties_saved': 0,
                }
                if played
                else []
            )
            if version >= 5:
                rating, raw_rating = player['hops']
                record['skills'] = (
                    {'HOPS': {'rating': rating, 'raw_rating': raw_rating}} if played else []
                )
            lineup.append(record)

        team = {'team_id': team_id, 'team_name': TEAMS[team_id], 'lineup': lineup}
        if version >= 4:
            team_events = [event for event in events if event['team']['id'] == team_id]
            team['formations'] = [
                {
                    'period': event['period'],
                    'timestamp': clock(match_clock(event), short=False),
                    'reason': event['type']['name'],
                    'formation': event['tactics']['formation'],
                }
                for event in team_events
                if event['type']['name'] in TACTICS
            ]
            team['events'] = []
            for event in team_events:
                name = event['type']['name']
                entry = {
                    'period': event['period'],
                    'timestamp': clock(match_clock(event), short=False),
                    'type': name,
                }
                if name in ('Half Start', 'Half End'):
                    team['events'].append(entry)
                elif name in ('Foul Committed', 'Bad Behaviour', 'Shot'):
                    outcome = (
                        event['shot']['outcome']
                        if name == 'Shot'
                        else event[EVENT_TYPES[name][1]]['card']
                    )
                    team['events'].append(
                        {
                            'player_id': event['player']['id'],
                            'player_name': event['player']['name'],
                            **entry,
                            'outcome': outcome['name'],
                        }
                    )
        teams.append(team)
    return teams


def make_threesixty(version, events):
    """A 360 frame for each event with a location."""
    rng = random.Random(f'threesixty{version}')
    frames = []
    for event in events:
        if 'location' not in event:
            continue
        players = [
            {'teammate': True, 'actor': True, 'keeper': False, 'location': event['location'][:2]}
        ]
        for _ in range(rng.randint(6, 14)):
            players.append(
                {
                    'teammate': rng.random() < 0.5,
                    'actor': False,
                    'keeper': rng.random() < 0.1,
                    'location': [round(rng.uniform(0, 120), 6), round(rng.uniform(0, 80), 6)],
                }
            )
        # a closed loop of x, y pairs around the visible area
        area = [round(rng.uniform(0, 120 if i % 2 == 0 else 80), 6) for i in range(10)]
        frame = {
            'event_uuid': event['id'],
            'visible_area': area + area[:2],
            'freeze_frame': players,
        }
        if version >= 2:
            teammates = sum(player['teammate'] for player in players)
            frame.update(
                line_breaking_pass=rng.choice([True, False])
                if event['type']['name'] == 'Pass'
                else None,
                num_defenders_on_goal_side_of_actor=rng.randint(0, 10),
                distance_to_nearest_defender=round(rng.uniform(0, 15), 6),
                ball_receipt_in_space=rng.choice([True, False])
                if event['type']['name'] == 'Ball Receipt*'
                else None,
                ball_receipt_exceeds_distance=rng.choice([2, 5, 10])
                if event['type']['name'] == 'Ball Receipt*'
                else None,
                visible_player_counts=[
                    {'team_id': event['team']['id'], 'count': teammates},
                    {'team_id': other_team(event['team']['id']), 'count': len(players) - teammates},
                ],
                distances_from_edge_of_visible_area=[
                    {'point_id': point_id, 'distance': round(rng.uniform(0, 20), 6)}
                    for point_id in range(1, len(players) + 1)
                ],
            )
        frames.append(frame)
    return frames


def make_matches(version):
    rng = random.Random(f'matches{version}')
    matches = []
    for week, ((match_id, home, away), (last_updated, last_updated_360)) in enumerate(
        zip(MATCHES, MATCH_UPDATED, strict=True), start=1
    ):
        match = {
            'match_id': match_id,
            'match_date': f'2025-08-{week + 10}',
            'kick_off': '15:00:00.000',
            'competition': COMPETITION,
            'season': SEASON,
        }
        for side, team_id in [('home', home), ('away', away)]:
            team = {
                f'{side}_team_id': team_id,
                f'{side}_team_name': TEAMS[team_id],
                f'{side}_team_gender': 'male',
            }
            if version >= 6:
                team[f'{side}_team_youth'] = False
            manager = {
                'id': team_id,
                'name': f'{TEAMS[team_id].split()[0]} Manager',
                'nickname': 'Boss' if team_id % 2 else None,
                'dob': '1975-01-01',
                'country': COUNTRY,
            }
            group = 'Group A' if team_id in (101, 102) else 'Group B'
            team.update({f'{side}_team_group': group, 'country': COUNTRY, 'managers': [manager]})
            match[f'{side}_team'] = team
        match.update(home_score=rng.randint(0, 4), away_score=rng.randint(0, 4))
        if version >= 6:
            match.update(
                attendance=rng.randint(5000, 40000),
                behind_closed_doors=False,
                neutral_ground=False,
                collection_status='Complete',
                play_status='Normal',
            )
        match.update(
            match_status='available',
            match_status_360='available' if last_updated_360 else 'unscheduled',
            last_updated=last_updated,
            last_updated_360=last_updated_360,
            metadata={
                'data_version': '1.1.0',
                'shot_fidelity_version': '2',
                'xy_fidelity_version': '2',
            },
            match_week=week,
            competition_stage={'id': 1, 'name': 'Regular Season'},
            stadium={'id': home, 'name': f'{TEAMS[home].split()[0]} Park', 'country': COUNTRY},
            referee={'id': week, 'name': f'Referee {week}', 'country': COUNTRY},
        )
        matches.append(match)
    return matches


def make_competitions(version):
    """A row per season, the second with no 360 data."""
    seasons = [
        (SEASON, MICROSECONDS, MILLISECONDS, MICROSECONDS, MICROSECONDS),
        ({'season_id': 2, 'season_name': '2024/2025'}, MICROSECONDS, None, MICROSECONDS, None),
    ]
    return [
        {
            'competition_id': COMPETITION['competition_id'],
            'season_id': season['season_id'],
            'country_name': COMPETITION['country_name'],
            'competition_name': COMPETITION['competition_name'],
            'competition_gender': 'male',
            'competition_youth': False,
            'competition_international': False,
            'season_name': season['season_name'],
            'match_updated': updated,
            'match_updated_360': updated_360,
            'match_available_360': available_360,
            'match_available': available,
        }
        for season, updated, updated_360, available, available_360 in seasons
    ]


def main():
    shutil.rmtree(OUT_DIR, ignore_errors=True)
    world = World()
    for version in EVENTS_VERSIONS:
        events = EventMaker(world, version).match()
        write(events, 'events', f'v{version}', f'{MATCH_ID}.json')
    # the lineups and 360 frames describe the events at the latest version
    for version in LINEUP_VERSIONS:
        write(make_lineups(world, version, events), 'lineups', f'v{version}', f'{MATCH_ID}.json')
    for version in THREESIXTY_VERSIONS:
        write(make_threesixty(version, events), 'threesixty', f'v{version}', f'{MATCH_ID}.json')
    for version in MATCHES_VERSIONS:
        write(make_matches(version), 'matches', f'v{version}', 'matches.json')
    for version in COMPETITIONS_VERSIONS:
        write(make_competitions(version), 'competitions', f'v{version}', 'competitions.json')


if __name__ == '__main__':
    main()
