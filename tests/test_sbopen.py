"""Tests for Sbopen, which reads the StatsBomb open-data over the network.

The tests that fetch data are marked network. The rest only construct the parser.
"""

import duckdb
import pytest

from conftest import column, match_ids, row_count
from duckstatsbomb import Sbopen

# a Euro 2020 match, which has 360 data in the open-data
MATCH_ID = 3788741
COMPETITION_ID = 55
SEASON_ID = 43

OPEN_DATA_KINDS = [
    'lineup_players',
    'events',
    'frames',
    'tactics',
    'related_events',
    'threesixty_frames',
    'threesixty',
]


@pytest.fixture(scope='module')
def parser(tmp_path_factory):
    return Sbopen(cache_path=str(tmp_path_factory.mktemp('cache') / 'cache'))


def test_kinds_map_to_the_open_data_directories(parser):
    """Each kind names the open-data directory its file is read from."""
    directories = {'events': 'events', 'lineups': 'lineups', 'threesixty': 'three-sixty'}
    for kind, source in parser.kinds.items():
        assert parser.url_map[kind].endswith('/' + directories[source])


def test_versions_are_fixed(parser):
    """The open-data is published in one shape, so the data versions are fixed."""
    assert (
        parser.competitions_version,
        parser.matches_version,
        parser.events_version,
        parser.lineup_version,
        parser.threesixty_version,
    ) == (4, 3, 4, 2, 1)


def test_invalid_kind(parser):
    with pytest.raises(ValueError, match='kind should be one of'):
        parser.match_data(MATCH_ID, kind='lineup_positions')


@pytest.mark.network
@pytest.mark.parametrize('kind', OPEN_DATA_KINDS)
def test_match_data(parser, kind):
    df = parser.match_data(MATCH_ID, kind=kind)
    assert row_count(df) > 0
    assert match_ids(df) == [MATCH_ID]


@pytest.mark.network
def test_competitions(parser):
    df = parser.competitions()
    assert row_count(df) > 0
    assert (COMPETITION_ID, SEASON_ID) in set(
        zip(column(df, 'competition_id'), column(df, 'season_id'), strict=True)
    )


@pytest.mark.network
def test_matches(parser):
    df = parser.matches(COMPETITION_ID, SEASON_ID)
    assert MATCH_ID in column(df, 'match_id')


@pytest.mark.network
def test_a_missing_match_is_a_plain_404():
    """Only Sbapi explains errors, so the open-data does not talk about credentials."""
    with pytest.raises(duckdb.HTTPException) as raised:
        Sbopen(cache_enabled=False).match_data(1, kind='events').fetchall()
    assert raised.value.status_code == 404
    assert not getattr(raised.value, '__notes__', [])
