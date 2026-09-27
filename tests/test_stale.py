"""Tests for finding out of date match data.

The matches file is never cached, so its last_updated is always current, and the
cache records when each match file was downloaded. stale_matches compares the two.
"""

import datetime
import json
import shutil

import pytest

from conftest import COMPETITION_ID, DATA_DIR, SEASON_ID
from duckstatsbomb import Sbapi
from dummy_api import DummyStatsBombAPI


@pytest.fixture
def served(tmp_path):
    """A copy of the test data the test can edit, standing in for StatsBomb reprocessing."""
    copy = tmp_path / 'served'
    shutil.copytree(DATA_DIR, copy)
    return copy


@pytest.fixture
def api(served):
    with DummyStatsBombAPI(served, alias_match_ids=True, max_matches=3) as server:
        yield server


@pytest.fixture
def parser(api, tmp_path):
    return Sbapi(
        sb_username=api.username,
        sb_password=api.password,
        url=api.url,
        cache_path=str(tmp_path / 'cache'),
    )


@pytest.fixture
def match_ids(parser):
    return sorted(parser._match_ids(COMPETITION_ID, SEASON_ID))


def reprocess(served, match_id, field='last_updated'):
    """Move a match's last_updated to now, as if StatsBomb had just reprocessed it.

    last_updated and the cache's download time both keep microseconds, so no wait is needed.
    """
    path = served / 'matches' / 'v6' / 'matches.json'
    matches = json.loads(path.read_text())
    now = datetime.datetime.now(datetime.UTC)
    for match in matches:
        if match['match_id'] == match_id:
            match[field] = now.strftime('%Y-%m-%dT%H:%M:%S.%f')
    path.write_text(json.dumps(matches))


def test_the_matches_index_is_never_cached(parser, api):
    for _ in range(2):
        before = len(api.requests)
        parser.matches(COMPETITION_ID, SEASON_ID).fetchall()
        assert len(api.requests) > before


def test_everything_is_stale_before_anything_is_cached(parser, match_ids):
    assert parser.stale_matches(COMPETITION_ID, SEASON_ID) == match_ids


def test_stale_matches_over_paired_lists(parser, match_ids):
    """The second season is not in the test data, so it adds no matches."""
    assert parser.stale_matches([COMPETITION_ID, 11], [SEASON_ID, 90]) == match_ids


def test_competition_data_for_a_season_with_no_matches(parser):
    with pytest.raises(ValueError, match=f'no matches found for competition_id {COMPETITION_ID}'):
        parser.competition_data(COMPETITION_ID, 90, kind='events')


def test_a_match_missing_from_the_cache_is_stale(parser, match_ids):
    parser.competition_data(COMPETITION_ID, SEASON_ID, kind='events')
    parser.clear_match_data(match_ids[0], source='events')
    assert parser.stale_matches(COMPETITION_ID, SEASON_ID) == [match_ids[0]]


def test_each_source_file_is_cached_separately(parser, match_ids):
    """Reading the events caches the file tactics is parsed from, but not the lineups."""
    parser.competition_data(COMPETITION_ID, SEASON_ID, kind='tactics')
    assert parser.stale_matches(COMPETITION_ID, SEASON_ID, source='events') == []
    assert parser.stale_matches(COMPETITION_ID, SEASON_ID, source='lineups') == match_ids
    parser.competition_data(COMPETITION_ID, SEASON_ID, kind='lineup_players')
    assert parser.stale_matches(COMPETITION_ID, SEASON_ID, source='lineups') == []


def test_threesixty_uses_its_own_timestamp(parser, served, match_ids):
    """Reprocessing the events does not make the 360 data stale, and vice versa."""
    parser.competition_data(COMPETITION_ID, SEASON_ID, kind='events')
    parser.competition_data(COMPETITION_ID, SEASON_ID, kind='threesixty')
    reprocess(served, match_ids[1], field='last_updated')
    assert parser.stale_matches(COMPETITION_ID, SEASON_ID, source='threesixty') == []
    reprocess(served, match_ids[2], field='last_updated_360')
    assert parser.stale_matches(COMPETITION_ID, SEASON_ID, source='threesixty') == [match_ids[2]]
    assert parser.stale_matches(COMPETITION_ID, SEASON_ID, source='events') == [match_ids[1]]


def test_refreshing_clears_the_stale_matches(parser, served, match_ids):
    """Nothing is stale straight after loading, a reprocessed match is, and reading it
    again clears it."""
    parser.competition_data(COMPETITION_ID, SEASON_ID, kind='events')
    assert parser.stale_matches(COMPETITION_ID, SEASON_ID) == []
    reprocess(served, match_ids[1])
    stale = parser.stale_matches(COMPETITION_ID, SEASON_ID)
    assert stale == [match_ids[1]]

    parser.clear_match_data(stale, source='events')
    parser.match_data(stale, kind='events').fetchall()
    assert parser.stale_matches(COMPETITION_ID, SEASON_ID) == []


def test_everything_is_stale_when_the_cache_is_disabled(api, tmp_path, match_ids):
    parser = Sbapi(
        sb_username=api.username,
        sb_password=api.password,
        url=api.url,
        cache_enabled=False,
        cache_path=str(tmp_path / 'cache'),
    )
    parser.competition_data(COMPETITION_ID, SEASON_ID, kind='events').fetchall()
    assert parser.stale_matches(COMPETITION_ID, SEASON_ID) == match_ids


@pytest.mark.parametrize('source', ['not_a_source', 'tactics'])
def test_invalid_source(parser, source):
    """A kind is not a source, even one parsed from the events file."""
    with pytest.raises(ValueError, match='source should be one of'):
        parser.stale_matches(COMPETITION_ID, SEASON_ID, source=source)
    with pytest.raises(ValueError, match='source should be one of'):
        parser.clear_match_data(1, source=source)
