"""Tests for Sbfiles, which reads StatsBomb JSON files you already have."""

import datetime
import os
import shutil
from pathlib import Path

import pytest

from conftest import MATCH_ID, N_MATCHES, column, data_path, match_ids, row_count
from duckstatsbomb import Sbfiles
from duckstatsbomb.parser import KINDS, SUPPORTED_VERSIONS

VERSION_ARGUMENTS = {
    'events': 'events_version',
    'lineups': 'lineup_version',
    'threesixty': 'threesixty_version',
}

# every data type at every supported version, so a version without a data file fails
DATA_FILES = [
    (kind, spec.source, version)
    for kind, spec in KINDS.items()
    for version in SUPPORTED_VERSIONS[VERSION_ARGUMENTS[spec.source]]
    if version >= spec.min_version
]


def empty_columns(relation):
    """The columns with no value in any row, i.e. the ones the test data does not exercise."""
    return [
        name for name in relation.columns if relation.filter(f'"{name}" is not null').shape[0] == 0
    ]


def events_file():
    """The events file at the API default version, and the Sbfiles arguments to read it.

    Returns
    -------
    tuple of (str, dict)
    """
    return str(data_path('events', 8, MATCH_ID)), {'events_version': 8}


@pytest.mark.parametrize(('kind', 'source', 'version'), DATA_FILES)
def test_match_data(kind, source, version):
    """Every column has a value somewhere, so a change to any of them is tested.

    A column added to the SQL fails here until make_test_data.py fills it in.
    """
    parser = Sbfiles(**{VERSION_ARGUMENTS[source]: version})
    df = parser.match_data(str(data_path(source, version, MATCH_ID)), kind=kind)
    assert match_ids(df) == [MATCH_ID]
    assert empty_columns(df) == []


def test_the_defaults_are_the_latest_versions():
    """Adding a version moves the default, so a new version needs no arguments."""
    parser = Sbfiles()
    for parameter, supported in SUPPORTED_VERSIONS.items():
        assert getattr(parser, parameter) == max(supported), parameter


@pytest.mark.parametrize('version', SUPPORTED_VERSIONS['matches_version'])
def test_matches(version):
    df = Sbfiles(matches_version=version).matches(str(data_path('matches', version)))
    assert row_count(df) == N_MATCHES
    assert MATCH_ID in column(df, 'match_id')
    assert empty_columns(df) == []


@pytest.mark.parametrize('version', SUPPORTED_VERSIONS['matches_version'])
def test_every_update_time_format_is_read(version):
    """StatsBomb sends update times to the microsecond, millisecond or minute, or null.

    stale_matches compares them with the cache's download times, so the fraction of a
    second has to survive.
    """
    df = Sbfiles(matches_version=version).matches(str(data_path('matches', version)))
    microseconds = datetime.datetime(2025, 6, 1, 12, 0, 0, 123456)
    milliseconds = datetime.datetime(2025, 6, 1, 12, 0, 0, 123000)
    minutes = datetime.datetime(2025, 6, 1, 12, 0)
    assert df.order('match_id').select('last_updated, last_updated_360').fetchall() == [
        (microseconds, milliseconds),
        (milliseconds, minutes),
        (minutes, microseconds),
        (microseconds, None),
    ]


@pytest.mark.parametrize('version', SUPPORTED_VERSIONS['competitions_version'])
def test_competitions(version):
    df = Sbfiles(competitions_version=version).competitions(str(data_path('competitions', version)))
    assert row_count(df) > 0
    assert empty_columns(df) == []


def test_a_repeated_file_is_read_once():
    """read_json returns a file's rows as many times as it is listed, so paths are deduplicated."""
    path, versions = events_file()
    parser = Sbfiles(**versions)
    once = parser.match_data(path, kind='events')
    repeated = parser.match_data([path, path], kind='events')
    assert row_count(repeated) == row_count(once)


def test_a_path_can_be_a_pathlib_path():
    path, versions = events_file()
    df = Sbfiles(**versions).match_data(Path(path), kind='events')
    assert match_ids(df) == [MATCH_ID]


def test_a_glob_reads_every_matching_file(tmp_path):
    """A glob is passed through to duckdb, which resolves it."""
    path, versions = events_file()
    for match_id in [1, 2]:
        shutil.copy(path, tmp_path / f'{match_id}.json')
    df = Sbfiles(**versions).match_data(str(tmp_path / '*.json'), kind='events')
    assert match_ids(df) == [1, 2]


def test_paths_are_quoted_for_sql(tmp_path):
    """The paths go into the SQL as literals, so a quote in a directory name must be escaped."""
    source, versions = events_file()
    directory = tmp_path / "andy's data"
    directory.mkdir()
    path = directory / Path(source).name
    shutil.copy(source, path)
    df = Sbfiles(**versions).match_data([str(path)], kind='events')
    assert row_count(df) > 0


def test_match_data_with_no_files():
    _, versions = events_file()
    with pytest.raises(ValueError, match='filename is empty'):
        Sbfiles(**versions).match_data([], kind='events')


@pytest.mark.skipif(os.name == 'nt', reason='a backslash already separates directories')
def test_match_id_comes_from_a_windows_path(tmp_path):
    """Windows separates directories with a backslash. Elsewhere a backslash can be part
    of a file name, so 'events\\1001.json' stands in for a Windows path."""
    source, versions = events_file()
    path = tmp_path / 'events\\1001.json'
    shutil.copy(source, path)
    df = Sbfiles(**versions).match_data(str(path), kind='events')
    assert match_ids(df) == [MATCH_ID]


@pytest.mark.parametrize('kind', ['events', 'related_events'])
def test_the_asterisk_is_dropped_from_ball_receipt(kind):
    """StatsBomb names the event type 'Ball Receipt*'."""
    path, versions = events_file()
    df = Sbfiles(**versions).match_data(path, kind=kind)
    names = set(column(df, 'type_name'))
    assert 'Ball Receipt' in names
    assert not any('*' in name for name in names)


@pytest.mark.parametrize('version', SUPPORTED_VERSIONS['events_version'])
def test_related_events_go_both_ways(version):
    """Each link comes out both ways, once.

    StatsBomb records most links both ways, but some only one way: a carry lists the
    pass before it, but the pass does not list the carry.
    """
    parser = Sbfiles(events_version=version)
    df = parser.match_data(str(data_path('events', version, MATCH_ID)), kind='related_events')
    links = df.select('event_uuid, event_uuid_related').fetchall()
    assert len(links) == len(set(links))
    assert set(links) == {(related, event) for event, related in links}
    assert ('Pass', 'Carry') in df.select('type_name, type_name_related').fetchall()
