"""Tests for Sbapi, run against a dummy StatsBomb API rather than the real one."""

import base64
import datetime
import json
import shutil
import time
from pathlib import Path

import duckdb
import pytest

from conftest import (
    COMPETITION_ID,
    DATA_DIR,
    MATCH_ID,
    N_MATCHES,
    SEASON_ID,
    column,
    data_path,
    match_ids,
    row_count,
)
from duckstatsbomb import Sbapi
from duckstatsbomb.parser import KINDS
from dummy_api import DummyStatsBombAPI


@pytest.mark.parametrize(
    ('versions', 'missing'),
    [
        ({}, []),  # the defaults (events v11, lineups v5, 360 v2) carry every kind
        ({'events_version': 8}, ['defensive_responsibility']),
        ({'lineup_version': 2}, ['lineup_events', 'lineup_formations', 'lineup_positions']),
        ({'threesixty_version': 1}, ['threesixty_visible_count', 'threesixty_visible_distance']),
    ],
)
def test_kinds_are_gated_on_the_version_of_their_source(api, versions, missing):
    """A kind is offered when the version of the file it is parsed from carries it."""
    parser = Sbapi(
        sb_username=api.username,
        sb_password=api.password,
        url=api.url,
        cache_enabled=False,
        **versions,
    )
    assert sorted(parser.kinds) == sorted(set(KINDS) - set(missing))


def test_kinds_name_their_source_file(parser):
    """Each kind maps to the file it is parsed from, which is the file its url points at."""
    slugs = {'events': '/v11/events', 'lineups': '/v5/lineups', 'threesixty': '/v2/360-frames'}
    for kind, source in parser.kinds.items():
        assert parser.url_map[kind].endswith(slugs[source])


@pytest.mark.parametrize('kind', ['tactics', 'lineup_formations', 'threesixty_visible_count'])
def test_match_data(parser, kind):
    """A kind from each source file is read from its url and tagged with the match.

    The SQL for every kind is covered by test_sbfiles, and the urls only differ by source.
    """
    df = parser.match_data(MATCH_ID, kind=kind)
    assert row_count(df) > 0
    assert match_ids(df) == [MATCH_ID]


def test_competitions(parser):
    df = parser.competitions()
    seasons = list(zip(column(df, 'competition_id'), column(df, 'season_id'), strict=True))
    assert (COMPETITION_ID, SEASON_ID) in seasons


def test_matches(parser):
    df = parser.matches(COMPETITION_ID, SEASON_ID)
    assert row_count(df) == N_MATCHES
    assert MATCH_ID in column(df, 'match_id')


def test_multiple_matches_are_labelled_and_read_once(multi_match_parser):
    """Reading several matches at once labels each row with its own match_id, and a match
    listed twice is read once, as read_json returns a file's rows as many times as it is listed."""
    matches = column(multi_match_parser.matches(COMPETITION_ID, SEASON_ID), 'match_id')
    once = multi_match_parser.match_data(matches, kind='lineup_formations')
    assert match_ids(once) == sorted(matches)
    repeated = multi_match_parser.match_data(matches + matches[:1], kind='lineup_formations')
    assert row_count(repeated) == row_count(once)
    assert match_ids(repeated) == sorted(matches)


def test_competition_data_over_paired_lists(multi_match_parser, multi_match_api):
    """competition_data resolves the match ids of every season listed, then reads them all.

    Lists are paired up in order, as in matches.
    """
    matches = column(multi_match_parser.matches(COMPETITION_ID, SEASON_ID), 'match_id')
    before = len(multi_match_api.requests)
    df = multi_match_parser.competition_data(
        [COMPETITION_ID, 11], [SEASON_ID, 90], kind='lineup_formations'
    )
    requested = [path for path, _ in multi_match_api.requests[before:]]
    assert f'/api/v6/competitions/{COMPETITION_ID}/seasons/{SEASON_ID}/matches' in requested
    assert '/api/v6/competitions/11/seasons/90/matches' in requested
    # the second season is not in the test data, so it adds no matches
    assert match_ids(df) == sorted(matches)


def test_sends_basic_auth(parser, api):
    """The credentials go out as an HTTP basic Authorization header."""
    before = len(api.requests)
    parser.match_data(MATCH_ID, kind='tactics').fetchall()  # the relation is lazy
    token = base64.b64encode(f'{api.username}:{api.password}'.encode()).decode()
    requests = api.requests[before:]
    paths = [path for path, _ in requests]
    authorizations = {auth for _, auth in requests}
    assert f'/api/v11/events/{MATCH_ID}' in paths
    assert authorizations == {f'Basic {token}'}


def test_a_direct_read_is_lazy(api):
    """Without the cache a relation is a query that has not run yet, so nothing is
    requested until it is used, and a rejection surfaces then as duckdb's own error."""
    parser = Sbapi(sb_username='wrong', sb_password='wrong', url=api.url, cache_enabled=False)
    requests_before = len(api.requests)
    relation = parser.match_data(MATCH_ID, kind='events')
    assert len(api.requests) == requests_before
    with pytest.raises(duckdb.HTTPException) as raised:
        relation.fetchall()
    assert raised.value.status_code == 0


def test_bad_credentials_are_explained_for_a_dataframe(api):
    """duckdb reports a rejected request as HTTP 0, so the error says what that means.

    A dataframe is materialised straight away, so the note is added on a direct read too.
    """
    pytest.importorskip('pandas')
    parser = Sbapi(
        sb_username='wrong',
        sb_password='wrong',
        url=api.url,
        cache_enabled=False,
        output_format='pandas',
    )
    with pytest.raises(duckdb.HTTPException) as raised:
        parser.match_data(MATCH_ID, kind='events')
    assert raised.value.status_code == 0
    assert any('username or password' in note for note in raised.value.__notes__)


def test_other_http_errors_get_no_note(caching_parser):
    """A 404 is already clear, so it is left alone.

    Downloading into the cache raises straight away, so no dataframe library is needed.
    """
    with pytest.raises(duckdb.HTTPException) as raised:
        caching_parser.match_data(1, kind='events')
    assert raised.value.status_code == 404
    assert not getattr(raised.value, '__notes__', [])


def test_parsers_do_not_share_credentials(api):
    """Each parser gets its own database, so a second one cannot re-authenticate the first.

    On duckdb's shared ':default:' connection the second parser's create or replace
    secret silently overwrote the first's, and the first could no longer read.
    """
    first = Sbapi(
        sb_username=api.username,
        sb_password=api.password,
        url=api.url,
        cache_enabled=False,
    )
    Sbapi(sb_username='someone', sb_password='else', url=api.url, cache_enabled=False)
    assert row_count(first.match_data(MATCH_ID, kind='tactics')) > 0


def test_connection_kws_work_with_the_default_database(api):
    """The default database accepts connection options, which ':default:' refused."""
    parser = Sbapi(
        sb_username=api.username,
        sb_password=api.password,
        url=api.url,
        cache_enabled=False,
        connection_kws={'config': {'threads': 1}},
    )
    assert parser.con.execute("select current_setting('threads')").fetchone()[0] == 1


def test_missing_credentials_raise(api, monkeypatch):
    monkeypatch.delenv('SB_USERNAME', raising=False)
    monkeypatch.delenv('SB_PASSWORD', raising=False)
    with pytest.raises(ValueError, match='credentials are required'):
        Sbapi(url=api.url, cache_enabled=False)


def test_credential_arguments_beat_the_environment(api, monkeypatch):
    monkeypatch.setenv('SB_USERNAME', 'wrong')
    monkeypatch.setenv('SB_PASSWORD', 'wrong')
    parser = Sbapi(
        sb_username=api.username,
        sb_password=api.password,
        url=api.url,
        cache_enabled=False,
    )
    assert row_count(parser.match_data(MATCH_ID, kind='tactics')) > 0


def test_credentials_come_from_the_environment(api, monkeypatch):
    monkeypatch.setenv('SB_USERNAME', api.username)
    monkeypatch.setenv('SB_PASSWORD', api.password)
    parser = Sbapi(url=api.url, cache_enabled=False)
    assert row_count(parser.match_data(MATCH_ID, kind='tactics')) > 0


def test_sources(parser):
    assert parser.sources == ['events', 'lineups', 'threesixty']


def test_invalid_kind(parser):
    with pytest.raises(ValueError, match='kind should be one of'):
        parser.match_data(MATCH_ID, kind='not_a_kind')


SEASON_METHODS = ['matches', 'competition_data', 'stale_matches']


@pytest.mark.parametrize('method', SEASON_METHODS)
@pytest.mark.parametrize(
    ('competition_id', 'season_id'), [(COMPETITION_ID, [SEASON_ID]), ([COMPETITION_ID], SEASON_ID)]
)
def test_ids_are_both_lists_or_neither(parser, method, competition_id, season_id):
    with pytest.raises(ValueError, match='should both be lists, or both single ids'):
        getattr(parser, method)(competition_id, season_id)


@pytest.mark.parametrize('method', SEASON_METHODS)
def test_id_lists_must_be_the_same_length(parser, method):
    with pytest.raises(ValueError, match='should be the same length'):
        getattr(parser, method)([COMPETITION_ID, COMPETITION_ID], [SEASON_ID])


def test_match_data_with_no_match_ids(parser):
    with pytest.raises(ValueError, match='match_id is empty'):
        parser.match_data([], kind='events')


def test_supported_versions_match_the_sql_directories():
    """SUPPORTED_VERSIONS is written by hand, so check it against the vN directories.

    Adding a version means adding a directory of SQL and adding the number here. This
    fails if either is done without the other.
    """
    from duckstatsbomb.parser import SUPPORTED_VERSIONS

    directories = {
        'competitions_version': 'competitions',
        'matches_version': 'matches',
        'events_version': 'events',
        'lineup_version': 'lineups',
        'threesixty_version': 'threesixty',
    }
    assert set(directories) == set(SUPPORTED_VERSIONS)
    sql = Path(__file__).parent.parent / 'duckstatsbomb' / 'sql'
    for parameter, directory in directories.items():
        on_disk = []
        for path in (sql / directory).iterdir():
            version = path.name.removeprefix('v')
            if path.is_dir() and version.isdecimal():
                on_disk.append(int(version))
        assert SUPPORTED_VERSIONS[parameter] == sorted(on_disk), parameter


@pytest.mark.parametrize(
    'kwargs',
    [
        {'events_version': 5},
        {'lineup_version': 3},
        {'threesixty_version': 3},
        {'matches_version': 4},
        {'competitions_version': 5},
    ],
)
def test_unsupported_versions_raise(api, kwargs):
    with pytest.raises(ValueError, match='Invalid argument'):
        Sbapi(
            sb_username=api.username,
            sb_password=api.password,
            url=api.url,
            cache_enabled=False,
            **kwargs,
        )


@pytest.fixture
def cache_dir(tmp_path):
    return tmp_path / 'cache'


@pytest.fixture
def caching_parser(api, cache_dir):
    """An Sbapi parser that caches into a fresh directory."""
    return Sbapi(
        sb_username=api.username,
        sb_password=api.password,
        url=api.url,
        cache_path=str(cache_dir),
    )


def test_cache_files_mirror_the_api_paths(caching_parser, cache_dir):
    """A file is cached at its url path below the base url, as JSON."""
    caching_parser.match_data(MATCH_ID, kind='events')
    caching_parser.match_data(MATCH_ID, kind='lineup_players')
    assert sorted(path.relative_to(cache_dir).as_posix() for path in cache_dir.rglob('*')) == [
        'v11',
        'v11/events',
        f'v11/events/{MATCH_ID}.json',
        'v5',
        'v5/lineups',
        f'v5/lineups/{MATCH_ID}.json',
    ]
    assert not list(cache_dir.rglob('*.part'))


def test_cache_is_the_raw_file(caching_parser, cache_dir):
    """The cached file is byte for byte what the API served, so anything can read it."""
    caching_parser.match_data(MATCH_ID, kind='tactics')
    cached = (cache_dir / 'v11' / 'events' / f'{MATCH_ID}.json').read_bytes()
    assert cached == data_path('events', 11, MATCH_ID).read_bytes()


def test_relative_cache_path_survives_a_change_of_directory(api, tmp_path, monkeypatch):
    """The relation is lazy, so it is read after the working directory may have changed."""
    monkeypatch.chdir(tmp_path)
    parser = Sbapi(
        sb_username=api.username,
        sb_password=api.password,
        url=api.url,
        cache_path='cache',
    )
    events = parser.match_data(MATCH_ID, kind='events')
    monkeypatch.chdir(tmp_path.parent)
    assert match_ids(events) == [MATCH_ID]


def test_kinds_sharing_a_file_share_the_cache(caching_parser, api):
    """events, frames, tactics and related_events all read the events file."""
    caching_parser.match_data(MATCH_ID, kind='events')
    before = len(api.requests)
    for kind in ['frames', 'tactics', 'related_events']:
        caching_parser.match_data(MATCH_ID, kind=kind)
    assert len(api.requests) == before


def test_only_the_missing_files_are_downloaded(multi_match_api, tmp_path):
    parser = Sbapi(
        sb_username=multi_match_api.username,
        sb_password=multi_match_api.password,
        url=multi_match_api.url,
        cache_path=str(tmp_path / 'cache'),
    )
    matches = column(parser.matches(COMPETITION_ID, SEASON_ID), 'match_id')
    parser.match_data(matches[0], kind='lineup_formations')
    before = len(multi_match_api.requests)
    df = parser.match_data(matches, kind='lineup_formations')
    assert match_ids(df) == sorted(matches)
    requested = [path for path, _ in multi_match_api.requests[before:]]
    assert f'/api/v5/lineups/{matches[0]}' not in requested
    assert all(f'/api/v5/lineups/{match}' in requested for match in matches[1:])


def test_clear_match_data_deletes_the_file(caching_parser, cache_dir, api):
    """The file is deleted and the next read downloads it again. A match that is not
    cached is ignored."""
    caching_parser.match_data(MATCH_ID, kind='events')
    caching_parser.clear_match_data([MATCH_ID, 1], source='events')
    assert not (cache_dir / 'v11' / 'events' / f'{MATCH_ID}.json').exists()
    before = len(api.requests)
    caching_parser.match_data(MATCH_ID, kind='events')
    assert len(api.requests) > before


def test_clear_match_data_deletes_only_its_source(caching_parser, cache_dir):
    caching_parser.match_data(MATCH_ID, kind='events')
    caching_parser.match_data(MATCH_ID, kind='lineup_players')
    caching_parser.clear_match_data(MATCH_ID, source='lineups')
    assert not (cache_dir / 'v5' / 'lineups' / f'{MATCH_ID}.json').exists()
    assert (cache_dir / 'v11' / 'events' / f'{MATCH_ID}.json').exists()


def test_clear_cache_removes_the_directory(caching_parser, cache_dir):
    caching_parser.match_data(MATCH_ID, kind='events')
    caching_parser.clear_cache()
    assert not cache_dir.exists()
    caching_parser.clear_cache()  # clearing an absent cache is fine


def test_cached_files(caching_parser, cache_dir):
    assert row_count(caching_parser.cached_files()) == 0
    caching_parser.match_data(MATCH_ID, kind='events')
    files = caching_parser.cached_files()
    assert files.columns == ['path', 'size', 'downloaded_at']
    path, size, downloaded_at = files.fetchone()
    cached = cache_dir / 'v11' / 'events' / f'{MATCH_ID}.json'
    assert Path(path) == cached
    assert size == cached.stat().st_size
    utc_now = datetime.datetime.now(datetime.UTC).replace(tzinfo=None)
    assert abs((utc_now - downloaded_at).total_seconds()) < 60


def test_files_are_downloaded_in_parallel(tmp_path, monkeypatch):
    """The cache downloads files at once, as read_json does over a list of urls.

    read_blob, the obvious way to download raw bytes, reads a list of files one
    after another, so this guards the choice of read_json_objects. With every
    response slowed down, a serial download would take at least the delay times
    the number of requests.
    """
    delay, n_matches = 0.3, 6
    payload = DummyStatsBombAPI._payload

    def slow_payload(self, path):
        time.sleep(delay)
        return payload(self, path)

    monkeypatch.setattr(DummyStatsBombAPI, '_payload', slow_payload)
    with DummyStatsBombAPI(DATA_DIR, alias_match_ids=True) as slow_api:
        parser = Sbapi(
            sb_username=slow_api.username,
            sb_password=slow_api.password,
            url=slow_api.url,
            cache_path=str(tmp_path / 'cache'),
            duckdb_threads=n_matches,
        )
        started = time.time()
        df = parser.match_data(list(range(1, n_matches + 1)), kind='lineup_formations')
        elapsed = time.time() - started
        assert match_ids(df) == list(range(1, n_matches + 1))
        serial = delay * len(slow_api.requests)
        assert elapsed < serial / 2, f'{elapsed:.2f}s, serial would be {serial:.2f}s'


def test_files_are_written_in_batches_of_one_per_thread(tmp_path, monkeypatch):
    """A batch is written before the next is downloaded, so memory is bounded.

    DuckDB reads every file in a query before returning a row, so downloading a
    season in one query would hold every file in memory and write nothing until
    the last download finished.
    """
    delay, n_matches, threads = 0.1, 8, 2
    payload = DummyStatsBombAPI._payload
    request_times = []

    def slow_payload(self, path):
        time.sleep(delay)
        request_times.append(time.time())
        return payload(self, path)

    monkeypatch.setattr(DummyStatsBombAPI, '_payload', slow_payload)
    with DummyStatsBombAPI(DATA_DIR, alias_match_ids=True) as slow_api:
        parser = Sbapi(
            sb_username=slow_api.username,
            sb_password=slow_api.password,
            url=slow_api.url,
            cache_path=str(tmp_path / 'cache'),
            duckdb_threads=threads,
        )
        assert parser.duckdb_threads == threads
        write_times = []
        write = parser.cache.write

        def timed_write(key, content):
            write_times.append(time.time())
            write(key, content)

        monkeypatch.setattr(parser.cache, 'write', timed_write)
        df = parser.match_data(list(range(1, n_matches + 1)), kind='lineup_formations')
        assert match_ids(df) == list(range(1, n_matches + 1))
        # all but the last batch are written before the last download finishes
        written_early = sum(written < max(request_times) for written in write_times)
        assert written_early == n_matches - threads


def test_the_cache_does_not_hide_a_rejected_request(api, tmp_path):
    """Downloading into the cache explains a rejection the same way as a direct read."""
    parser = Sbapi(
        sb_username='wrong', sb_password='wrong', url=api.url, cache_path=str(tmp_path / 'cache')
    )
    with pytest.raises(duckdb.HTTPException) as raised:
        parser.match_data(MATCH_ID, kind='events')
    assert raised.value.status_code == 0
    assert any('username or password' in note for note in raised.value.__notes__)
    assert not list((tmp_path / 'cache').rglob('*.json'))


def test_an_interrupted_write_is_not_cached(caching_parser, cache_dir, api, monkeypatch):
    """A file is written under a .part name and renamed once complete, so a write that
    fails part way through is not served from the cache: the next read downloads it again."""
    write_bytes = Path.write_bytes

    def interrupted_write_bytes(self, data):
        write_bytes(self, data[: len(data) // 2])
        raise OSError('interrupted')

    monkeypatch.setattr(Path, 'write_bytes', interrupted_write_bytes)
    with pytest.raises(OSError, match='interrupted'):
        caching_parser.match_data(MATCH_ID, kind='events')
    cached = cache_dir / 'v11' / 'events' / f'{MATCH_ID}.json'
    assert not cached.exists()

    monkeypatch.undo()
    before = len(api.requests)
    assert match_ids(caching_parser.match_data(MATCH_ID, kind='events')) == [MATCH_ID]
    assert len(api.requests) > before
    assert cached.exists()
    assert not list(cache_dir.rglob('*.part'))


def test_a_file_over_duckdbs_default_object_size_is_cached(tmp_path):
    """DuckDB reads JSON objects up to 16MB by default, and the cache downloads a whole
    file as one object. Some 360 files are larger."""
    served = tmp_path / 'served'
    shutil.copytree(DATA_DIR, served)
    path = served / 'threesixty' / 'v2' / f'{MATCH_ID}.json'
    frames = json.loads(path.read_text())
    frames *= 17 * 2**20 // len(json.dumps(frames)) + 1
    path.write_text(json.dumps(frames))
    assert path.stat().st_size > 16 * 2**20
    with DummyStatsBombAPI(served) as big_api:
        parser = Sbapi(
            sb_username=big_api.username,
            sb_password=big_api.password,
            url=big_api.url,
            cache_path=str(tmp_path / 'cache'),
        )
        assert row_count(parser.match_data(MATCH_ID, kind='threesixty')) == len(frames)
