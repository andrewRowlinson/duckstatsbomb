"""pytest fixtures and helpers for the duckstatsbomb tests.

The parsers use the default output_format, a duckdb relation, so the suite needs
nothing but duckdb. test_output_formats covers the other formats.
"""

import importlib.util
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))

from dummy_api import DummyStatsBombAPI  # noqa: E402

# The test data is laid out as data/{kind}/v{version}/, holding {match_id}.json for the
# match level data (events, lineups, threesixty) and matches.json or competitions.json
# for the competition level data. They are made up by make_test_data.py, as the real
# StatsBomb data is not redistributable.
DATA_DIR = Path(__file__).parent / 'data'

# the one match with events, lineups and 360 data, in a competition of four matches
MATCH_ID = 1001
COMPETITION_ID = 1
SEASON_ID = 1
N_MATCHES = 4


def data_path(kind, version, match_id=None):
    """The path of a test data file.

    Parameters
    ----------
    kind : str
        The data directory: 'events', 'lineups', 'threesixty', 'matches' or
        'competitions'.
    version : int
        The StatsBomb data version the file was downloaded at.
    match_id : int, default None
        The match, for the match level data. The competition level files are
        named after their kind, e.g. matches/v6/matches.json.

    Returns
    -------
    pathlib.Path
    """
    name = f'{match_id}.json' if match_id is not None else f'{kind}.json'
    return DATA_DIR / kind / f'v{version}' / name


# the formats whose library is installed, so the suite runs on a minimal install
INSTALLED_FORMATS = ['relation'] + [
    fmt
    for fmt, package in [('pandas', 'pandas'), ('polars', 'polars'), ('arrow', 'pyarrow')]
    if importlib.util.find_spec(package) is not None
]


def row_count(relation):
    """Return the number of rows in a duckdb relation."""
    return relation.shape[0]


def column(relation, name):
    """Return one column of a duckdb relation as a list.

    Parameters
    ----------
    relation : duckdb.DuckDBPyRelation
    name : str
        The column name.
    """
    return [row[0] for row in relation.select(name).fetchall()]


def match_ids(relation):
    """Return the sorted unique match_id values of a duckdb relation."""
    return sorted(set(column(relation, 'match_id')))


@pytest.fixture(scope='session')
def api():
    """A dummy StatsBomb API serving the test data."""
    with DummyStatsBombAPI(DATA_DIR) as server:
        yield server


@pytest.fixture(scope='session')
def multi_match_api():
    """A dummy API that serves its test data under any match id.

    The test data covers a single match, so this stands in for a subscription with
    several matches when testing reads that span more than one.
    """
    with DummyStatsBombAPI(DATA_DIR, alias_match_ids=True, max_matches=3) as server:
        yield server


@pytest.fixture
def parser(api):
    """An Sbapi parser pointed at the dummy API, with caching off."""
    return _parser(api)


@pytest.fixture
def multi_match_parser(multi_match_api):
    """An Sbapi parser pointed at the dummy API that serves several matches."""
    return _parser(multi_match_api)


def _parser(server, **kwargs):
    """Build an Sbapi parser for a dummy API server.

    Parameters
    ----------
    server : DummyStatsBombAPI
    **kwargs
        Passed on to Sbapi, e.g. output_format.
    """
    from duckstatsbomb import Sbapi

    return Sbapi(
        sb_username=server.username,
        sb_password=server.password,
        url=server.url,
        cache_enabled=False,
        **kwargs,
    )


@pytest.fixture(params=INSTALLED_FORMATS)
def format_parser(api, request):
    """An Sbapi parser for each output format whose library is installed."""
    return _parser(api, output_format=request.param)
