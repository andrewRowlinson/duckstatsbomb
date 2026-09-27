"""Tests for the output_format argument.

The default, 'relation', returns an unmaterialised duckdb relation and needs nothing
but duckdb. The other formats each need the library that provides them.
"""

import importlib.util
import re
import tomllib
from pathlib import Path

import pytest

from conftest import MATCH_ID, _parser
from duckstatsbomb import Sbfiles
from duckstatsbomb.parser import OUTPUT_FORMATS


def test_every_parser_defaults_to_a_relation(parser):
    """The default is the same for every parser, so none of them needs pandas."""
    assert Sbfiles().output_format == 'relation'
    assert parser.output_format == 'relation'


def test_format_returns_the_right_type(format_parser):
    """Each output format returns the type it says it does, holding the data."""
    expected = {
        'relation': 'DuckDBPyRelation',
        'pandas': 'DataFrame',
        'polars': 'DataFrame',
        'arrow': 'Table',
    }[format_parser.output_format]
    result = format_parser.match_data(MATCH_ID, kind='tactics')
    assert type(result).__name__ == expected
    # duckdb queries a relation, dataframe or table in a local variable by name
    ids = format_parser.con.sql('select distinct match_id from result').fetchall()
    assert ids == [(MATCH_ID,)]


def test_an_unknown_format_raises(api):
    with pytest.raises(ValueError, match='supported output_formats'):
        _parser(api, output_format='spark')


def test_extras_cover_what_each_format_needs():
    """Every package a format needs is in the extra of the same name in pyproject."""
    pyproject = Path(__file__).parent.parent / 'pyproject.toml'
    extras = tomllib.loads(pyproject.read_text())['project']['optional-dependencies']
    for name, output_format in OUTPUT_FORMATS.items():
        if not output_format.packages:
            continue
        declared = {re.split(r'[<>=! ]', item)[0] for item in extras[name]}
        assert set(output_format.packages) <= declared, (
            f'duckstatsbomb[{name}] is missing {set(output_format.packages) - declared}'
        )


@pytest.mark.parametrize('missing', ['polars', 'pyarrow'])
def test_a_missing_library_raises_at_construction(api, monkeypatch, missing):
    """The error names the extra to install, rather than failing part way through a read.

    duckdb converts to polars through arrow, so polars alone is not enough.
    """
    real_find_spec = importlib.util.find_spec

    def find_spec(name, *args, **kwargs):
        if name == missing:
            return None
        return real_find_spec(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, 'find_spec', find_spec)
    with pytest.raises(ImportError, match=rf'needs {missing}.*duckstatsbomb\[polars\]'):
        _parser(api, output_format='polars')
