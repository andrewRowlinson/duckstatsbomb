=============
duckstatsbomb
=============

Get flat tables from Hudl StatsBomb data, in multiple formats, in seconds once cached.

DuckDB does the parsing in parallel. You can parse a whole Premier League season of
1.3 million events from JSON into pandas, Polars or Arrow tables in a few seconds.
Or use the default output, a DuckDB relation, and filter to the shots and sum the xG
before building a DataFrame, so you can query the whole open-data set on your laptop.

------------
Installation
------------

The duckstatsbomb default has one dependency, DuckDB.
You can install optional dependencies to export to pandas, Polars, or Arrow:

.. code-block:: bash

    pip install duckstatsbomb
    pip install "duckstatsbomb[pandas]"  # pandas
    pip install "duckstatsbomb[polars]"  # Polars & PyArrow
    pip install "duckstatsbomb[arrow]"  # PyArrow
    pip install "duckstatsbomb[all]"  # pandas, Polars & PyArrow

-----------
Quick start
-----------

By default the library outputs a DuckDBPyRelation,
which you can filter, aggregate, or query.

.. code-block:: python

    from duckstatsbomb import Sbopen
    parser = Sbopen()
    events = parser.competition_data(competition_id=43, season_id=106)
    shots = events.filter("type_name = 'Shot'")
    # top 4 goal scorers at the 2022 World Cup
    top = (
        shots.aggregate(
            'player_name, team_name, count(*) as shots, '
            'round(sum(shot_statsbomb_xg), 2) as xg, '
            "count(*) filter (outcome_name = 'Goal') as goals"
        )
        .order('goals desc, xg desc')
        .limit(4)
    )
    top.show()

You can also export to different formats:

.. code-block:: python

    df_pandas = shots.df()  # pip install "duckstatsbomb[pandas]"
    df_polars = shots.pl()  # pip install "duckstatsbomb[polars]"
    arrow_table = shots.to_arrow_table()  # pip install "duckstatsbomb[arrow]"
    shots.to_csv('world_cup_2022_shots.csv')
    shots.to_parquet('world_cup_2022_shots.parquet')

Or set the output format when creating the parser:

.. code-block:: python

    parser = Sbopen(output_format='pandas')  # 'relation', 'pandas', 'polars' or 'arrow'

-------------
Three parsers
-------------

There is one parser for each place the data comes from:

- :doc:`gallery/open_data` with :class:`~duckstatsbomb.Sbopen`, for the free
  `StatsBomb open-data <https://github.com/statsbomb/open-data>`_
- :doc:`gallery/api` with :class:`~duckstatsbomb.Sbapi`, which needs a Hudl StatsBomb
  API subscription
- :doc:`gallery/files` with :class:`~duckstatsbomb.Sbfiles`, for JSON files you
  already have, on disk or in an object store

-------------
Want to help?
-------------

Take a look at the
`open issues <https://github.com/andrewRowlinson/duckstatsbomb/issues>`_
for inspiration. Please get in touch at rowlinsonandy@gmail.com to find out more.

-------
License
-------
`MIT <https://choosealicense.com/licenses/mit/>`_

The StatsBomb open-data has its own
`licence <https://github.com/statsbomb/open-data/blob/master/LICENSE.pdf>`_,
which is for non-commercial use.

.. toctree::
   :hidden:
   :caption: Contents

   Quick start <self>
   gallery/open_data
   gallery/api
   gallery/files

.. toctree::
   :hidden:
   :caption: Reference

   duckstatsbomb.parser
   duckstatsbomb.cache
