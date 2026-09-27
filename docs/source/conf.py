# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import duckstatsbomb

# -- Project information -----------------------------------------------------

project = 'duckstatsbomb'
copyright = '2026, Andrew Rowlinson'
author = 'Andrew Rowlinson'

# The full version, including alpha/beta/rc tags
VERSION = duckstatsbomb.__version__
release = VERSION


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.viewcode',
    'sphinx_gallery.gen_gallery',
    'sphinx.ext.autosectionlabel',
    'sphinx.ext.intersphinx',
    'numpydoc',
]

# https://github.com/readthedocs/readthedocs.org/issues/2569
master_doc = 'index'

# this is needed for some reason...
# see https://github.com/numpy/numpydoc/issues/69
numpydoc_class_members_toctree = False
# link the parameter types, e.g. duckstatsbomb.cache.CacheBase. Napoleon must not be
# loaded as well, as it rewrites the Parameters sections before numpydoc sees them.
numpydoc_xref_param_type = True
numpydoc_xref_ignore = {'of', 'or', 'default', 'optional', 'depending', 'on', 'output_format'}
# numpydoc links bool, iterable etc. to the Python docs
intersphinx_mapping = {'python': ('https://docs.python.org/3', None)}

# sphinx-gallery generates several pages with the same section names
autosectionlabel_prefix_document = True

# generate autosummary even if no references
autosummary_generate = True
# order api docs by order they appear in the code
autodoc_member_order = 'bysource'

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ['_build']

# sphinx gallery
# one page per parser, linked from the index toctree, so the gallery index is orphaned.
# filename_pattern only decides which examples are run: the api page is still shown,
# but it is not run as it needs a subscription.
sphinx_gallery_conf = {
    'examples_dirs': ['../../examples'],
    'gallery_dirs': ['gallery'],
    'filename_pattern': r'[\\/](open_data|files)\.py$',
    'image_scrapers': (),
    'reset_modules': (),
}

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = 'sphinx_rtd_theme'

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []

html_theme_options = {}
