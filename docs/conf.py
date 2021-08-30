# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
# import os
# import sys
# sys.path.insert(0, os.path.abspath('.'))
import os
import shutil
from pathlib import Path

from recommonmark.transform import AutoStructify

from generate_schema_csv import generate_csv, generate_latest_files

# -- Project information -----------------------------------------------------

project = "Oaebu Workflows"
copyright = "2020-2021 Curtin University"
author = "Curtin University"

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "pbr.sphinxext",
    "sphinx_rtd_theme",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "autoapi.extension",
    "recommonmark",
]

# Auto API settings: https://github.com/readthedocs/sphinx-autoapi
autoapi_type = "python"
autoapi_dirs = ["../oaebu_workflows"]
autoapi_add_toctree_entry = True
autoapi_python_use_implicit_namespaces = True

# Add any paths that contain templates here, relative to this directory.
templates_path = ["templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "sphinx_rtd_theme"
html_logo = "logo.png"
html_theme_options = {
    "logo_only": True,
    "display_version": False,
}

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []


# recommonmark config, used to enable rst to be evaluated within markdown files
def setup(app):
    app.add_config_value("recommonmark_config", {"enable_eval_rst": True, "auto_toc_tree_section": "Contents"}, True)
    app.add_transform(AutoStructify)


generate_csv(schema_dir="../oaebu_workflows/database/schema")
generate_latest_files()
html_build_dir = "_build/html"
Path(html_build_dir).mkdir(exist_ok=True, parents=True)
