import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.abspath("../../src"))

project = "trackinsight-data-python"
copyright = f"{datetime.now().year}, trackinsight-data-python"
author = "trackinsight-data-python"
release = "0.1.0"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx_autodoc_typehints",
]
autodoc_member_order = "bysource"

templates_path = ["_templates"]
exclude_patterns = []

html_theme = "furo"
html_static_path = ["_static"]
