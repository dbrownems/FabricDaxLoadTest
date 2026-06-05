"""Runtime helpers for the FabricDaxLoadTest LoadGen notebook.

Importable from the LoadTest notebook after cell 2 unzips and
pip-installs the wheel bundled in `Files/loadgen-bin.zip`. This
module is the supported boundary between the notebook and LoadGen —
notebook code should call into here rather than re-implementing the
helpers inline.
"""

from .queries import (
    load_queries,
    load_users,
    normalize_queries,
    normalize_users,
)
from .analyze import plot_run

try:
    from ._version import version as __version__
except ImportError:  # pragma: no cover — only when built without setuptools-scm
    __version__ = "0.0.0+unknown"

__all__ = [
    "__version__",
    "load_queries",
    "load_users",
    "normalize_queries",
    "normalize_users",
    "plot_run",
]
