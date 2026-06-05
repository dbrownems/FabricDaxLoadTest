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
from .env import (
    LakehouseInfo,
    DatasetInfo,
    discover_lakehouse,
    resolve_workspace,
    resolve_target_dataset,
    find_dotnet,
    stage_loadgen_zip,
    find_bundled_wheel,
)
from .runner import RunConfig, RunResult, run_load_test, render_progress
from .persist import WriteSummary, write_run
from . import notebook

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
    "LakehouseInfo",
    "DatasetInfo",
    "discover_lakehouse",
    "resolve_workspace",
    "resolve_target_dataset",
    "find_dotnet",
    "stage_loadgen_zip",
    "find_bundled_wheel",
    "RunConfig",
    "RunResult",
    "run_load_test",
    "render_progress",
    "WriteSummary",
    "write_run",
    "notebook",
]
