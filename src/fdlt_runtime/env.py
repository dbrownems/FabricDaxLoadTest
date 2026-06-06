"""Fabric environment discovery + LoadGen staging.

Pulled out of notebook cell 2. The notebook keeps the small amount of
glue that needs `notebookutils` (which is only available inside Fabric
Spark kernels); everything else lives here so it can be unit-tested and
upgraded as part of the wheel.
"""
from __future__ import annotations

import os
import re
import shutil
import subprocess
import zipfile
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

import requests

FABRIC_API = "https://api.fabric.microsoft.com/v1"

_DOTNET_CANDIDATES = (
    "DOTNET_HOST_PATH",  # explicit override
    "/home/trusted-service-user/cluster-env/trident_env/bin/dotnet",  # Fabric Spark
    "_which_dotnet",     # sentinel for shutil.which("dotnet")
    "/usr/bin/dotnet",
    "/usr/local/bin/dotnet",
)


@dataclass
class LakehouseInfo:
    """The handful of identifiers cell-2 used to compute by hand."""

    workspace_id: str
    workspace_name: str
    lakehouse_id: str
    lakehouse_name: str
    abfss: str
    schema: str  # "" for flat lakehouses, "dbo" (or other) for schema-enabled
    sql_endpoint_id: Optional[str] = None  # parent SQL analytics endpoint, when known

    @property
    def table_base(self) -> str:
        return f"{self.abfss}/Tables" + (f"/{self.schema}" if self.schema else "")


def discover_lakehouse(
    workspace_id: str,
    lakehouse_name: str,
    token: str,
    *,
    workspace_name: Optional[str] = None,
    schema_override: Optional[str] = None,
    list_tables: Optional[Callable[[str], Iterable[object]]] = None,
) -> LakehouseInfo:
    """Resolve `lakehouse_name` in `workspace_id` and detect schema layout.

    `schema_override` follows cell-1 LAKEHOUSE_SCHEMA semantics:
      * None — auto-detect via Fabric items API + Tables/dbo probe
      * ""   — force flat (Tables/)
      * other — force that schema (Tables/<schema>/)

    `list_tables(abfss_path)` is the optional `notebookutils.fs.ls` shim
    used for the dbo probe; when omitted the probe is skipped.
    """
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/items?type=Lakehouse",
        headers=headers, timeout=30)
    r.raise_for_status()
    matches = [i for i in r.json().get("value", []) if i["displayName"] == lakehouse_name]
    if not matches:
        raise RuntimeError(
            f"Lakehouse '{lakehouse_name}' not found in workspace {workspace_id}")
    lh_id = matches[0]["id"]
    abfss = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lh_id}"

    if schema_override is None:
        schema = ""
        sql_endpoint_id: Optional[str] = None
        try:
            lh = requests.get(
                f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lh_id}",
                headers=headers, timeout=30).json()
            props = lh.get("properties") or {}
            schema = props.get("defaultSchema") or ""
            sep = props.get("sqlEndpointProperties") or {}
            sql_endpoint_id = sep.get("id")
        except Exception:
            schema = ""
        if not schema and list_tables is not None:
            try:
                entries = list_tables(f"{abfss}/Tables")
                if any(getattr(e, "name", "").rstrip("/") == "dbo" for e in entries):
                    schema = "dbo"
            except Exception:
                pass
    else:
        schema = schema_override
        sql_endpoint_id = None
        try:
            lh = requests.get(
                f"{FABRIC_API}/workspaces/{workspace_id}/lakehouses/{lh_id}",
                headers=headers, timeout=30).json()
            sql_endpoint_id = ((lh.get("properties") or {})
                               .get("sqlEndpointProperties") or {}).get("id")
        except Exception:
            pass

    return LakehouseInfo(
        workspace_id=workspace_id,
        workspace_name=workspace_name or workspace_id,
        lakehouse_id=lh_id,
        lakehouse_name=lakehouse_name,
        abfss=abfss,
        schema=schema,
        sql_endpoint_id=sql_endpoint_id,
    )


def refresh_sql_endpoint_metadata(
    workspace_id: str,
    sql_endpoint_id: str,
    token: str,
    *,
    timeout: int = 60,
) -> dict:
    """Force the lakehouse SQL analytics endpoint to refresh its catalog.

    Without this call, newly written/altered Delta tables become visible
    to T-SQL/PBI only after the background metadata sync catches up
    (anywhere from seconds to minutes). Calling this after every notebook
    write means the SQL endpoint and Direct Lake report see the new data
    immediately.

    Best-effort: raises only on 4xx/5xx HTTP errors that aren't 202
    (Accepted = LRO started, also success). Caller should wrap in try.

    Docs: https://learn.microsoft.com/rest/api/fabric/sqlendpoint/items/refresh-sql-endpoint-metadata
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    url = (f"{FABRIC_API}/workspaces/{workspace_id}/sqlEndpoints/"
           f"{sql_endpoint_id}/refreshMetadata")
    r = requests.post(url, headers=headers, json={}, timeout=timeout)
    # 200 = synchronous completion; 202 = LRO accepted (we don't poll, the
    # background process finishes within seconds for our table count)
    if r.status_code in (200, 202):
        try:
            return {"status_code": r.status_code, "body": r.json()}
        except Exception:
            return {"status_code": r.status_code, "body": None}
    r.raise_for_status()
    return {"status_code": r.status_code, "body": None}


@dataclass
class DatasetInfo:
    """Resolved (workspace, semantic-model) pair for the XMLA endpoint."""

    workspace_id: str
    workspace_name: str
    dataset_id: str
    dataset_name: str


_GUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE)


def resolve_workspace(
    workspace: str,
    token: str,
) -> tuple[str, str]:
    """Return `(workspace_id, workspace_name)` for a name or GUID.

    Accepts either the workspace display name or its GUID. Useful when
    the notebook user passes `TARGET_WORKSPACE = "Sales BI"` and we
    need an id to call the items API.
    """
    headers = {"Authorization": f"Bearer {token}"}
    if _GUID_RE.match(workspace):
        r = requests.get(
            f"{FABRIC_API}/workspaces/{workspace}",
            headers=headers, timeout=30)
        r.raise_for_status()
        ws = r.json()
        return ws["id"], ws.get("displayName") or workspace
    # Name lookup — list and filter (Fabric has no name-based GET).
    r = requests.get(f"{FABRIC_API}/workspaces", headers=headers, timeout=30)
    r.raise_for_status()
    matches = [w for w in r.json().get("value", [])
               if w.get("displayName") == workspace]
    if not matches:
        raise RuntimeError(f"Workspace '{workspace}' not found.")
    if len(matches) > 1:
        ids = ", ".join(w["id"] for w in matches)
        raise RuntimeError(
            f"Workspace name '{workspace}' is ambiguous ({len(matches)} "
            f"matches): {ids}. Use the GUID instead.")
    return matches[0]["id"], matches[0]["displayName"]


def resolve_target_dataset(
    workspace_id: str,
    token: str,
    *,
    workspace_name: Optional[str] = None,
    dataset_name: Optional[str] = None,
) -> DatasetInfo:
    """Resolve the semantic model under test in `workspace_id`.

    Rules (matches notebook cell-1 contract):
      * `dataset_name` given → look it up by display name (exact match);
        error if not found.
      * `dataset_name` None and the workspace contains exactly one
        SemanticModel → return that one.
      * `dataset_name` None and the workspace contains zero → error.
      * `dataset_name` None and >1 → error listing the candidates so
        the user can pick.
    """
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/items?type=SemanticModel",
        headers=headers, timeout=30)
    r.raise_for_status()
    models = r.json().get("value", []) or []

    if dataset_name:
        m = [x for x in models if x.get("displayName") == dataset_name]
        if not m:
            names = ", ".join(sorted(x.get("displayName", "?") for x in models)) or "(none)"
            raise RuntimeError(
                f"Semantic model '{dataset_name}' not found in workspace "
                f"{workspace_name or workspace_id}. Available: {names}")
        chosen = m[0]
    else:
        if len(models) == 0:
            raise RuntimeError(
                f"No semantic models in workspace {workspace_name or workspace_id}. "
                "Create one, or set TARGET_DATASET to a model in another workspace "
                "(and set TARGET_WORKSPACE accordingly).")
        if len(models) > 1:
            names = ", ".join(sorted(x.get("displayName", "?") for x in models))
            raise RuntimeError(
                f"Workspace {workspace_name or workspace_id} contains {len(models)} "
                f"semantic models; set TARGET_DATASET to one of: {names}")
        chosen = models[0]

    return DatasetInfo(
        workspace_id=workspace_id,
        workspace_name=workspace_name or workspace_id,
        dataset_id=chosen["id"],
        dataset_name=chosen["displayName"],
    )


def find_dotnet() -> str:
    """Locate the .NET 8 host on the current kernel."""
    probed = []
    for cand in _DOTNET_CANDIDATES:
        if cand == "_which_dotnet":
            p = shutil.which("dotnet")
        elif cand == "DOTNET_HOST_PATH":
            p = os.environ.get("DOTNET_HOST_PATH")
        else:
            p = cand
        if p:
            probed.append(p)
            if os.path.exists(p):
                # Sanity-probe the runtime so we fail fast on a broken host.
                info = subprocess.run([p, "--info"],
                                      capture_output=True, text=True, timeout=10)
                if info.returncode == 0:
                    return p
                raise RuntimeError(f"`{p} --info` failed:\n{info.stderr}")
    raise RuntimeError(
        "Could not find a working `dotnet` runtime on this kernel. "
        "LoadGen.dll is a framework-dependent .NET 8 build and needs the "
        f"runtime to be installed. Probed: {probed}")


def stage_loadgen_zip(local_zip: str, stage_dir: str) -> str:
    """Extract `local_zip` into `stage_dir` and return the LoadGen.dll path."""
    shutil.rmtree(stage_dir, ignore_errors=True)
    os.makedirs(stage_dir, exist_ok=True)
    with zipfile.ZipFile(local_zip, "r") as zf:
        zf.extractall(stage_dir)
    dll = os.path.join(stage_dir, "LoadGen.dll")
    if not os.path.exists(dll):
        raise FileNotFoundError(
            f"LoadGen.dll not found under {stage_dir} after extracting {local_zip}. "
            "(Legacy zip-based path. As of v0.5.0 LoadGen.dll ships inside the "
            "fdlt_runtime wheel; re-run scripts/Deploy-LoadTests.ps1 to deploy "
            "the wheel-based bootstrap.)")
    return dll


def find_bundled_wheel(stage_dir: str) -> Optional[str]:
    """Return the path of the fdlt_runtime wheel inside `stage_dir`, or None."""
    cands = sorted(
        os.path.join(stage_dir, f) for f in os.listdir(stage_dir)
        if f.startswith("fdlt_runtime-") and f.endswith(".whl"))
    return cands[-1] if cands else None
