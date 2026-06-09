"""Query and user scenario loading.

Resolution order for a given filename:

  1. ``"abfss://..."`` — literal lakehouse URL (read via the caller-
     supplied ``read_abfss`` callback, or ``open()`` if it happens to
     resolve as a regular path).
  2. ``"name.json"`` — load ``builtin/name.json`` from the kernel CWD
     (the Fabric notebook Resources panel is mounted there).
  3. ``None`` — auto-discover: if exactly one ``*.json`` file is present
     under ``builtin/``, use it. Auto-discovery applies to **queries
     only**; users must always be named explicitly.
  4. Nothing matches — caller falls back to its inline default.
"""

from __future__ import annotations

import json
import os
from typing import Any, Callable, Iterable

ReadAbfss = Callable[[str], str]


def _read_text(path: str, read_abfss: ReadAbfss | None) -> str:
    if path.startswith("abfss://"):
        if read_abfss is None:
            raise RuntimeError(
                "abfss:// path supplied but no `read_abfss` reader provided. "
                "Pass notebookutils.fs.head (or a wrapper) into load_queries / load_users."
            )
        return read_abfss(path)
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def normalize_queries(raw_json: str) -> list[str]:
    """Parse a queries JSON blob into a flat list of DAX strings.

    Accepted shapes:

      * Power BI Desktop *Performance Analyzer* export
        (``{"version": ..., "events": [...]}``). DAX text is pulled from
        ``event.metrics.QueryText`` for events whose ``name`` is
        ``"Execute DAX Query"`` (the actual DSE-issued DAX). Older /
        alternative shapes (``event.query`` or ``event.Query.Query``)
        are also accepted.
      * Object array: ``[{"query": "..."}, ...]`` or
        ``[{"Query": "..."}, ...]``
      * String array: ``["EVALUATE ...", ...]``

    Tolerates a leading UTF-8 BOM (Power BI Desktop writes one).
    """
    obj = json.loads(raw_json.lstrip("\ufeff"))
    if isinstance(obj, dict) and isinstance(obj.get("events"), list):
        events = obj["events"]
        out: list[str] = []
        query_named_events = 0
        for ev in events:
            if not isinstance(ev, dict):
                continue
            if ev.get("name") in ("Execute DAX Query", "Query"):
                query_named_events += 1
            # Power BI Desktop Performance Analyzer: DAX lives in
            # metrics.QueryText on "Execute DAX Query" events.
            q = None
            metrics = ev.get("metrics")
            if isinstance(metrics, dict):
                q = metrics.get("QueryText") or metrics.get("queryText")
            if not q:
                q = ev.get("query") or ev.get("QueryText") or ev.get("queryText")
            if not q:
                qd = ev.get("Query") or {}
                if isinstance(qd, dict):
                    q = qd.get("Query")
            if isinstance(q, str) and q.strip():
                out.append(q)
        if not out:
            raise ValueError(
                "Performance Analyzer export contains no DAX query text "
                f"({len(events)} events, {query_named_events} Query/Execute DAX Query "
                "events, but none had metrics.QueryText). Re-record the trace from "
                "Power BI Desktop (View > Performance Analyzer > Start recording > "
                "Refresh visuals > Export), or in the Fabric/Service portal click "
                "'Refresh visuals' after starting the recording. Some browser/portal "
                "modes capture only timings and omit the DAX text."
            )
        return out
    if isinstance(obj, list):
        out2: list[str] = []
        for q in obj:
            if isinstance(q, str):
                out2.append(q)
            elif isinstance(q, dict):
                v = q.get("query") or q.get("Query")
                if isinstance(v, str):
                    out2.append(v)
        if not out2:
            raise ValueError(
                f"Queries JSON array is empty or contains no recognizable query "
                f"strings ({len(obj)} entries). Expected list of DAX strings or "
                "objects with a 'query'/'Query' field."
            )
        return out2
    raise ValueError("Unrecognized queries.json shape (expected list or {events: [...]})")


def normalize_users(raw_json: str) -> list[dict[str, str]]:
    """Parse a users JSON blob into a list of impersonation dicts.

    Returns dicts with keys ``effectiveUserName``, ``customData``, ``roles``
    (always all three; missing values default to ``""``). ``roles`` is a
    comma-separated string when multiple roles apply (matching the AS
    connection-string ``Roles=R1,R2`` wire format).

    Accepted shapes:

      * Object array: ``[{"effectiveUserName": "...", "customData": "...",
        "roles": "..." | ["..."]}, ...]`` (case-insensitive keys).
      * String array: ``["alice@contoso.com", ...]`` -> each entry maps to
        ``effectiveUserName`` (no customData/roles).

    See ``docs/impersonation.md`` for combination semantics.
    """
    obj = json.loads(raw_json.lstrip("\ufeff"))
    if not isinstance(obj, list):
        raise ValueError("users.json must be a JSON array")

    def _ci_get(d: dict, *keys: str) -> str:
        lc = {k.lower(): v for k, v in d.items()}
        for k in keys:
            v = lc.get(k.lower())
            if isinstance(v, str):
                return v
        return ""

    out: list[dict[str, str]] = []
    for u in obj:
        if isinstance(u, str):
            s = u.strip()
            if s:
                out.append({"effectiveUserName": s, "customData": "", "roles": ""})
            continue
        if not isinstance(u, dict):
            continue
        eun = _ci_get(u, "effectiveUserName").strip()
        cd = _ci_get(u, "customData")
        roles_val = u.get("roles") or u.get("Roles") or ""
        if isinstance(roles_val, list):
            roles = ",".join(str(r) for r in roles_val if isinstance(r, str) and r)
        else:
            roles = str(roles_val)
        # Skip entirely-empty entries unless string shorthand asked for one.
        if eun or cd or roles:
            out.append({"effectiveUserName": eun, "customData": cd, "roles": roles})
        elif not u:
            # An empty {} is intentional: a slot with no impersonation.
            out.append({"effectiveUserName": "", "customData": "", "roles": ""})
    return out


def _resolve_resource(name: str | None, *, allow_auto_discover: bool) -> tuple[str | None, str]:
    if isinstance(name, str) and name.startswith("abfss://"):
        return name, name
    if isinstance(name, str) and name.strip():
        p = f"builtin/{name.lstrip('/')}"
        return (p, f"resources:{p}") if os.path.exists(p) else (None, f"(missing resource '{name}')")
    if allow_auto_discover and os.path.isdir("builtin"):
        candidates = sorted(f for f in os.listdir("builtin") if f.lower().endswith(".json"))
        if len(candidates) == 1:
            p = f"builtin/{candidates[0]}"
            return p, f"resources:{p} (auto-discovered)"
    return None, "(no resource)"


def load_queries(
    queries_file: str | None,
    queries_inline: Iterable[str],
    *,
    read_abfss: ReadAbfss | None = None,
) -> tuple[list[str], str]:
    """Resolve and load the Load Test Scenario query list.

    Returns ``(queries, source_label)``. ``source_label`` is a short
    human-readable description suitable for printing in the notebook.
    """
    p, src = _resolve_resource(queries_file, allow_auto_discover=True)
    if p is not None:
        return normalize_queries(_read_text(p, read_abfss)), src
    return [str(q) for q in queries_inline], "(QUERIES_INLINE fallback)"


def load_users(
    users_file: str | None,
    users_inline: Iterable[dict[str, Any]],
    *,
    read_abfss: ReadAbfss | None = None,
) -> tuple[list[dict[str, str]], str]:
    p, src = _resolve_resource(users_file, allow_auto_discover=False)
    if p is not None:
        return normalize_users(_read_text(p, read_abfss)), src
    inline = list(users_inline)
    if inline:
        return normalize_users(json.dumps(inline)), "(USERS_INLINE)"
    return [{"effectiveUserName": "", "customData": "", "roles": ""}], "(default anonymous user)"
