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

    Thin wrapper around :func:`normalize_queries_with_visuals` that drops
    the per-query visual metadata. See that function for the recognised
    shapes and Performance-Analyzer caching warning.
    """
    queries, _visuals = normalize_queries_with_visuals(raw_json)
    return queries


def normalize_queries_with_visuals(
    raw_json: str,
) -> tuple[list[str], list[dict[str, str] | None]]:
    """Parse a queries JSON blob into ``(queries, visuals)``.

    ``queries`` is a flat list of DAX strings. ``visuals`` is a parallel
    list (same length, same order) of per-query visual metadata dicts
    with keys ``visualId``, ``visualTitle``, ``visualType``, or ``None``
    when the source has no visual binding (inline arrays etc.).

    Accepted shapes:

      * Power BI Desktop *Performance Analyzer* export
        (``{"version": ..., "events": [...]}``). DAX text is pulled from
        ``event.metrics.QueryText`` for events whose ``name`` is
        ``"Execute DAX Query"`` (the actual DSE-issued DAX). Each
        ``"Execute DAX Query"`` is attributed to the most recent
        ``"Visual Container Lifecycle"`` event observed before it (the
        canonical ordering Power BI Desktop emits). Lifecycle events
        with no following ``"Execute DAX Query"`` indicate the visual
        served from cache; we emit a stderr warning explaining how to
        re-capture without caching. Older / alternative event shapes
        (``event.query`` or ``event.Query.Query``) are also accepted
        but carry no visual metadata.
      * Object array: ``[{"query": "..."}, ...]`` or
        ``[{"Query": "..."}, ...]``
      * String array: ``["EVALUATE ...", ...]``

    Tolerates a leading UTF-8 BOM (Power BI Desktop writes one).
    """
    import sys

    obj = json.loads(raw_json.lstrip("\ufeff"))
    if isinstance(obj, dict) and isinstance(obj.get("events"), list):
        events = obj["events"]
        queries: list[str] = []
        visuals: list[dict[str, str] | None] = []
        query_named_events = 0
        pending_visual: dict[str, str] | None = None
        cached_visuals: list[dict[str, str]] = []
        for ev in events:
            if not isinstance(ev, dict):
                continue
            name = ev.get("name")
            if name == "Visual Container Lifecycle":
                # If a previous lifecycle was never paired with an
                # Execute DAX Query, the visual served from cache.
                if pending_visual is not None:
                    cached_visuals.append(pending_visual)
                m = ev.get("metrics") or {}
                if isinstance(m, dict):
                    vid = m.get("visualId") or m.get("VisualId")
                    if isinstance(vid, str) and vid:
                        pending_visual = {
                            "visualId": vid,
                            "visualTitle": str(m.get("visualTitle")
                                              or m.get("VisualTitle") or ""),
                            "visualType":  str(m.get("visualType")
                                              or m.get("VisualType")  or ""),
                        }
                    else:
                        pending_visual = None
                continue
            if name in ("Execute DAX Query", "Query"):
                query_named_events += 1
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
                queries.append(q)
                visuals.append(pending_visual)
                pending_visual = None
        if pending_visual is not None:
            cached_visuals.append(pending_visual)
        if cached_visuals:
            titles = ", ".join(
                f"{v.get('visualTitle') or '(untitled)'} [{v.get('visualType') or '?'}]"
                for v in cached_visuals[:5])
            extra = "" if len(cached_visuals) <= 5 else f" (+{len(cached_visuals)-5} more)"
            print(
                f"\u26a0  {len(cached_visuals)} Visual Container Lifecycle event(s) "
                "had no following 'Execute DAX Query' — those visuals likely "
                f"served from cache: {titles}{extra}.\n"
                "   To capture DAX for every visual, in Power BI Desktop:\n"
                "     1. Home > Transform data > Data source settings > "
                "Clear permissions, OR restart Desktop to drop the visual cache.\n"
                "     2. View > Performance Analyzer > Start recording.\n"
                "     3. Performance Analyzer pane > 'Refresh visuals' "
                "(NOT page-level refresh — page refresh re-uses cached results).\n"
                "     4. Stop recording > Export.\n"
                "   In the Fabric/Power BI service, hard-reload the report tab "
                "(Ctrl+F5) before clicking Refresh visuals so the browser-side "
                "visual cache is dropped too.",
                file=sys.stderr,
            )
        if not queries:
            raise ValueError(
                "Performance Analyzer export contains no DAX query text "
                f"({len(events)} events, {query_named_events} Query/Execute DAX Query "
                "events, but none had metrics.QueryText). Re-record the trace from "
                "Power BI Desktop (View > Performance Analyzer > Start recording > "
                "Refresh visuals > Export), or in the Fabric/Service portal click "
                "'Refresh visuals' after starting the recording. Some browser/portal "
                "modes capture only timings and omit the DAX text."
            )
        return queries, visuals
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
        return out2, [None] * len(out2)
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
) -> tuple[list[str], list[dict[str, str] | None], str]:
    """Resolve and load the Load Test Scenario query list.

    Returns ``(queries, visuals, source_label)``. ``visuals`` is a
    parallel list (same length, same order as ``queries``) of per-query
    visual metadata dicts (``visualId``/``visualTitle``/``visualType``)
    or ``None`` when the source has no visual binding.
    ``source_label`` is a short human-readable description suitable for
    printing in the notebook.
    """
    p, src = _resolve_resource(queries_file, allow_auto_discover=True)
    if p is not None:
        queries, visuals = normalize_queries_with_visuals(_read_text(p, read_abfss))
        return queries, visuals, src
    qs = [str(q) for q in queries_inline]
    return qs, [None] * len(qs), "(QUERIES_INLINE fallback)"


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
