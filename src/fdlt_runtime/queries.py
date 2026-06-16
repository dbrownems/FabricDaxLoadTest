"""Query and user scenario loading.

Resolution order for a given ``queries_file`` / ``users_file``:

  1. ``"abfss://..."`` or ``"https://*.dfs.fabric.microsoft.com/..."`` —
     OneLake URL (read via the caller-supplied ``read_abfss`` callback;
     in the notebook that's ``notebookutils.fs.head``).
  2. ``"name.json"`` / ``"name.jsonl"`` — load ``builtin/<name>`` from
     the kernel CWD (the Fabric notebook Resources panel is mounted
     there).
  3. ``None`` — auto-discover: if exactly one ``*.json`` or ``*.jsonl``
     file is present under ``builtin/``, use it. Auto-discovery applies
     to **queries only**; users must always be named explicitly.
  4. None of the above and ``queries_file`` / ``users_file`` is empty
     → caller falls back to its inline default.

A non-empty ``queries_file`` that fails to resolve raises
``FileNotFoundError`` rather than silently falling through to inline,
so a typo'd URL or a not-yet-attached resource is loud.

Supported queries shapes:

  * ``.json`` — Power BI Performance Analyzer export, object array, or
    plain string array. See :func:`normalize_queries_with_visuals`.
  * ``.jsonl`` — Profiler / SSAS trace export with one event per line:
    ``{"eventClass":"QueryEnd","cols":{"TextData":"<dax>"}}``. Only
    ``QueryEnd`` events contribute; other event classes are ignored.
    See :func:`normalize_queries_jsonl`.
"""

from __future__ import annotations

import json
import os
from typing import Any, Callable, Iterable

ReadAbfss = Callable[[str], str]

# Schemes we delegate to the `read_abfss` callback (notebookutils.fs.head).
# Both abfss:// and https:// OneLake DFS forms are accepted — they point at
# the same backend; abfss is the AS-friendly form, https is what the OneLake
# UI shows in "Copy URL".
_REMOTE_PREFIXES = ("abfss://", "https://", "http://")


def _is_remote_url(s: str) -> bool:
    return s.startswith(_REMOTE_PREFIXES)


def _read_text(path: str, read_abfss: ReadAbfss | None) -> str:
    if _is_remote_url(path):
        if read_abfss is None:
            raise RuntimeError(
                f"Remote URL {path!r} supplied but no `read_abfss` reader "
                "provided. Pass notebookutils.fs.head (or a wrapper) into "
                "load_queries / load_users."
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


def normalize_queries_jsonl(raw_text: str) -> tuple[list[str], list[dict[str, str] | None]]:
    """Parse a Profiler / SSAS trace JSONL export into ``(queries, visuals)``.

    Each non-blank line must be a JSON object of the shape::

        {"eventClass": "QueryEnd", "cols": {"TextData": "<dax>"}}

    Only events whose ``eventClass`` is ``"QueryEnd"`` (case-insensitive)
    contribute a query. Other event classes (``QueryBegin``,
    ``VertiPaqSEQueryEnd``, ``ExecutionMetrics``, etc.) are silently
    ignored — they may share the same ``TextData`` as the matching
    ``QueryEnd`` and would produce duplicates. ``cols.TextData`` is
    accepted case-insensitively (``textData`` works too).

    Trace JSONL files don't carry per-query visual metadata, so the
    returned visuals list is all ``None`` (parallel to ``queries``).

    Tolerates a leading UTF-8 BOM, blank lines, and trailing whitespace.
    """
    queries: list[str] = []
    end_event_count = 0
    bad_lines = 0
    for raw_line in raw_text.lstrip("\ufeff").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            bad_lines += 1
            continue
        if not isinstance(ev, dict):
            continue
        ec = ev.get("eventClass") or ev.get("EventClass") or ev.get("event_class")
        if not isinstance(ec, str) or ec.strip().lower() != "queryend":
            continue
        end_event_count += 1
        cols = ev.get("cols") or ev.get("Cols") or {}
        text = None
        if isinstance(cols, dict):
            for k in ("TextData", "textData", "text_data"):
                v = cols.get(k)
                if isinstance(v, str) and v.strip():
                    text = v
                    break
        if text is None:
            # Some exports flatten cols into the top-level event.
            for k in ("TextData", "textData", "text_data"):
                v = ev.get(k)
                if isinstance(v, str) and v.strip():
                    text = v
                    break
        if text is not None:
            queries.append(text)
    if not queries:
        raise ValueError(
            "Trace JSONL file contains no QueryEnd events with TextData "
            f"({end_event_count} QueryEnd event(s) seen, "
            f"{bad_lines} unparseable line(s)). Expected one JSON object "
            "per line of the form "
            '{"eventClass":"QueryEnd","cols":{"TextData":"<DAX>"}}.'
        )
    return queries, [None] * len(queries)


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
    if isinstance(name, str) and _is_remote_url(name):
        return name, name
    if isinstance(name, str) and name.strip():
        p = f"builtin/{name.lstrip('/')}"
        return (p, f"resources:{p}") if os.path.exists(p) else (None, f"(missing resource '{name}')")
    if allow_auto_discover and os.path.isdir("builtin"):
        candidates = sorted(
            f for f in os.listdir("builtin")
            if f.lower().endswith((".json", ".jsonl"))
        )
        if len(candidates) == 1:
            p = f"builtin/{candidates[0]}"
            return p, f"resources:{p} (auto-discovered)"
    return None, "(no resource)"


def _raise_unresolved(kind: str, name: str) -> None:
    raise FileNotFoundError(
        f"{kind}_file={name!r} did not resolve to a readable path. "
        "Expected one of: a filename present on the notebook's Resources "
        "panel (e.g. 'queries.jsonl'), an abfss://... URL, or an "
        "https://*.dfs.fabric.microsoft.com/... OneLake URL."
    )


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

    Raises ``FileNotFoundError`` when ``queries_file`` is a non-empty
    string that doesn't resolve to a readable path — the inline fallback
    only kicks in when no ``queries_file`` was supplied.
    """
    p, src = _resolve_resource(queries_file, allow_auto_discover=True)
    if p is not None:
        text = _read_text(p, read_abfss)
        if p.lower().endswith(".jsonl"):
            queries, visuals = normalize_queries_jsonl(text)
        else:
            queries, visuals = normalize_queries_with_visuals(text)
        return queries, visuals, src
    if isinstance(queries_file, str) and queries_file.strip():
        _raise_unresolved("queries", queries_file)
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
    if isinstance(users_file, str) and users_file.strip():
        _raise_unresolved("users", users_file)
    inline = list(users_inline)
    if inline:
        return normalize_users(json.dumps(inline)), "(USERS_INLINE)"
    return [{"effectiveUserName": "", "customData": "", "roles": ""}], "(default anonymous user)"
