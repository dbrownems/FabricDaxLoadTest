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
        (``{"version": ..., "events": [{"query": "..."}, ...]}``)
      * Object array: ``[{"query": "..."}, ...]`` or
        ``[{"Query": "..."}, ...]``
      * String array: ``["EVALUATE ...", ...]``
    """
    obj = json.loads(raw_json)
    if isinstance(obj, dict) and isinstance(obj.get("events"), list):
        out: list[str] = []
        for ev in obj["events"]:
            q = ev.get("query")
            if not q:
                qd = ev.get("Query") or {}
                if isinstance(qd, dict):
                    q = qd.get("Query")
            if isinstance(q, str) and q.strip():
                out.append(q)
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
        return out2
    raise ValueError("Unrecognized queries.json shape (expected list or {events: [...]})")


def normalize_users(raw_json: str) -> list[dict[str, str]]:
    """Parse a users JSON blob into a list of ``{email, role}`` dicts.

    Accepted shapes:

      * Object array: ``[{"email": "...", "role": "..."}, ...]``
        (case-insensitive keys)
      * String array: ``["alice@contoso.com", "bob@contoso.com"]``
        (roles default to "")
    """
    obj = json.loads(raw_json)
    if not isinstance(obj, list):
        raise ValueError("users.json must be a JSON array")
    out: list[dict[str, str]] = []
    for u in obj:
        if isinstance(u, str):
            email = u.strip()
            if email:
                out.append({"email": email, "role": ""})
        elif isinstance(u, dict):
            email = (u.get("email") or u.get("Email") or "").strip()
            role = (u.get("role") or u.get("Role") or "")
            if email:
                out.append({"email": email, "role": role})
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
        return [{"email": u.get("email", ""), "role": u.get("role", "")} for u in inline], "(USERS_INLINE)"
    return [{"email": "anonymous@local", "role": ""}], "(default anonymous user)"
