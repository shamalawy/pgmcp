"""Result containers and serialisation.

Tool results are plain strings so that every MCP client renders them, including
ones that ignore ``structuredContent``.  Three shapes are offered: JSON (the
default, unambiguous), Markdown (compact and readable in a chat transcript) and
CSV (paste-ready).  All three go through the same truncation budget so a single
``SELECT *`` on a wide table can never blow up a context window.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import Enum
from ipaddress import IPv4Address, IPv4Interface, IPv4Network, IPv6Address, IPv6Interface, IPv6Network
from typing import Any
from uuid import UUID

from .config import OutputFormat

__all__ = ["QueryResult", "json_default", "render", "to_jsonable", "truncate"]

_TRUNCATION_SUFFIX = "\n\n[output truncated: exceeded max_result_bytes]"


def json_default(value: Any) -> Any:
    """Fallback encoder for PostgreSQL types with no direct JSON equivalent."""
    if isinstance(value, Decimal):
        # Preserve exactness: a float round-trip silently corrupts money columns.
        return str(value)
    if isinstance(value, datetime | date | time):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, memoryview | bytes | bytearray):
        raw = bytes(value)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return f"\\x{raw.hex()}"
    if isinstance(
        value,
        IPv4Address | IPv6Address | IPv4Network | IPv6Network | IPv4Interface | IPv6Interface,
    ):
        return str(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, set | frozenset | tuple):
        return list(value)
    if hasattr(value, "isoformat"):  # any other date-like
        return value.isoformat()
    return str(value)


def to_jsonable(value: Any) -> Any:
    """Recursively convert ``value`` into something ``json.dumps`` accepts."""
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set | frozenset):
        return [to_jsonable(v) for v in value]
    return json_default(value)


def _cell(value: Any) -> str:
    """Render one value for a text-shaped output (Markdown or CSV)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return value
    if isinstance(value, dict | list | tuple):
        return json.dumps(to_jsonable(value), default=json_default, ensure_ascii=False)
    return str(json_default(value)) if not isinstance(value, int | float) else str(value)


@dataclass
class QueryResult:
    """A tabular result plus everything a caller needs to interpret it."""

    columns: list[str] = field(default_factory=list)
    rows: list[dict[str, Any]] = field(default_factory=list)
    row_count: int = 0
    """Rows actually returned (``len(rows)``)."""

    truncated: bool = False
    """True when more rows existed than the applied limit."""

    limit: int | None = None
    command_status: str | None = None
    """The server's status tag, e.g. ``SELECT 12`` or ``UPDATE 3``."""

    affected_rows: int | None = None
    duration_ms: float | None = None
    notices: list[str] = field(default_factory=list)
    database: str | None = None
    read_only: bool = True

    def metadata(self) -> dict[str, Any]:
        """The non-row part of the result, for embedding in serialised output."""
        meta: dict[str, Any] = {"row_count": self.row_count}
        if self.truncated:
            meta["truncated"] = True
            meta["limit"] = self.limit
            meta["note"] = (
                f"Only the first {self.limit} row(s) are shown. Add LIMIT/OFFSET or raise "
                "the limit argument to see more."
            )
        if self.affected_rows is not None:
            meta["affected_rows"] = self.affected_rows
        if self.command_status:
            meta["command"] = self.command_status
        if self.duration_ms is not None:
            meta["duration_ms"] = round(self.duration_ms, 2)
        if self.database:
            meta["database"] = self.database
        if self.notices:
            meta["notices"] = self.notices
        return meta

    def to_dict(self) -> dict[str, Any]:
        return {**self.metadata(), "columns": self.columns, "rows": [to_jsonable(r) for r in self.rows]}


def _render_json(result: QueryResult) -> str:
    return json.dumps(result.to_dict(), indent=2, default=json_default, ensure_ascii=False)


def _render_markdown(result: QueryResult) -> str:
    if not result.columns:
        lines = [f"_{result.command_status or 'no columns returned'}_"]
        meta = result.metadata()
        if meta.get("affected_rows") is not None:
            lines.append(f"\n{meta['affected_rows']} row(s) affected.")
        return "\n".join(lines)
    if not result.rows:
        header = " | ".join(result.columns)
        return f"| {header} |\n|{'|'.join('---' for _ in result.columns)}|\n\n_0 rows_"

    def escape(text: str) -> str:
        return text.replace("|", "\\|").replace("\n", "<br>")

    body = [
        "| " + " | ".join(escape(c) for c in result.columns) + " |",
        "|" + "|".join("---" for _ in result.columns) + "|",
    ]
    body.extend(
        "| " + " | ".join(escape(_cell(row.get(col))) for col in result.columns) + " |" for row in result.rows
    )
    footer = [f"\n_{result.row_count} row(s)"]
    if result.truncated:
        footer.append(f", truncated at limit {result.limit}")
    if result.duration_ms is not None:
        footer.append(f", {result.duration_ms:.1f} ms")
    footer.append("_")
    return "\n".join(body) + "".join(footer)


def _render_csv(result: QueryResult) -> str:
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(result.columns)
    for row in result.rows:
        writer.writerow(["" if row.get(c) is None else _cell(row.get(c)) for c in result.columns])
    text = buffer.getvalue()
    if result.truncated:
        text += f"# truncated at limit {result.limit}\n"
    return text


def render(result: QueryResult, fmt: OutputFormat | str = OutputFormat.JSON, *, max_bytes: int = 0) -> str:
    """Serialise ``result`` in ``fmt``, trimmed to ``max_bytes`` if positive."""
    output_format = OutputFormat(fmt) if not isinstance(fmt, OutputFormat) else fmt
    renderers = {
        OutputFormat.JSON: _render_json,
        OutputFormat.MARKDOWN: _render_markdown,
        OutputFormat.CSV: _render_csv,
    }
    return truncate(renderers[output_format](result), max_bytes)


def truncate(text: str, max_bytes: int) -> str:
    """Cut ``text`` to ``max_bytes`` UTF-8 bytes, appending a visible marker.

    Trims on a character boundary so the result is always valid UTF-8.
    """
    if max_bytes <= 0:
        return text
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    budget = max(0, max_bytes - len(_TRUNCATION_SUFFIX.encode("utf-8")))
    return encoded[:budget].decode("utf-8", errors="ignore") + _TRUNCATION_SUFFIX
