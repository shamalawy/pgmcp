"""pgmcp — a fully featured Model Context Protocol server for PostgreSQL."""

from __future__ import annotations

from .config import AccessMode, OutputFormat, Settings, Toolset, Transport
from .errors import (
    AccessDeniedError,
    ConfigurationError,
    DatabaseNotAllowedError,
    PgMcpError,
    QueryExecutionError,
    SchemaNotAllowedError,
    UnsafeQueryError,
)

try:  # pragma: no cover - trivial packaging shim
    from importlib.metadata import PackageNotFoundError, version

    __version__ = version("pgmcp")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0.dev0"

__all__ = [
    "AccessDeniedError",
    "AccessMode",
    "ConfigurationError",
    "DatabaseNotAllowedError",
    "OutputFormat",
    "PgMcpError",
    "QueryExecutionError",
    "SchemaNotAllowedError",
    "Settings",
    "Toolset",
    "Transport",
    "UnsafeQueryError",
    "__version__",
]
