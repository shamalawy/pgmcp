"""Runtime configuration for pgmcp.

Settings come from (in increasing order of precedence): built-in defaults, a
``.env`` file, environment variables, then explicit CLI flags.  Every field is
settable as ``PGMCP_<FIELD>``; the connection string additionally honours the
conventional ``DATABASE_URL`` and the standard libpq ``PG*`` variables so the
server drops into an existing environment without extra wiring.
"""

from __future__ import annotations

import json
import os
from enum import Enum
from typing import Annotated, Any
from urllib.parse import quote

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from .errors import ConfigurationError, redact_dsn

__all__ = ["AccessMode", "OutputFormat", "Settings", "Toolset", "Transport"]

# Schemas that are noise for almost every question an agent asks.  They can be
# re-enabled per call with ``include_system_schemas=True`` where it makes sense.
SYSTEM_SCHEMAS: tuple[str, ...] = (
    "pg_catalog",
    "information_schema",
    "pg_toast",
)


class AccessMode(str, Enum):
    """How much the server is permitted to change."""

    READ_ONLY = "read-only"
    WRITE = "write"

    @property
    def allows_write(self) -> bool:
        return self is AccessMode.WRITE


class OutputFormat(str, Enum):
    """Serialisation used for tabular tool results."""

    JSON = "json"
    MARKDOWN = "markdown"
    CSV = "csv"


class Toolset(str, Enum):
    """A group of related tools that can be enabled or disabled as a unit.

    Clients pay for every tool in their context window, so an operator who only
    wants schema introspection should not have to ship 30 tool definitions.
    """

    SCHEMA = "schema"
    """Catalog introspection: schemas, tables, columns, indexes, DDL, relationships."""

    QUERY = "query"
    """Running SELECTs, EXPLAIN, sampling and column statistics."""

    HEALTH = "health"
    """Server and database observability: activity, locks, bloat, index usage."""

    WRITE = "write"
    """Mutating statements. Ignored unless access_mode is ``write``."""


class Transport(str, Enum):
    """MCP transport to listen on."""

    STDIO = "stdio"
    STREAMABLE_HTTP = "streamable-http"
    SSE = "sse"


class Settings(BaseSettings):
    """Validated pgmcp configuration."""

    model_config = SettingsConfigDict(
        env_prefix="PGMCP_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    # -- connection ---------------------------------------------------------
    dsn: str | None = Field(
        default=None,
        description="libpq connection string. Falls back to DATABASE_URL, then PG* variables.",
    )
    application_name: str = Field(
        default="pgmcp",
        description="Value reported in pg_stat_activity.application_name.",
    )
    connect_timeout: int = Field(default=10, ge=1, le=300, description="Connect timeout in seconds.")
    pool_min_size: int = Field(default=1, ge=0, le=100)
    pool_max_size: int = Field(default=5, ge=1, le=200)
    pool_timeout: float = Field(
        default=30.0, gt=0, le=600, description="Seconds to wait for a free pooled connection."
    )
    pool_max_idle: float = Field(
        default=300.0, gt=0, description="Seconds an idle pooled connection is kept before closing."
    )

    # -- safety -------------------------------------------------------------
    access_mode: AccessMode = Field(
        default=AccessMode.READ_ONLY,
        description="read-only hides every mutating tool; write enables them.",
    )
    statement_timeout_ms: int = Field(
        default=30_000,
        ge=100,
        le=3_600_000,
        description="Per-statement timeout applied with SET LOCAL on every query.",
    )
    max_rows: int = Field(
        default=500, ge=1, le=100_000, description="Hard cap on rows returned by any single tool call."
    )
    max_result_bytes: int = Field(
        default=1_000_000,
        ge=1024,
        description="Byte budget for a serialised result; output is truncated to fit.",
    )
    max_query_length: int = Field(
        default=200_000, ge=100, description="Reject SQL longer than this many characters."
    )
    allowed_schemas: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="If non-empty, only these schemas are visible. Comma-separated in env vars.",
    )
    blocked_schemas: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="Schemas hidden from every tool, in addition to allowed_schemas filtering.",
    )
    include_system_schemas: bool = Field(
        default=False, description="Expose pg_catalog/information_schema in listings by default."
    )
    allowed_databases: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="If non-empty, cross-database tool calls are limited to these names.",
    )
    allow_cross_database: bool = Field(
        default=True,
        description="Permit tools to target a database other than the one in the DSN.",
    )

    enabled_toolsets: Annotated[list[Toolset], NoDecode] = Field(
        default_factory=lambda: [Toolset.SCHEMA, Toolset.QUERY, Toolset.HEALTH, Toolset.WRITE],
        description="Which tool groups to register. Comma-separated in env vars.",
    )

    # -- output -------------------------------------------------------------
    default_format: OutputFormat = Field(default=OutputFormat.JSON)
    default_row_limit: int = Field(
        default=100, ge=1, description="Row limit used when a tool call does not supply one."
    )

    # -- transport ----------------------------------------------------------
    transport: Transport = Field(default=Transport.STDIO)
    host: str = Field(default="127.0.0.1", description="Bind address for HTTP transports.")
    port: int = Field(default=8000, ge=1, le=65535)
    mount_path: str = Field(default="/mcp", description="URL path for the streamable-http endpoint.")
    json_response: bool = Field(
        default=False, description="Return plain JSON instead of SSE framing on streamable-http."
    )
    stateless_http: bool = Field(
        default=False, description="Run streamable-http without server-side session state."
    )
    log_level: str = Field(default="INFO")

    # -- validators ---------------------------------------------------------

    @field_validator("enabled_toolsets", mode="before")
    @classmethod
    def _split_toolsets(cls, value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            if stripped.startswith("["):
                return json.loads(stripped)
            if stripped.lower() == "all":
                return [t.value for t in Toolset]
            return [part.strip().lower() for part in stripped.split(",") if part.strip()]
        return value

    @field_validator("allowed_schemas", "blocked_schemas", "allowed_databases", mode="before")
    @classmethod
    def _split_csv(cls, value: Any) -> Any:
        """Accept ``a,b,c`` from environment variables as well as a JSON list.

        These fields are annotated ``NoDecode`` so pydantic-settings hands us the
        raw environment string; comma-separated is the ergonomic form and JSON is
        accepted for values that contain commas.
        """
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            if stripped.startswith("["):
                try:
                    return json.loads(stripped)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"expected a JSON list or comma-separated names: {exc}") from exc
            return [part.strip() for part in stripped.split(",") if part.strip()]
        return value

    @field_validator("log_level", mode="before")
    @classmethod
    def _upper_log_level(cls, value: Any) -> Any:
        if isinstance(value, str):
            level = value.upper()
            if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
                raise ValueError(f"invalid log level: {value!r}")
            return level
        return value

    @field_validator("access_mode", mode="before")
    @classmethod
    def _normalise_access_mode(cls, value: Any) -> Any:
        """Accept the ergonomic spellings people actually type."""
        if isinstance(value, str):
            aliases = {
                "readonly": "read-only",
                "read_only": "read-only",
                "ro": "read-only",
                "rw": "write",
                "read-write": "write",
                "readwrite": "write",
            }
            return aliases.get(value.strip().lower(), value.strip().lower())
        return value

    @field_validator("mount_path")
    @classmethod
    def _leading_slash(cls, value: str) -> str:
        return value if value.startswith("/") else f"/{value}"

    @model_validator(mode="after")
    def _check_pool_sizes(self) -> Settings:
        if self.pool_max_size < max(self.pool_min_size, 1):
            raise ValueError("pool_max_size must be >= pool_min_size and at least 1")
        return self

    # -- derived ------------------------------------------------------------

    def resolve_dsn(self) -> str:
        """Return the connection string, deriving one from the environment if needed.

        Resolution order: explicit ``dsn`` / ``PGMCP_DSN`` → ``DATABASE_URL`` →
        ``POSTGRES_URL`` → assembled from ``PGHOST``/``PGPORT``/``PGUSER``/
        ``PGPASSWORD``/``PGDATABASE``.  Raises :class:`ConfigurationError` when
        nothing usable is present, rather than silently connecting to a local
        default the operator did not ask for.
        """
        if self.dsn:
            return self.dsn

        for env_name in ("DATABASE_URL", "POSTGRES_URL", "POSTGRES_DSN"):
            value = os.getenv(env_name)
            if value:
                return value

        host = os.getenv("PGHOST")
        database = os.getenv("PGDATABASE")
        user = os.getenv("PGUSER")
        if not (host or database or user):
            raise ConfigurationError(
                "No database connection configured. Set PGMCP_DSN (or DATABASE_URL) to a "
                "libpq connection string such as "
                "postgresql://user:password@host:5432/dbname, or pass --dsn."
            )

        password = os.getenv("PGPASSWORD")
        port = os.getenv("PGPORT", "5432")
        userinfo = ""
        if user:
            userinfo = quote(user, safe="")
            if password:
                userinfo += f":{quote(password, safe='')}"
            userinfo += "@"
        return f"postgresql://{userinfo}{host or 'localhost'}:{port}/{database or user or 'postgres'}"

    @property
    def safe_dsn(self) -> str:
        """The resolved DSN with its password masked, for logs and tool output."""
        try:
            return redact_dsn(self.resolve_dsn())
        except ConfigurationError:
            return "<unconfigured>"

    def toolset_enabled(self, toolset: Toolset) -> bool:
        """Whether ``toolset`` should be registered given the current settings.

        The write toolset additionally requires write access mode, so a stray
        ``PGMCP_ENABLED_TOOLSETS=write`` can never widen access on its own.
        """
        if toolset is Toolset.WRITE and not self.access_mode.allows_write:
            return False
        return toolset in self.enabled_toolsets

    def hidden_schemas(self) -> tuple[str, ...]:
        """Schemas that must never appear in listings."""
        hidden = set(self.blocked_schemas)
        if not self.include_system_schemas:
            hidden.update(SYSTEM_SCHEMAS)
        return tuple(sorted(hidden))

    def describe(self) -> dict[str, Any]:
        """A redacted summary of the active configuration, safe to return to a client."""
        return {
            "dsn": self.safe_dsn,
            "access_mode": self.access_mode.value,
            "statement_timeout_ms": self.statement_timeout_ms,
            "max_rows": self.max_rows,
            "max_result_bytes": self.max_result_bytes,
            "default_row_limit": self.default_row_limit,
            "default_format": self.default_format.value,
            "allowed_schemas": self.allowed_schemas or None,
            "blocked_schemas": self.blocked_schemas or None,
            "include_system_schemas": self.include_system_schemas,
            "allow_cross_database": self.allow_cross_database,
            "allowed_databases": self.allowed_databases or None,
            "pool": {"min_size": self.pool_min_size, "max_size": self.pool_max_size},
            "transport": self.transport.value,
            "enabled_toolsets": [t.value for t in self.enabled_toolsets if self.toolset_enabled(t)],
        }
