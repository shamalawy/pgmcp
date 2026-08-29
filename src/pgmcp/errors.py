"""Exception hierarchy and secret-redaction helpers for pgmcp.

Every error raised by pgmcp derives from :class:`PgMcpError`.  Tool wrappers
convert these into human-readable messages, so error text is considered
user-facing: never interpolate a raw DSN or password into one.  Use
:func:`redact_dsn` on anything that may carry credentials.
"""

from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit

__all__ = [
    "AccessDeniedError",
    "ConfigurationError",
    "DatabaseNotAllowedError",
    "PgMcpError",
    "QueryExecutionError",
    "ResultTooLargeError",
    "SchemaNotAllowedError",
    "UnsafeQueryError",
    "redact_dsn",
    "redact_text",
]

# ``password=hunter2`` / ``password = 'hunter2'`` in libpq keyword/value DSNs.
_KEYWORD_PASSWORD_RE = re.compile(r"(?i)\b(password)\s*=\s*('(?:[^']|'')*'|\S+)")


class PgMcpError(Exception):
    """Base class for every error raised by pgmcp."""


class ConfigurationError(PgMcpError):
    """The server was started with missing or contradictory configuration."""


class AccessDeniedError(PgMcpError):
    """A write was attempted while the server is in read-only access mode."""


class UnsafeQueryError(PgMcpError):
    """SQL was rejected by the static guard before it reached the server."""


class SchemaNotAllowedError(PgMcpError):
    """The requested schema is excluded by the allow/deny configuration."""


class DatabaseNotAllowedError(PgMcpError):
    """The requested database is not in the configured allow-list."""


class ResultTooLargeError(PgMcpError):
    """A result exceeded the configured byte budget even after row truncation."""


class QueryExecutionError(PgMcpError):
    """PostgreSQL rejected or aborted the statement.

    Carries the structured diagnostic fields psycopg exposes so that callers
    can surface ``SQLSTATE``, the position of a syntax error, and the server's
    own ``HINT`` without re-parsing the message.
    """

    def __init__(
        self,
        message: str,
        *,
        sqlstate: str | None = None,
        detail: str | None = None,
        hint: str | None = None,
        position: int | None = None,
    ) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.detail = detail
        self.hint = hint
        self.position = position

    def as_dict(self) -> dict[str, object]:
        """Return the diagnostics as a JSON-serialisable mapping."""
        payload: dict[str, object] = {"error": str(self)}
        for key, value in (
            ("sqlstate", self.sqlstate),
            ("detail", self.detail),
            ("hint", self.hint),
            ("position", self.position),
        ):
            if value is not None:
                payload[key] = value
        return payload


def redact_dsn(dsn: str) -> str:
    """Return ``dsn`` with any password replaced by ``***``.

    Handles both libpq URI form (``postgresql://user:pw@host/db``) and
    keyword/value form (``host=x password=pw``).  Unparseable input is returned
    fully masked rather than passed through, so a malformed DSN can never leak.
    """
    if not dsn:
        return dsn

    if "://" in dsn:
        try:
            parts = urlsplit(dsn)
        except ValueError:
            return "***"
        if parts.password:
            userinfo = f"{parts.username or ''}:***"
            host = parts.hostname or ""
            if parts.port:
                host = f"{host}:{parts.port}"
            netloc = f"{userinfo}@{host}"
            return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
        return dsn

    return _KEYWORD_PASSWORD_RE.sub(r"\1=***", dsn)


def redact_text(text: str, *secrets: str | None) -> str:
    """Strip literal ``secrets`` out of ``text``.

    Used as a last line of defence around driver error messages, which
    occasionally echo the connection string back.
    """
    result = text
    for secret in secrets:
        if secret:
            result = result.replace(secret, "***")
    return result
