"""Connection pooling and statement execution.

Design notes
------------
*Read-only is enforced by PostgreSQL, not by us.*  Every read runs inside a
transaction that begins with ``SET TRANSACTION READ ONLY``; the server then
raises SQLSTATE 25006 for any write, no matter how it is disguised.  The static
guard in :mod:`pgmcp.sql_guard` runs first only so the user gets a clear message.

*Every statement is bounded.*  ``statement_timeout``, ``idle_in_transaction_session_timeout``
and ``lock_timeout`` are set with ``SET LOCAL`` inside the transaction, so they
apply to that statement alone and are reverted when it ends — nothing leaks onto
the next borrower of a pooled connection.

*Row limits are applied by fetching one extra row* rather than by rewriting the
SQL.  Wrapping user SQL in a subquery breaks statements that already have
``LIMIT``, ``ORDER BY ... FOR UPDATE`` or multiple result shapes; fetching
``limit + 1`` and reporting ``truncated`` is exact and never changes semantics.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import Any

import psycopg
from psycopg import AsyncConnection, sql
from psycopg.rows import DictRow, dict_row
from psycopg_pool import AsyncConnectionPool

from .config import AccessMode, Settings
from .errors import (
    AccessDeniedError,
    ConfigurationError,
    DatabaseNotAllowedError,
    QueryExecutionError,
    redact_text,
)
from .formatting import QueryResult
from .sql_guard import StatementInfo, StatementKind, analyze, assert_read_only

# psycopg's own ``Query`` alias includes ``string.templatelib.Template``, which
# ``execute`` does not actually accept; narrow it to the forms pgmcp passes.
SQLQuery = str | bytes | sql.SQL | sql.Composed

logger = logging.getLogger("pgmcp.database")

__all__ = ["Database", "get_database", "set_database"]

_database: Database | None = None


def set_database(database: Database | None) -> None:
    """Install the process-wide :class:`Database` used by the tool functions."""
    global _database
    _database = database


def get_database() -> Database:
    """Return the installed :class:`Database` or raise a clear configuration error."""
    if _database is None:
        raise ConfigurationError("the database layer is not initialised; the server is still starting")
    return _database


# Statement kinds that pgmcp refuses in *every* access mode. Transaction and
# session control would corrupt the state of a pooled connection; COPY to or from
# a file, and anything unrecognised, are refused because their blast radius is not
# something a static guard can bound.
_ALWAYS_REFUSED = frozenset(
    {
        StatementKind.TRANSACTION,
        StatementKind.SESSION,
        StatementKind.COPY,
        StatementKind.UNKNOWN,
        StatementKind.EMPTY,
    }
)


def _apply_policy(info: StatementInfo, *, allow_writes: bool) -> StatementInfo:
    """Decide whether an analysed statement may run, raising if it may not.

    Write access widens what is permitted to DML, DDL, maintenance and procedure
    calls. It never unlocks the always-refused kinds, and never unlocks a
    statement flagged for calling a filesystem or cross-session function --
    write access is permission to change *this database*, not the host.
    """
    if info.kind in _ALWAYS_REFUSED:
        detail = info.reason or f"{info.kind.value} statements are not permitted"
        raise AccessDeniedError(f"refused: {detail}")
    if info.kind is StatementKind.READ and info.reason:
        raise AccessDeniedError(f"refused: {info.reason}")
    if not info.is_read_only and not allow_writes:
        raise AccessDeniedError(
            f"refused in read-only mode: {info.reason or 'this statement modifies the database'}"
        )
    return info


class Database:
    """Owns one connection pool per database name and runs guarded statements."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._base_dsn = settings.resolve_dsn()
        self._pools: dict[str, AsyncConnectionPool[AsyncConnection[DictRow]]] = {}
        self._lock = asyncio.Lock()
        self._default_database: str | None = None

    # -- lifecycle ----------------------------------------------------------

    async def connect(self) -> None:
        """Open the pool for the DSN's own database and record its name."""
        pool = await self._pool_for(None)
        async with pool.connection() as conn:
            cursor = await conn.execute("SELECT current_database()")
            row = await cursor.fetchone()
            self._default_database = str(row["current_database"]) if row else None
        logger.info(
            "connected to %s (database=%s, access_mode=%s)",
            self.settings.safe_dsn,
            self._default_database,
            self.settings.access_mode.value,
        )

    async def close(self) -> None:
        """Close every pool this instance opened."""
        async with self._lock:
            pools, self._pools = self._pools, {}
        for pool in pools.values():
            await pool.close()

    @property
    def default_database(self) -> str | None:
        return self._default_database

    @property
    def access_mode(self) -> AccessMode:
        return self.settings.access_mode

    # -- pools --------------------------------------------------------------

    def _dsn_for(self, database: str | None) -> str:
        if database is None:
            return self._base_dsn
        # Rebuild the DSN with a different dbname rather than string-splicing, so
        # odd characters in the password or host cannot corrupt the result.
        info: dict[str, Any] = dict(psycopg.conninfo.conninfo_to_dict(self._base_dsn))
        info["dbname"] = database
        return psycopg.conninfo.make_conninfo(**info)

    def _check_database_allowed(self, database: str | None) -> None:
        if database is None:
            return
        if self._default_database is not None and database == self._default_database:
            return
        if not self.settings.allow_cross_database:
            raise DatabaseNotAllowedError(
                f"cross-database access is disabled; cannot target database {database!r}"
            )
        allowed = self.settings.allowed_databases
        if allowed and database not in allowed:
            raise DatabaseNotAllowedError(
                f"database {database!r} is not in the configured allow-list ({', '.join(allowed)})"
            )

    async def _pool_for(self, database: str | None) -> AsyncConnectionPool[AsyncConnection[DictRow]]:
        self._check_database_allowed(database)
        key = database or "__default__"
        pool = self._pools.get(key)
        if pool is not None:
            return pool
        async with self._lock:
            pool = self._pools.get(key)
            if pool is not None:
                return pool
            settings = self.settings
            pool = AsyncConnectionPool[AsyncConnection[DictRow]](
                conninfo=self._dsn_for(database),
                min_size=settings.pool_min_size,
                max_size=settings.pool_max_size,
                timeout=settings.pool_timeout,
                max_idle=settings.pool_max_idle,
                open=False,
                kwargs={
                    "row_factory": dict_row,
                    "application_name": settings.application_name,
                    "connect_timeout": settings.connect_timeout,
                },
                name=f"pgmcp-{key}",
            )
            try:
                await pool.open(wait=True, timeout=settings.pool_timeout)
            except Exception as exc:  # pragma: no cover - depends on a live server
                await pool.close()
                raise QueryExecutionError(
                    f"could not connect to {self.settings.safe_dsn}: "
                    f"{redact_text(str(exc), self._password())}"
                ) from exc
            self._pools[key] = pool
            return pool

    def _password(self) -> str | None:
        try:
            return psycopg.conninfo.conninfo_to_dict(self._base_dsn).get("password")  # type: ignore[return-value]
        except Exception:  # pragma: no cover - malformed DSN already rejected at connect
            return None

    # -- execution ----------------------------------------------------------

    @asynccontextmanager
    async def _session(
        self,
        *,
        database: str | None,
        read_only: bool,
        timeout_ms: int | None,
    ) -> AsyncIterator[AsyncConnection[DictRow]]:
        """A transaction with the guard rails applied, rolled back if read-only.

        A read-only transaction is always rolled back: nothing it did should
        outlive it, and rollback is cheaper than commit for a read.
        """
        pool = await self._pool_for(database)
        timeout = timeout_ms if timeout_ms is not None else self.settings.statement_timeout_ms
        async with pool.connection() as conn:
            try:
                async with conn.transaction(force_rollback=read_only):
                    if read_only:
                        await conn.execute("SET TRANSACTION READ ONLY")
                    # SET LOCAL reverts at transaction end, so pooled connections
                    # are never left with a modified timeout.
                    await conn.execute(f"SET LOCAL statement_timeout = {int(timeout)}")
                    await conn.execute(f"SET LOCAL lock_timeout = {int(min(timeout, 10_000))}")
                    await conn.execute(
                        f"SET LOCAL idle_in_transaction_session_timeout = {int(timeout) + 5_000}"
                    )
                    yield conn
            except psycopg.Error as exc:
                raise self._wrap(exc) from exc

    def _wrap(self, exc: psycopg.Error) -> QueryExecutionError:
        """Turn a psycopg error into a :class:`QueryExecutionError` with diagnostics."""
        diag = getattr(exc, "diag", None)
        message = redact_text(str(exc).strip(), self._password())
        position = None
        raw_position = getattr(diag, "statement_position", None)
        if raw_position:
            try:
                position = int(raw_position)
            except (TypeError, ValueError):
                position = None
        return QueryExecutionError(
            message,
            sqlstate=getattr(exc, "sqlstate", None),
            detail=getattr(diag, "message_detail", None),
            hint=getattr(diag, "message_hint", None),
            position=position,
        )

    async def fetch(
        self,
        query: SQLQuery,
        params: Sequence[Any] | dict[str, Any] | None = None,
        *,
        database: str | None = None,
        limit: int | None = None,
        timeout_ms: int | None = None,
        read_only: bool = True,
    ) -> QueryResult:
        """Run ``query`` and return at most ``limit`` rows.

        ``params`` are bound by the driver, never interpolated. ``limit`` is
        applied by over-fetching a single row so that ``truncated`` is exact.
        """
        effective_limit = self._clamp_limit(limit)
        started = time.perf_counter()
        notices: list[str] = []

        async with self._session(database=database, read_only=read_only, timeout_ms=timeout_ms) as conn:
            conn.add_notice_handler(lambda diag: notices.append(str(diag.message_primary or "").strip()))
            try:
                cursor = await conn.execute(query, params)
            except psycopg.Error as exc:
                raise self._wrap(exc) from exc

            columns = [d.name for d in cursor.description] if cursor.description else []
            rows: list[dict[str, Any]] = []
            truncated = False
            if cursor.description:
                fetched = await cursor.fetchmany(effective_limit + 1)
                if len(fetched) > effective_limit:
                    truncated = True
                    fetched = fetched[:effective_limit]
                rows = list(fetched)

            return QueryResult(
                columns=columns,
                rows=rows,
                row_count=len(rows),
                truncated=truncated,
                limit=effective_limit if truncated else None,
                command_status=cursor.statusmessage,
                affected_rows=cursor.rowcount if not cursor.description and cursor.rowcount >= 0 else None,
                duration_ms=(time.perf_counter() - started) * 1000,
                notices=[n for n in notices if n],
                database=database or self._default_database,
                read_only=read_only,
            )

    async def fetch_all(
        self,
        query: SQLQuery,
        params: Sequence[Any] | dict[str, Any] | None = None,
        *,
        database: str | None = None,
        timeout_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        """Convenience wrapper for internal introspection queries with known-small results."""
        result = await self.fetch(
            query, params, database=database, limit=self.settings.max_rows, timeout_ms=timeout_ms
        )
        return result.rows

    async def fetch_one(
        self,
        query: SQLQuery,
        params: Sequence[Any] | dict[str, Any] | None = None,
        *,
        database: str | None = None,
        timeout_ms: int | None = None,
    ) -> dict[str, Any] | None:
        """Return the first row of ``query`` or ``None``."""
        result = await self.fetch(query, params, database=database, limit=1, timeout_ms=timeout_ms)
        return result.rows[0] if result.rows else None

    async def run_user_query(
        self,
        sql: str,
        params: Sequence[Any] | dict[str, Any] | None = None,
        *,
        database: str | None = None,
        limit: int | None = None,
        timeout_ms: int | None = None,
    ) -> QueryResult:
        """Run client-supplied SQL after applying the guard for the current access mode."""
        self._check_length(sql)
        info = self._guard(sql)
        return await self.fetch(
            info.sql,
            params,
            database=database,
            limit=limit,
            timeout_ms=timeout_ms,
            read_only=info.is_read_only,
        )

    async def run_transaction(
        self,
        statements: Sequence[str],
        *,
        database: str | None = None,
        timeout_ms: int | None = None,
        limit: int | None = None,
        rollback: bool = False,
    ) -> list[QueryResult]:
        """Run several statements atomically; any failure rolls the whole batch back.

        ``rollback=True`` forces a rollback even on success, which makes this a
        dry run: useful for validating a migration without applying it.
        """
        if not statements:
            raise QueryExecutionError("no statements were supplied")
        if self.access_mode is not AccessMode.WRITE:
            raise AccessDeniedError(
                "transactions are only available in write access mode; "
                "start the server with --access-mode=write"
            )
        analysed: list[StatementInfo] = []
        for statement in statements:
            self._check_length(statement)
            analysed.append(_apply_policy(analyze(statement), allow_writes=True))

        effective_limit = self._clamp_limit(limit)
        results: list[QueryResult] = []
        pool = await self._pool_for(database)
        timeout = timeout_ms if timeout_ms is not None else self.settings.statement_timeout_ms
        async with pool.connection() as conn:
            try:
                async with conn.transaction(force_rollback=rollback):
                    await conn.execute(f"SET LOCAL statement_timeout = {int(timeout)}")
                    await conn.execute(f"SET LOCAL lock_timeout = {int(min(timeout, 10_000))}")
                    for info in analysed:
                        started = time.perf_counter()
                        cursor = await conn.execute(info.sql)
                        columns = [d.name for d in cursor.description] if cursor.description else []
                        rows = list(await cursor.fetchmany(effective_limit)) if cursor.description else []
                        results.append(
                            QueryResult(
                                columns=columns,
                                rows=rows,
                                row_count=len(rows),
                                command_status=cursor.statusmessage,
                                affected_rows=cursor.rowcount if cursor.rowcount >= 0 else None,
                                duration_ms=(time.perf_counter() - started) * 1000,
                                database=database or self._default_database,
                                read_only=False,
                            )
                        )
            except psycopg.Error as exc:
                raise self._wrap(exc) from exc
        return results

    async def execute_autocommit(
        self,
        query: SQLQuery,
        params: Sequence[Any] | dict[str, Any] | None = None,
        *,
        database: str | None = None,
        timeout_ms: int | None = None,
    ) -> QueryResult:
        """Run a statement outside a transaction block.

        Some statements like VACUUM and CREATE INDEX CONCURRENTLY cannot run
        inside a transaction.  This method borrows a connection, sets autocommit
        mode, runs the statement, and returns. Any statement passed here should
        already be validated by the caller.

        In psycopg3, autocommit mode means each statement is committed
        immediately and not wrapped in an implicit transaction.
        """
        started = time.perf_counter()
        pool = await self._pool_for(database)
        timeout = timeout_ms if timeout_ms is not None else self.settings.statement_timeout_ms
        notices: list[str] = []

        async with pool.connection() as conn:
            # Set autocommit mode to allow VACUUM, CREATE INDEX CONCURRENTLY, etc.
            await conn.set_autocommit(True)
            conn.add_notice_handler(lambda diag: notices.append(str(diag.message_primary or "").strip()))
            try:
                await conn.execute(f"SET statement_timeout = {int(timeout)}")
                cursor = await conn.execute(query, params)
            except psycopg.Error as exc:
                raise self._wrap(exc) from exc
            finally:
                # Reset autocommit mode for the pooled connection
                await conn.set_autocommit(False)

            columns = [d.name for d in cursor.description] if cursor.description else []
            rows: list[dict[str, Any]] = []
            if cursor.description:
                rows = list(await cursor.fetchall())

            return QueryResult(
                columns=columns,
                rows=rows,
                row_count=len(rows),
                command_status=cursor.statusmessage,
                affected_rows=cursor.rowcount if cursor.rowcount >= 0 else None,
                duration_ms=(time.perf_counter() - started) * 1000,
                notices=[n for n in notices if n],
                database=database or self._default_database,
                read_only=False,
            )

    # -- helpers ------------------------------------------------------------

    def _guard(self, sql: str) -> StatementInfo:
        """Apply the access-mode policy to client SQL."""
        if self.access_mode is AccessMode.WRITE:
            return _apply_policy(analyze(sql), allow_writes=True)
        return assert_read_only(sql)

    def _check_length(self, sql: str) -> None:
        if len(sql) > self.settings.max_query_length:
            raise QueryExecutionError(
                f"the statement is {len(sql)} characters, over the {self.settings.max_query_length} "
                "character limit (PGMCP_MAX_QUERY_LENGTH)"
            )

    def _clamp_limit(self, limit: int | None) -> int:
        """Bound a caller-supplied row limit by the server's configured ceiling."""
        if limit is None:
            return min(self.settings.default_row_limit, self.settings.max_rows)
        if limit < 1:
            return 1
        return min(limit, self.settings.max_rows)
