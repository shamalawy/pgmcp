"""Mutating tools: INSERT/UPDATE/DELETE, DDL, VACUUM, ANALYZE, index operations.

These tools are only available when access_mode is 'write'. Every tool includes
destructive_hint=True so clients can prompt the user for confirmation.
"""

from __future__ import annotations

import hashlib
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations
from psycopg import sql as pg_sql

from ..config import AccessMode, Settings
from ..errors import AccessDeniedError, QueryExecutionError
from ._helpers import as_json, db, ident, show, tool, validate_schema

__all__ = ["register"]

WRITE_ANNOTATIONS = ToolAnnotations(read_only_hint=False, destructive_hint=True, idempotent_hint=False)


def register(mcp: MCPServer, settings: Settings) -> None:
    """Register mutating tools on the server."""
    if settings.access_mode is not AccessMode.WRITE:
        raise AccessDeniedError(
            "write tools require access_mode='write'; call them only when write access is enabled"
        )

    @mcp.tool(annotations=WRITE_ANNOTATIONS)
    @tool
    async def execute_sql(
        sql: str,
        params: list[str] | None = None,
        limit: int | None = None,
        format: str | None = None,
        database: str | None = None,
        timeout_ms: int | None = None,
    ) -> str:
        """Run a single INSERT/UPDATE/DELETE/MERGE/DDL statement and return affected rows.

        **WARNING: This modifies the database. The caller should confirm with the user first.**
        An UPDATE or DELETE without a WHERE clause modifies every row.

        Use `execute_transaction` with `dry_run=true` to validate a statement before running
        it, or to inspect the result without persisting changes.

        Args:
            sql: The SQL statement to execute (INSERT/UPDATE/DELETE/MERGE/DDL).
            params: Optional list of bind parameters, in the order they appear in the SQL.
            limit: Maximum rows to return from RETURNING clauses. Server cap applies.
            format: "json" (default), "markdown", or "csv".
            database: Target database. Defaults to the configured database.
            timeout_ms: Query timeout in milliseconds. Defaults to statement_timeout_ms.
        """
        # Convert params from list to tuple for psycopg
        param_tuple = tuple(params) if params else None
        result = await db().run_user_query(
            sql, param_tuple, database=database, limit=limit, timeout_ms=timeout_ms
        )
        return show(result, format, settings)

    @mcp.tool(annotations=WRITE_ANNOTATIONS)
    @tool
    async def execute_transaction(
        statements: list[str],
        dry_run: bool = False,
        database: str | None = None,
        timeout_ms: int | None = None,
        limit: int | None = None,
        format: str | None = None,
    ) -> str:
        """Run multiple SQL statements atomically in one transaction.

        All statements succeed or all roll back together. `dry_run=True` executes
        everything and then rolls back regardless of success — the safe way to
        validate a migration or complex update without applying it.

        Returns a per-statement summary (statement, command status, affected rows,
        duration). The output clearly indicates whether the transaction was
        committed or rolled back.

        Args:
            statements: List of SQL statements to execute.
            dry_run: If true, roll back even on success. Defaults to false.
            database: Target database. Defaults to the configured database.
            timeout_ms: Query timeout in milliseconds per statement.
            limit: Maximum rows per statement to return from RETURNING clauses.
            format: "json" (default), "markdown", or "csv".
        """
        results = await db().run_transaction(
            statements, database=database, timeout_ms=timeout_ms, limit=limit, rollback=dry_run
        )

        # Build a summary that includes transaction status
        summary: dict[str, Any] = {
            "statements": len(results),
            "committed": not dry_run,
            "transaction_status": "rolled back" if dry_run else "committed",
            "statements_detail": [],
        }
        for i, result in enumerate(results):
            stmt_detail: dict[str, Any] = {
                "index": i,
                "command": result.command_status or "",
                "affected_rows": result.affected_rows,
                "rows_returned": result.row_count,
                "duration_ms": result.duration_ms,
            }
            if result.rows:
                stmt_detail["rows"] = result.rows
            summary["statements_detail"].append(stmt_detail)

        return as_json(summary, settings)

    @mcp.tool(annotations=WRITE_ANNOTATIONS)
    @tool
    async def analyze_table(
        table_name: str | None = None, schema_name: str = "public", verbose: bool = False
    ) -> str:
        """Refresh planner statistics for one table or the entire database.

        ANALYZE examines table contents and updates pg_statistic so the query
        planner can make better decisions. This is a low-impact maintenance
        operation: it holds only a light share lock and does not block writes.

        Locks: Acquires an AccessShareLock (shareable with other readers). Does not
        block writes or reads.
        Duration: Proportional to table size; typically completes in seconds.

        Args:
            table_name: Name of the table. If omitted, analyzes the entire database.
            schema_name: Schema of the table. Defaults to "public". Ignored if table_name is omitted.
            verbose: If true, returns planner statistics (verbose output).
        """
        if table_name:
            schema_name = validate_schema(schema_name, settings)
            # Check that the table exists by binding it to to_regclass
            regclass_arg = _make_regclass_arg(schema_name, table_name)
            exists = await db().fetch_one("SELECT to_regclass(%s) AS relid", (regclass_arg,))
            if not exists or not exists.get("relid"):
                raise QueryExecutionError(f"table {schema_name!r}.{table_name!r} does not exist")

            stmt_parts = ["ANALYZE", f" {ident(schema_name, table_name).as_string(None)}"]
            if verbose:
                stmt_parts.append(" VERBOSE")
            stmt_str = "".join(stmt_parts)
        else:
            stmt_str = "ANALYZE VERBOSE" if verbose else "ANALYZE"

        result = await db().run_user_query(stmt_str)
        return show(result, None, settings)

    @mcp.tool(annotations=WRITE_ANNOTATIONS)
    @tool
    async def vacuum_table(
        table_name: str | None = None,
        schema_name: str = "public",
        full: bool = False,
        analyze: bool = True,
        verbose: bool = False,
    ) -> str:
        """Reclaim disk space and optionally refresh statistics.

        VACUUM removes dead rows created by UPDATE/DELETE and marks space as
        reusable. VACUUM FULL rewrites the entire table (expensive but reclaims
        the most space). When analyze=True, also refreshes planner statistics.

        **WARNING for VACUUM FULL:** It acquires an ACCESS EXCLUSIVE lock,
        blocking all access for the duration. It requires free disk space equal
        to the table size. Only use when the database is offline or the table is
        quiet, and only after confirming available disk space.

        Locks:
        - VACUUM (incremental): ShareUpdateExclusiveLock (allows reads and writes).
        - VACUUM FULL: ACCESS EXCLUSIVE lock (blocks all access).
        Duration: VACUUM is fast; VACUUM FULL is proportional to table size.

        Args:
            table_name: Name of the table. If omitted, vacuums the entire database.
            schema_name: Schema of the table. Defaults to "public".
            full: If true, run VACUUM FULL (expensive, reclaims most space).
            analyze: If true, also run ANALYZE to refresh statistics. Defaults to true.
            verbose: If true, return verbose output.
        """
        if table_name:
            schema_name = validate_schema(schema_name, settings)
            # Check that the table exists
            regclass_arg = _make_regclass_arg(schema_name, table_name)
            exists = await db().fetch_one("SELECT to_regclass(%s) AS relid", (regclass_arg,))
            if not exists or not exists.get("relid"):
                raise QueryExecutionError(f"table {schema_name!r}.{table_name!r} does not exist")

            stmt_parts = ["VACUUM"]
            if full:
                stmt_parts.append(" FULL")
            if analyze:
                stmt_parts.append(" ANALYZE")
            if verbose:
                stmt_parts.append(" VERBOSE")
            stmt_parts.append(f" {ident(schema_name, table_name).as_string(None)}")
            stmt_str = "".join(stmt_parts)
        else:
            stmt_parts = ["VACUUM"]
            if full:
                stmt_parts.append(" FULL")
            if analyze:
                stmt_parts.append(" ANALYZE")
            if verbose:
                stmt_parts.append(" VERBOSE")
            stmt_str = "".join(stmt_parts)

        # VACUUM cannot run in a transaction block, so use autocommit
        result = await db().execute_autocommit(stmt_str)
        return show(result, None, settings)

    @mcp.tool(annotations=WRITE_ANNOTATIONS)
    @tool
    async def create_index(
        table_name: str,
        columns: list[str],
        schema_name: str = "public",
        index_name: str | None = None,
        unique: bool = False,
        method: str = "btree",
        concurrently: bool = True,
        where: str | None = None,
    ) -> str:
        """Create an index on one or more columns.

        Validates every column against the catalog before building the DDL, so a
        typo gives a clear error. By default, creates the index CONCURRENTLY,
        which does not block writes but cannot run inside a transaction.

        Index names are generated deterministically from the table, columns, and
        options if not supplied, ensuring reproducibility.

        Locks:
        - CONCURRENTLY (default): ShareUpdateExclusiveLock (allows reads/writes).
        - Without CONCURRENTLY: AccessExclusiveLock (blocks all access).
        Duration: Proportional to table size; CONCURRENTLY is slower but non-blocking.

        Args:
            table_name: Name of the table.
            columns: List of column names (in order).
            schema_name: Schema of the table. Defaults to "public".
            index_name: Name of the index. Auto-generated if omitted.
            unique: If true, create a unique index (NULL values are distinct).
            method: Index method: "btree" (default), "hash", "gist", "gin", "brin", "spgist".
            concurrently: If true, create CONCURRENTLY (does not block writes).
            where: Optional WHERE clause to create a partial index. The clause is validated by
                PostgreSQL's parser; invalid expressions cause the tool to error.
        """
        schema_name = validate_schema(schema_name, settings)

        # Validate method
        allowed_methods = {"btree", "hash", "gist", "gin", "brin", "spgist"}
        method = method.strip().lower()
        if method not in allowed_methods:
            raise QueryExecutionError(
                f"index method {method!r} is not supported; use one of: {', '.join(sorted(allowed_methods))}"
            )

        # Check that the table exists
        regclass_arg = _make_regclass_arg(schema_name, table_name)
        exists = await db().fetch_one("SELECT to_regclass(%s) AS relid", (regclass_arg,))
        if not exists or not exists.get("relid"):
            raise QueryExecutionError(f"table {schema_name!r}.{table_name!r} does not exist")

        # Validate all columns exist
        if not columns or not all(columns):
            raise QueryExecutionError("at least one column name is required")

        column_result = await db().fetch_all(
            "SELECT a.attname FROM pg_attribute a "
            "JOIN pg_class c ON a.attrelid = c.oid "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped",
            (schema_name, table_name),
        )
        valid_cols = {row["attname"] for row in column_result}
        for col in columns:
            if col not in valid_cols:
                raise QueryExecutionError(
                    f"column {col!r} does not exist on table {schema_name!r}.{table_name!r}"
                )

        # Validate WHERE clause if provided
        if where:
            where = where.strip()
            if not where:
                raise QueryExecutionError("WHERE clause cannot be empty")
            # Reject dangerous patterns that could break out of the WHERE clause.
            # Semicolons allow multiple statements; -- and /* */ allow SQL comments
            # and statement truncation. These are not valid in WHERE clauses anyway.
            dangerous_patterns = [
                (";", "semicolon — would allow multiple statements"),
                ("--", "double-dash comment"),
                ("/*", "block comment start"),
                ("*/", "block comment end"),
            ]
            where_upper = where.upper()
            for pattern, reason in dangerous_patterns:
                if pattern in where:
                    raise QueryExecutionError(f"WHERE clause contains {reason} and will be rejected")
            # Also reject leading dangerous keywords
            dangerous_keywords = {
                "SELECT",
                "INSERT",
                "UPDATE",
                "DELETE",
                "DROP",
                "CREATE",
                "ALTER",
                "GRANT",
                "REVOKE",
                "TRUNCATE",
                "BEGIN",
                "COMMIT",
                "ROLLBACK",
                "WITH",
            }
            first_word = where_upper.split()[0] if where_upper.split() else ""
            if first_word in dangerous_keywords:
                raise QueryExecutionError(f"WHERE clause cannot start with {first_word!r}")

        # Generate index name if not provided
        if not index_name:
            # Deterministic: hash the key parameters
            key_parts = [schema_name, table_name, ",".join(columns), str(unique), method]
            if where:
                key_parts.append(where)
            digest = hashlib.sha256("|".join(key_parts).encode()).hexdigest()[:8]
            index_name = f"idx_{table_name}_{digest}"

        # Build the CREATE INDEX statement
        stmt_parts = ["CREATE"]
        if unique:
            stmt_parts.append(" UNIQUE")
        stmt_parts.append(" INDEX")
        if concurrently:
            stmt_parts.append(" CONCURRENTLY")
        stmt_parts.append(f" {ident(index_name).as_string(None)}")
        stmt_parts.append(f" ON {ident(schema_name, table_name).as_string(None)}")
        stmt_parts.append(f" USING {method}")
        stmt_parts.append(" (")

        # Add columns
        col_list = ", ".join(ident(col).as_string(None) for col in columns)
        stmt_parts.append(col_list)
        stmt_parts.append(")")

        # Add WHERE clause if provided
        if where:
            stmt_parts.append(f" WHERE {where}")

        stmt_str = "".join(stmt_parts)

        # CREATE INDEX CONCURRENTLY cannot run in a transaction, so use autocommit
        if concurrently:
            result = await db().execute_autocommit(stmt_str)
        else:
            result = await db().run_user_query(stmt_str)

        return show(result, None, settings)

    @mcp.tool(annotations=WRITE_ANNOTATIONS)
    @tool
    async def drop_index(
        index_name: str, schema_name: str = "public", concurrently: bool = True, if_exists: bool = True
    ) -> str:
        """Drop an index, with safety checks.

        Refuses to drop an index that backs a constraint (PRIMARY KEY, UNIQUE,
        or EXCLUSION). The constraint must be dropped instead.

        Locks:
        - CONCURRENTLY (default): ShareUpdateExclusiveLock (allows reads/writes).
        - Without CONCURRENTLY: AccessExclusiveLock (blocks all access).
        Duration: Usually fast; CONCURRENTLY is slower.

        Args:
            index_name: Name of the index.
            schema_name: Schema of the index. Defaults to "public".
            concurrently: If true, drop CONCURRENTLY (does not block writes).
            if_exists: If true, do not error if the index does not exist.
        """
        schema_name = validate_schema(schema_name, settings)

        # Check if the index exists and is backing a constraint
        constraint_check = await db().fetch_one(
            "SELECT conname FROM pg_constraint c "
            "JOIN pg_index i ON c.conindid = i.indexrelid "
            "JOIN pg_class idx ON i.indexrelid = idx.oid "
            "JOIN pg_namespace n ON idx.relnamespace = n.oid "
            "WHERE idx.relname = %s AND n.nspname = %s",
            (index_name, schema_name),
        )

        if constraint_check:
            raise QueryExecutionError(
                f"index {schema_name!r}.{index_name!r} backs constraint {constraint_check['conname']!r}; "
                "drop the constraint instead"
            )

        # Build the DROP INDEX statement
        stmt_parts = ["DROP INDEX"]
        if concurrently:
            stmt_parts.append(" CONCURRENTLY")
        if if_exists:
            stmt_parts.append(" IF EXISTS")
        stmt_parts.append(f" {ident(schema_name, index_name).as_string(None)}")

        stmt_str = "".join(stmt_parts)

        # DROP INDEX CONCURRENTLY cannot run in a transaction, so use autocommit
        if concurrently:
            result = await db().execute_autocommit(stmt_str)
        else:
            result = await db().run_user_query(stmt_str)

        return show(result, None, settings)

    @mcp.tool(annotations=WRITE_ANNOTATIONS)
    @tool
    async def refresh_materialized_view(
        view_name: str, schema_name: str = "public", concurrently: bool = False
    ) -> str:
        """Refresh a materialized view with current data.

        REFRESH MATERIALIZED VIEW re-queries the view's definition and
        overwrites its data. CONCURRENTLY uses a unique index to perform an
        incremental refresh; if a unique index is not present, the tool returns
        a clear error naming the requirement.

        Locks:
        - Without CONCURRENTLY: AccessExclusiveLock (blocks all access to the view).
        - With CONCURRENTLY: ShareUpdateExclusiveLock (allows SELECT).
        Duration: Proportional to the complexity and size of the view definition.

        Args:
            view_name: Name of the materialized view.
            schema_name: Schema of the view. Defaults to "public".
            concurrently: If true, refresh CONCURRENTLY (requires a unique index).
        """
        schema_name = validate_schema(schema_name, settings)

        # Check that the materialized view exists
        exists = await db().fetch_one(
            "SELECT relkind FROM pg_class c "
            "JOIN pg_namespace n ON c.relnamespace = n.oid "
            "WHERE n.nspname = %s AND c.relname = %s",
            (schema_name, view_name),
        )
        if not exists:
            raise QueryExecutionError(f"materialized view {schema_name!r}.{view_name!r} does not exist")
        if exists.get("relkind") != "m":
            raise QueryExecutionError(
                f"{schema_name!r}.{view_name!r} is not a materialized view (relkind={exists.get('relkind')})"
            )

        # If CONCURRENTLY, check for a unique index
        if concurrently:
            unique_index = await db().fetch_one(
                "SELECT idx.relname AS indexname FROM pg_index i "
                "JOIN pg_class idx ON i.indexrelid = idx.oid "
                "WHERE i.indrelid = (SELECT oid FROM pg_class c "
                "  JOIN pg_namespace n ON c.relnamespace = n.oid "
                "  WHERE n.nspname = %s AND c.relname = %s) "
                "AND i.indisunique LIMIT 1",
                (schema_name, view_name),
            )
            if not unique_index:
                raise QueryExecutionError(
                    f"cannot refresh materialized view {schema_name!r}.{view_name!r} CONCURRENTLY: "
                    "a unique index is required. Create one with CREATE UNIQUE INDEX, then retry."
                )

        # Build the REFRESH MATERIALIZED VIEW statement
        stmt_parts = ["REFRESH MATERIALIZED VIEW"]
        if concurrently:
            stmt_parts.append(" CONCURRENTLY")
        stmt_parts.append(f" {ident(schema_name, view_name).as_string(None)}")

        stmt_str = "".join(stmt_parts)

        result = await db().run_user_query(stmt_str)
        return show(result, None, settings)


def _make_regclass_arg(schema: str, table: str) -> str:
    """Format a schema.table name for to_regclass() parameter binding."""
    # to_regclass accepts a text parameter and parses it, so we quote the parts
    # to preserve case and special characters.
    return pg_sql.Identifier(schema, table).as_string(None)
