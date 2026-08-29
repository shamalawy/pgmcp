"""Tools for running SELECT queries, EXPLAIN, sampling, and analyzing tables.

These tools let the model execute read-only queries against the database,
explore query plans, preview table contents, and analyse column statistics.
"""

from __future__ import annotations

import json
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations
from psycopg import sql as pg_sql

from ..config import OutputFormat, Settings
from ..errors import QueryExecutionError
from ..sql_guard import validate_fragment
from ._helpers import (
    as_json,
    db,
    ident,
    list_param,
    regclass,
    resolve_format,
    schema_predicate,
    show,
    tool,
    validate_schema,
)

__all__ = ["register"]

# All query tools are read-only and idempotent.
READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True)


def register(mcp: MCPServer, settings: Settings) -> None:
    """Register the query toolset on the server."""

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def query(
        sql: str,
        params: list[str] | None = None,
        limit: int | None = None,
        format: str | None = None,
        database: str | None = None,
        timeout_ms: int | None = None,
    ) -> str:
        """Run a SELECT query and return results.

        Execute a single read-only SQL statement. Only SELECT, TABLE, VALUES, SHOW
        and read-only EXPLAIN are permitted; multiple statements and writes are
        refused. Results are capped at the server limit; use LIMIT/OFFSET in the
        query to page through large result sets.

        Important: ``params`` are positional values for ``%s`` placeholders, and
        arrive as strings from the client. When the target column is not text, cast
        in the SQL: ``WHERE id = %s::bigint`` or ``WHERE price > %s::numeric``.

        Args:
            sql: A single read-only SQL statement (SELECT, TABLE, VALUES, etc).
            params: List of values for %s placeholders in the SQL, as strings.
            limit: Maximum rows to return (server cap applies). Defaults to the
                   configured default limit.
            format: Output format: "json" (default), "markdown", or "csv".
            database: Target database name. Defaults to the DSN's database.
            timeout_ms: Query timeout in milliseconds. Defaults to server setting.
        """
        return show(
            await db().run_user_query(
                sql,
                params,
                database=database,
                limit=limit,
                timeout_ms=timeout_ms,
            ),
            format,
            settings,
        )

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def explain_query(
        sql: str,
        analyze: bool = False,
        buffers: bool = False,
        verbose: bool = False,
        costs: bool = True,
        format: str | None = None,
        database: str | None = None,
        timeout_ms: int | None = None,
    ) -> str:
        """Run EXPLAIN to inspect a query plan.

        Show how PostgreSQL will or did execute a statement. With ``analyze=True``,
        the statement is actually executed and real timing/row counts are included;
        in read-only access mode this runs inside a read-only transaction and is
        safe. The JSON plan is always returned by default; pass ``format="markdown"``
        to also see a human-readable text plan with highlighted metrics.

        Args:
            sql: A single SQL statement to explain (SELECT, INSERT, UPDATE, etc).
            analyze: Run the statement for real and measure actual execution. With
                     ``analyze=True``, read-only statements run in a read-only
                     transaction; write statements are still refused in read-only mode.
            buffers: Include buffer statistics (requires analyze=True).
            verbose: Show additional output details (node type, etc).
            costs: Include planner cost estimates (default true). Set false to hide
                   estimated costs if they are distracting.
            format: "json" (default, nested plan structure) or "markdown" (human
                    text plan with summary metrics).
            database: Target database name. Defaults to the DSN's database.
            timeout_ms: Query timeout in milliseconds. Defaults to server setting.
        """
        # Build EXPLAIN option list from the boolean flags.
        options: list[str] = []
        if analyze:
            options.append("ANALYZE")
        if buffers:
            options.append("BUFFERS")
        if verbose:
            options.append("VERBOSE")
        if not costs:
            options.append("COSTS OFF")
        options.append("FORMAT JSON")

        explain_sql = f"EXPLAIN ({', '.join(options)}) {sql}"

        result = await db().run_user_query(
            explain_sql,
            None,
            database=database,
            limit=None,
            timeout_ms=timeout_ms,
        )

        # Parse the JSON plan from the first row.
        if not result.rows:
            return as_json({"error": "EXPLAIN returned no output"}, settings)

        plan_json_str = result.rows[0].get("QUERY PLAN")
        if not plan_json_str:
            return as_json({"error": "EXPLAIN returned no QUERY PLAN column"}, settings)

        try:
            plan = json.loads(plan_json_str) if isinstance(plan_json_str, str) else plan_json_str
        except (json.JSONDecodeError, TypeError) as exc:
            return as_json(
                {"error": f"Failed to parse EXPLAIN JSON: {exc}"},
                settings,
            )

        fmt = resolve_format(format, settings)

        if fmt == OutputFormat.MARKDOWN:
            # Return markdown with plan text summary.
            text_result = await db().run_user_query(
                f"EXPLAIN ({', '.join(o for o in options if o != 'FORMAT JSON')}) {sql}",
                None,
                database=database,
                limit=None,
                timeout_ms=timeout_ms,
            )

            summary_lines = []
            if text_result.rows:
                plan_text = "\n".join(row.get("QUERY PLAN", "") for row in text_result.rows if row)
                summary_lines.append("## Plan\n```\n" + plan_text + "\n```")

            # Extract key metrics from the JSON plan.
            summary = _extract_plan_summary(plan)
            if summary:
                summary_lines.append("\n## Summary\n" + summary)

            output = "\n\n".join(summary_lines)
            return output if output else as_json(plan, settings)

        # JSON format: return the parsed plan.
        return as_json(plan, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def sample_table(
        table_name: str,
        schema_name: str = "public",
        limit: int = 10,
        columns: list[str] | None = None,
        where: str | None = None,
        order_by: str | None = None,
        random: bool = False,
        format: str | None = None,
    ) -> str:
        """Preview rows from a table.

        Fetch and display a sample of rows to explore table contents, column names,
        and data types. The ``where`` and ``order_by`` arguments accept raw SQL
        fragments and are validated for safety before execution. Unknown column
        names are rejected with a helpful error listing the valid columns.

        Args:
            table_name: The table to sample from.
            schema_name: Schema containing the table (default "public").
            limit: Maximum rows to return (default 10, server cap applies).
            columns: Specific columns to fetch. If None, all columns. Validated
                     against the actual table schema.
            where: Optional WHERE fragment, e.g. "created_at > now() - interval '1 day'".
                   Must be a single self-contained expression: subqueries, UNION and
                   extra clauses are refused. Use the `query` tool for those.
                   Raw SQL, validated before use.
            order_by: Optional ORDER BY fragment, e.g. "created_at DESC". Same
                      restriction as `where`: a bare expression, no subqueries.
            random: Randomize row order with ORDER BY random(). Warning: on large
                    tables this is a full table scan and can be slow.
            format: Output format: "json" (default), "markdown", or "csv".
        """
        schema = validate_schema(schema_name, settings)

        # Fetch the actual column list from the catalog.
        col_query = pg_sql.SQL(
            """
            SELECT attname
            FROM pg_attribute
            WHERE attrelid = to_regclass(%s)
              AND attnum > 0
              AND NOT attisdropped
            ORDER BY attnum
            """
        )
        col_result = await db().fetch_all(col_query, [regclass(schema, table_name)], timeout_ms=None)
        if not col_result:
            raise QueryExecutionError(f"table {schema}.{table_name} not found or has no columns")

        actual_columns = [row["attname"] for row in col_result]

        # Validate requested columns against the actual columns.
        requested_cols = list_param(columns) or actual_columns
        unknown = [c for c in requested_cols if c not in actual_columns]
        if unknown:
            raise QueryExecutionError(
                f"unknown column(s): {', '.join(unknown)}. Valid columns: {', '.join(actual_columns)}"
            )

        # Build the SELECT with proper identifier quoting using psycopg.
        # We build as pg_sql.SQL to properly escape quotes in identifiers like 'a"b'.
        col_idents = pg_sql.SQL(", ").join(ident(c) for c in requested_cols)
        query_obj = pg_sql.SQL("SELECT {} FROM {}").format(
            col_idents,
            ident(schema, table_name),
        )

        # Convert to string for run_user_query validation. The fragments are
        # validated separately first: the statement guard would happily accept
        # `true UNION SELECT ... FROM other.table`, which reads straight past the
        # schema allow-list. validate_fragment holds them to a bare expression.
        query_str = query_obj.as_string(None)
        if where:
            query_str += f" WHERE {validate_fragment(where, kind='WHERE')}"
        if random:
            query_str += " ORDER BY random()"
        elif order_by:
            query_str += f" ORDER BY {validate_fragment(order_by, kind='ORDER BY')}"

        # Use run_user_query so the guard validates the whole thing.
        result = await db().run_user_query(query_str, None, limit=limit, timeout_ms=None)
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def count_rows(
        table_name: str,
        schema_name: str = "public",
        where: str | None = None,
        exact: bool = True,
        database: str | None = None,
    ) -> str:
        """Count rows in a table, exactly or by estimate.

        Return either an exact count via COUNT(*) or a fast estimate from
        ``pg_class.reltuples``. The exact count can be slow on very large tables;
        use ``exact=False`` in that case to get the planner's estimate instead.

        Returns both the count/estimate and the method used.

        Args:
            table_name: The table to count.
            schema_name: Schema containing the table (default "public").
            where: Optional WHERE fragment to count matching rows only. Must be a
                   single self-contained expression; subqueries and UNION are refused.
            exact: Count exactly with COUNT(*) (default true). Set false for a
                   fast reltuples estimate.
            database: Target database name. Defaults to the DSN's database.
        """
        schema = validate_schema(schema_name, settings)

        if exact:
            # Exact count via COUNT(*). Build query string for guard validation.
            count_query_str = f"SELECT count(*) as count FROM {ident(schema, table_name).as_string(None)}"
            if where:
                count_query_str += f" WHERE {validate_fragment(where, kind='WHERE')}"

            # Run through guard for validation.
            result = await db().run_user_query(
                count_query_str, None, database=database, limit=None, timeout_ms=None
            )
            count = result.rows[0]["count"] if result.rows else 0
            method = "exact COUNT(*)"
        else:
            # Fast estimate from pg_class.
            estimate_query = pg_sql.SQL(
                """
                SELECT reltuples::bigint as count
                FROM pg_class
                WHERE oid = to_regclass(%s)
                """
            )
            estimate_row = await db().fetch_one(
                estimate_query, [regclass(schema, table_name)], database=database, timeout_ms=None
            )
            count = int(estimate_row["count"]) if estimate_row and estimate_row["count"] is not None else 0
            method = "pg_class.reltuples estimate"

        return as_json({"count": count, "method": method}, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def column_stats(
        table_name: str,
        column_name: str,
        schema_name: str = "public",
        top_n: int = 10,
        format: str | None = None,
    ) -> str:
        """Profile a single column with statistics and live aggregates.

        Combine planner statistics from ``pg_stats`` (if available) with a live
        query to return null fraction, distinct counts, most common values,
        histogram bounds, and correlation. Also includes count, non-null count,
        distinct count, min, max, and for numeric columns, average and stddev.

        Returns the top N most common values with their frequencies.

        Args:
            table_name: The table containing the column.
            column_name: The column to profile.
            schema_name: Schema containing the table (default "public").
            top_n: Number of most common values to return (default 10).
            format: Output format: "json" (default), "markdown", or "csv".
        """
        schema = validate_schema(schema_name, settings)

        # Fetch pg_stats data (may be empty if table has not been ANALYZEd).
        pg_stats_query = pg_sql.SQL(
            """
            SELECT
              null_frac,
              n_distinct,
              avg_width,
              most_common_vals,
              most_common_freqs,
              histogram_bounds,
              correlation
            FROM pg_stats
            WHERE schemaname = %s
              AND tablename = %s
              AND attname = %s
            """
        )
        pg_stats_result = await db().fetch_one(
            pg_stats_query, [schema, table_name, column_name], timeout_ms=None
        )

        # Build a dynamic aggregate query to profile the column.
        col_ident = ident(schema, table_name, column_name).as_string(None)
        tbl_ident = ident(schema, table_name).as_string(None)

        # Use a generic approach: count total, count non-null, count distinct, min, max.
        # For numeric columns, also compute avg and stddev. We'll detect numeric type
        # from the catalog.
        type_query = pg_sql.SQL(
            """
            SELECT typname
            FROM pg_attribute
            JOIN pg_type ON pg_attribute.atttypid = pg_type.oid
            WHERE attrelid = to_regclass(%s)
              AND attname = %s
            """
        )
        type_result = await db().fetch_one(type_query, [regclass(schema, table_name), column_name])
        typname = type_result.get("typname") if type_result else None

        # Build the aggregate query.
        agg_parts = [
            "count(*) as total_count",
            f"count({col_ident}) as non_null_count",
            f"count(distinct {col_ident}) as distinct_count",
            f"min({col_ident}) as min_val",
            f"max({col_ident}) as max_val",
        ]

        # Add numeric aggregates if the type is numeric.
        if typname and typname in {"int2", "int4", "int8", "numeric", "float4", "float8"}:
            agg_parts.extend(
                [
                    f"avg({col_ident}) as avg_val",
                    f"stddev({col_ident}) as stddev_val",
                ]
            )

        agg_query_str = f"SELECT {', '.join(agg_parts)} FROM {tbl_ident}"
        agg_result = await db().fetch_one(pg_sql.SQL(agg_query_str), None)

        # Combine the results.
        output: dict[str, Any] = {
            "table": f"{schema}.{table_name}",
            "column": column_name,
            "column_type": typname,
            "planner_statistics": {
                "null_frac": pg_stats_result.get("null_frac") if pg_stats_result else None,
                "n_distinct": pg_stats_result.get("n_distinct") if pg_stats_result else None,
                "avg_width": pg_stats_result.get("avg_width") if pg_stats_result else None,
                "correlation": pg_stats_result.get("correlation") if pg_stats_result else None,
            },
            "live_aggregates": {
                "total_count": agg_result.get("total_count") if agg_result else 0,
                "non_null_count": agg_result.get("non_null_count") if agg_result else 0,
                "distinct_count": agg_result.get("distinct_count") if agg_result else 0,
                "min": agg_result.get("min_val") if agg_result else None,
                "max": agg_result.get("max_val") if agg_result else None,
                "avg": agg_result.get("avg_val") if agg_result else None,
                "stddev": agg_result.get("stddev_val") if agg_result else None,
            },
            "most_common_values": [],
        }

        # Extract most common values from pg_stats if available.
        if pg_stats_result:
            most_common_vals = pg_stats_result.get("most_common_vals")
            most_common_freqs = pg_stats_result.get("most_common_freqs")
            if most_common_vals and most_common_freqs:
                # These are arrays; parse them.
                try:
                    vals_parsed = (
                        most_common_vals
                        if isinstance(most_common_vals, list)
                        else json.loads(most_common_vals)
                    )
                    freqs_parsed = (
                        most_common_freqs
                        if isinstance(most_common_freqs, list)
                        else json.loads(most_common_freqs)
                    )
                    for i, (val, freq) in enumerate(zip(vals_parsed, freqs_parsed, strict=False)):
                        if i >= top_n:
                            break
                        output["most_common_values"].append({"value": val, "frequency": freq})
                except (json.JSONDecodeError, TypeError):
                    pass

        return as_json(output, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def table_sizes(
        schema_name: str | None = None,
        limit: int = 20,
        format: str | None = None,
    ) -> str:
        """List the largest relations by total size.

        Show which tables and indexes consume the most storage. Returns schema,
        table name, row count estimate, table size, index size, toast size, and
        total size for each relation, in descending order by total size.

        Args:
            schema_name: Restrict to one schema. If None, all visible schemas.
            limit: Maximum relations to return (default 20).
            format: Output format: "json" (default), "markdown", or "csv".
        """
        if schema_name:
            validate_schema(schema_name, settings)

        where_clause = ""
        params: list[Any] = []
        if schema_name:
            where_clause = "WHERE schemaname = %s"
            params = [schema_name]
        else:
            where_fragment, schema_params = schema_predicate("schemaname", settings)
            where_clause = f"WHERE true {where_fragment}"
            params = schema_params

        # Query to compute relation sizes.
        # Build size expressions for total, table, index, and toast.
        total_size = "pg_total_relation_size(schemaname || '.' || tablename)"
        table_size = "pg_relation_size(schemaname || '.' || tablename)"
        index_size = "pg_indexes_size(schemaname || '.' || tablename)"
        toast_size = f"{total_size} - {table_size} - {index_size}"

        size_query = pg_sql.SQL(
            f"""
            SELECT
              schemaname,
              tablename,
              pg_size_pretty({total_size}) as total_size_pretty,
              {total_size} as total_size_bytes,
              pg_size_pretty({table_size}) as table_size_pretty,
              {table_size} as table_size_bytes,
              pg_size_pretty({index_size}) as index_size_pretty,
              {index_size} as index_size_bytes,
              pg_size_pretty({toast_size}) as toast_size_pretty,
              {toast_size} as toast_size_bytes,
              (SELECT n_live_tup::bigint FROM pg_stat_user_tables t
               WHERE t.relname = tablename
                 AND t.schemaname = pg_tables.schemaname) as row_count_estimate
            FROM pg_tables
            {where_clause}
            ORDER BY {total_size} DESC
            LIMIT %s
            """
        )

        result = await db().fetch(size_query, [*params, limit], limit=limit, timeout_ms=None)
        return show(result, format, settings)


def _extract_plan_summary(plan: Any) -> str:
    """Extract key metrics from an EXPLAIN JSON plan for markdown output."""
    lines = []

    if not isinstance(plan, list) or not plan:
        return ""

    root = plan[0] if isinstance(plan, list) else plan
    if not isinstance(root, dict):
        return ""

    # Extract top-level plan metrics.
    total_cost = root.get("Total Cost")
    if total_cost:
        lines.append(f"- **Total Cost:** {total_cost}")

    plan_rows = root.get("Plan Rows")
    if plan_rows:
        lines.append(f"- **Estimated Rows:** {plan_rows}")

    actual_rows = root.get("Actual Rows")
    if actual_rows:
        lines.append(f"- **Actual Rows:** {actual_rows}")

    actual_time = root.get("Actual Total Time")
    if actual_time:
        lines.append(f"- **Actual Time:** {actual_time}ms")

    # Look for large estimate-vs-actual mismatches.
    if actual_rows is not None and plan_rows:
        try:
            actual = float(actual_rows)
            estimated = float(plan_rows)
            if estimated > 0 and actual > estimated * 10:
                lines.append(f"⚠️ **Large estimate mismatch:** estimated {estimated} but got {actual} rows")
        except (TypeError, ValueError):
            pass

    return "\n".join(lines) if lines else ""
