"""MCP prompts: user-invoked templates with embedded live schema context.

Prompts are slash-commands that embed schema/database context at call time
and provide guided instructions for common database tasks.
"""

from __future__ import annotations

from mcp.server.mcpserver import MCPServer

from .config import Settings
from .errors import SchemaNotAllowedError
from .tools._helpers import db, validate_schema

__all__ = ["register"]


def register(mcp: MCPServer, settings: Settings) -> None:
    """Register prompts on the server."""

    @mcp.prompt()
    async def write_sql(goal: str, schema_name: str = "public") -> str:
        """Write a correct PostgreSQL query for a given goal.

        Embeds the schema's tables and columns, then instructs the model to
        write a query that uses real column names, respects foreign keys,
        and runs it with the query tool.

        Args:
            goal: What you want to query or compute.
            schema_name: Schema to query. Defaults to "public".
        """
        try:
            validated_schema = validate_schema(schema_name, settings)
        except SchemaNotAllowedError as exc:
            return (
                f"Schema {schema_name!r} is not accessible. {exc}\n\n"
                f"Instead, use the tools to discover available schemas:\n"
                f"1. Call `list_schemas` to see what schemas exist.\n"
                f"2. Call `list_tables` for the schema you want to query.\n"
                f"3. Call `describe_table` to see column names and types.\n"
                f"4. Then write your query with the actual column names."
            )

        try:
            # Fetch schema structure
            tables_result = await db().fetch_all(
                """
                SELECT c.relname AS table_name
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relkind IN ('r', 'p')
                ORDER BY c.relname
                LIMIT 50
                """,
                [validated_schema],
            )

            table_names = [t["table_name"] for t in tables_result]

            # Fetch columns and FKs for each table
            schema_info = []
            for table_name in table_names[:30]:  # Cap tables in prompt
                cols_result = await db().fetch_all(
                    """
                    SELECT
                        a.attname,
                        format_type(a.atttypid, a.atttypmod) AS type,
                        a.attnotnull
                    FROM pg_attribute a
                    JOIN pg_class c ON c.oid = a.attrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                    AND a.attnum > 0 AND NOT a.attisdropped
                    ORDER BY a.attnum
                    LIMIT 20
                    """,
                    [validated_schema, table_name],
                )

                columns = [f"{c['attname']}: {c['type']}" for c in cols_result]
                schema_info.append(f"  {table_name}:\n    " + "\n    ".join(columns))

            schema_detail = "\n".join(schema_info) if schema_info else "  (no tables found)"

            return (
                f"You are writing a PostgreSQL query for the following goal:\n\n"
                f"{goal}\n\n"
                f"The {validated_schema!r} schema contains these tables and columns:\n\n"
                f"{schema_detail}\n\n"
                f"Instructions:\n"
                f"1. Write a correct PostgreSQL query that achieves the goal.\n"
                f"2. Use only the columns shown above; verify exact names and types.\n"
                f"3. Respect foreign key relationships: use JOIN instead of filtering by ID.\n"
                f"4. Run the query using the `query` tool with your SQL.\n"
                f"5. If the schema is incomplete or you need more details, call "
                f"`describe_table` for the relevant table.\n\n"
                f"Start by proposing your SQL query."
            )
        except Exception as exc:
            return (
                f"Could not load schema {validated_schema!r}: {exc}\n\n"
                f"Use the tools to discover the schema instead:\n"
                f"1. Call `list_tables` with schema_name={validated_schema!r}\n"
                f"2. Call `describe_table` for each table\n"
                f"3. Write your query based on the real column names you find"
            )

    @mcp.prompt()
    async def optimize_query(sql: str) -> str:
        """Optimize a PostgreSQL query for performance.

        Instructs the model to run EXPLAIN ANALYZE, check for issues like
        seq scans or missing indexes, and propose concrete improvements.

        Args:
            sql: The SQL query to optimize.
        """
        return (
            f"You are optimizing this PostgreSQL query:\n\n"
            f"```sql\n{sql}\n```\n\n"
            "Instructions:\n"
            f"1. Run `explain_query` with sql={sql!r} and analyze=true.\n"
            "2. Review the execution plan for:\n"
            "   - Seq Scans (should use indexes)\n"
            "   - Row estimate mismatches (hints show why joins are expensive)\n"
            "   - High-cost operations (sorts, aggregates)\n"
            "3. Call `index_usage` to see which indexes exist and their I/O.\n"
            "4. Call `table_stats` to understand table size and row count.\n"
            "5. Based on your findings, propose specific:\n"
            "   - CREATE INDEX statements to add missing indexes\n"
            "   - Query rewrites (e.g., subquery elimination, join reordering)\n"
            "   - Schema changes (if the model is the problem)\n"
            "6. Provide the DDL to create each new index or change.\n\n"
            "Prioritize fixes by impact: the biggest win first."
        )

    @mcp.prompt()
    async def explain_schema(schema_name: str = "public") -> str:
        """Explain the data model and relationships in a schema.

        Embeds tables, columns and foreign key relationships, then asks for
        a plain-language explanation of the schema design.

        Args:
            schema_name: Schema to explain. Defaults to "public".
        """
        try:
            validated_schema = validate_schema(schema_name, settings)
        except SchemaNotAllowedError as exc:
            return (
                f"Schema {schema_name!r} is not accessible. {exc}\n\n"
                f"Use `list_schemas` to find available schemas and pick a different one."
            )

        try:
            # Fetch tables and columns
            tables_result = await db().fetch_all(
                """
                SELECT c.relname AS table_name,
                       obj_description(c.oid, 'pg_class') AS description
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relkind IN ('r', 'p')
                ORDER BY c.relname
                LIMIT 50
                """,
                [validated_schema],
            )

            # Build schema description with FK relationships
            schema_lines = []
            for table_row in tables_result:
                table_name = table_row["table_name"]
                table_desc = table_row["description"] or ""

                # Fetch columns
                cols_result = await db().fetch_all(
                    """
                    SELECT
                        a.attname,
                        format_type(a.atttypid, a.atttypmod) AS type,
                        a.attnotnull,
                        col_description(c.oid, a.attnum) AS comment
                    FROM pg_attribute a
                    JOIN pg_class c ON c.oid = a.attrelid
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                    AND a.attnum > 0 AND NOT a.attisdropped
                    ORDER BY a.attnum
                    LIMIT 20
                    """,
                    [validated_schema, table_name],
                )

                # Fetch foreign keys
                fks_result = await db().fetch_all(
                    """
                    SELECT
                        a.attname AS from_column,
                        t.relname AS to_table,
                        b.attname AS to_column
                    FROM pg_constraint c
                    JOIN pg_class r ON r.oid = c.conrelid
                    JOIN pg_attribute a ON a.attrelid = r.oid AND a.attnum = ANY(c.conkey)
                    JOIN pg_class t ON t.oid = c.confrelid
                    JOIN pg_attribute b ON b.attrelid = t.oid AND b.attnum = ANY(c.confkey)
                    WHERE r.relnamespace = (
                        SELECT oid FROM pg_namespace WHERE nspname = %s
                    ) AND r.relname = %s AND c.contype = 'f'
                    """,
                    [validated_schema, table_name],
                )

                schema_lines.append(f"**{table_name}**")
                if table_desc:
                    schema_lines.append(f"  Description: {table_desc}")
                schema_lines.append("  Columns:")
                for col in cols_result:
                    nullable = "" if col["attnotnull"] else " (nullable)"
                    col_comment = f" - {col['comment']}" if col["comment"] else ""
                    schema_lines.append(f"    - {col['attname']}: {col['type']}{nullable}{col_comment}")
                if fks_result:
                    schema_lines.append("  Foreign Keys:")
                    for fk in fks_result:
                        schema_lines.append(f"    - {fk['from_column']} → {fk['to_table']}.{fk['to_column']}")
                schema_lines.append("")

            schema_detail = "\n".join(schema_lines)

            return (
                f"Explain this database schema in plain language:\n\n"
                f"{schema_detail}\n\n"
                f"For each table, describe:\n"
                f"1. What it represents (business entity or concept)\n"
                f"2. Key relationships (via the foreign keys listed)\n"
                f"3. Notable patterns or design quirks\n"
                f"4. Any design smells or potential issues\n\n"
                f"Structure your response as a narrative that a business analyst could read."
            )
        except Exception as exc:
            return (
                f"Could not load schema {validated_schema!r}: {exc}\n\n"
                f"Use `list_tables` to explore the schema manually."
            )

    @mcp.prompt()
    async def database_health_report() -> str:
        """Generate a prioritized database health and performance report.

        Instructs the model to run diagnostic tools and synthesize findings
        into actionable recommendations.
        """
        return (
            "Generate a comprehensive database health report:\n\n"
            "1. Call `health_check` to get an overview of database state.\n"
            "2. Call `index_usage` to find unused, duplicate or missing indexes.\n"
            "3. Call `vacuum_status` to check bloat and maintenance.\n"
            "4. Call `slow_queries` to identify performance bottlenecks.\n"
            "5. Call `connection_activity` to see active workload.\n\n"
            "Then synthesize these into a prioritized report that includes:\n"
            "- Current state (size, connections, cache hit ratio)\n"
            "- Top 3 performance issues with estimated impact\n"
            "- Actionable fixes:\n"
            "  - CREATE INDEX statements for missing indexes\n"
            "  - DROP INDEX for unused indexes\n"
            "  - VACUUM/ANALYZE commands if needed\n"
            "  - Query rewrites for slow queries\n"
            "- Risk assessment (what could fail next)\n\n"
            "Format the output so it can be handed directly to a DBA."
        )

    @mcp.prompt()
    async def data_quality_audit(table_name: str, schema_name: str = "public") -> str:
        """Audit a table for data quality issues.

        Embeds the table's real columns and constraints, then asks the model
        to profile and identify quality issues like nulls, orphaned FKs, and
        duplicates.

        Args:
            table_name: Table to audit.
            schema_name: Schema containing the table. Defaults to "public".
        """
        try:
            validated_schema = validate_schema(schema_name, settings)
        except SchemaNotAllowedError as exc:
            return (
                f"Schema {schema_name!r} is not accessible. {exc}\n\n"
                f"Use `list_schemas` to find available schemas."
            )

        try:
            # Fetch table structure
            cols_result = await db().fetch_all(
                """
                SELECT
                    a.attname,
                    format_type(a.atttypid, a.atttypmod) AS type,
                    a.attnotnull,
                    col_description(c.oid, a.attnum) AS comment
                FROM pg_attribute a
                JOIN pg_class c ON c.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
                LIMIT 50
                """,
                [validated_schema, table_name],
            )

            if not cols_result:
                return (
                    f"Table {validated_schema}.{table_name} not found. "
                    f"Use `list_tables` to find the correct table name."
                )

            # Fetch constraints
            constraints_result = await db().fetch_all(
                """
                SELECT
                    conname,
                    contype,
                    pg_get_constraintdef(oid) AS definition
                FROM pg_constraint
                WHERE conrelid = (
                    SELECT c.oid FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = %s AND c.relname = %s
                )
                ORDER BY conname
                """,
                [validated_schema, table_name],
            )

            col_lines = []
            for col in cols_result:
                nullable = "" if col["attnotnull"] else " (nullable)"
                col_desc = f" - {col['comment']}" if col["comment"] else ""
                col_lines.append(f"  - {col['attname']}: {col['type']}{nullable}{col_desc}")

            constraint_lines = []
            for c in constraints_result:
                constraint_lines.append(f"  - {c['conname']}: {c['definition']}")

            constraints_section = (
                "Constraints:\n" + "\n".join(constraint_lines)
                if constraint_lines
                else "No constraints defined."
            )

            return (
                f"Audit the data quality of {validated_schema}.{table_name}:\n\n"
                f"Columns:\n" + "\n".join(col_lines) + "\n\n" + constraints_section + "\n\n"
                "Instructions:\n"
                "1. Call `column_stats` for each column to see null rates, cardinality, outliers.\n"
                "2. For nullable columns marked as NOT NULL in constraints, "
                "check for unexpected NULLs.\n"
                "3. For each foreign key, check for orphaned references "
                "(rows whose FK does not exist).\n"
                "4. Look for duplicate business keys (if you can identify them).\n"
                "5. Check data types for inconsistencies "
                "(e.g., email in text column with bad format).\n"
                "6. Report findings as SQL queries that demonstrate each issue.\n\n"
                "For each finding, provide:\n"
                "- Description of the issue\n"
                "- SQL to reproduce it\n"
                "- Affected row count\n"
                "- Remediation steps"
            )
        except Exception as exc:
            return (
                f"Could not load table {validated_schema}.{table_name}: {exc}\n\n"
                f"Use `describe_table` to verify the table exists."
            )
