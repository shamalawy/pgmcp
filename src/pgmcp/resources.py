"""MCP resources: addressable, cacheable context about the database.

Resources expose live schema and configuration as structured data without
requiring tool calls, so clients can attach them wholesale to a context window.
"""

from __future__ import annotations

from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ResourceError, ResourceNotFoundError

from .config import Settings
from .errors import SchemaNotAllowedError
from .tools._helpers import (
    as_json,
    db,
    ident,
    schema_predicate,
    validate_schema,
)

__all__ = ["register"]


def register(mcp: MCPServer, settings: Settings) -> None:
    """Register resources on the server."""

    @mcp.resource(
        uri="postgres://server/info",
        name="Server Info",
        description="PostgreSQL version, uptime, database count, and server size",
        mime_type="application/json",
    )
    async def server_info() -> str:
        """Return server metadata and current state."""
        try:
            result = await db().fetch_one(
                """
                SELECT
                    version() AS version,
                    EXTRACT(EPOCH FROM (now() - pg_postmaster_start_time()))::int AS uptime_seconds,
                    (SELECT count(*) FROM pg_database WHERE datistemplate = false) AS database_count,
                    pg_database_size(current_database())::bigint AS current_db_bytes,
                    CASE WHEN pg_is_in_recovery() THEN 'standby' ELSE 'primary' END AS role,
                    current_database() AS current_database,
                    current_user AS current_user
                """,
                [],
            )
            if not result:
                raise ResourceError("Failed to query server info")
            return as_json(result)
        except SchemaNotAllowedError as exc:
            raise ResourceError(str(exc)) from exc
        except Exception as exc:
            raise ResourceError(f"Failed to fetch server info: {exc}") from exc

    @mcp.resource(
        uri="postgres://databases",
        name="Databases",
        description="All databases on the server with their sizes",
        mime_type="application/json",
    )
    async def list_databases() -> str:
        """Return list of databases and their sizes."""
        try:
            rows = await db().fetch_all(
                """
                SELECT
                    datname AS name,
                    datistemplate AS is_template,
                    pg_database_size(datname)::bigint AS size_bytes
                FROM pg_database d
                WHERE datistemplate = false
                ORDER BY datname
                """,
                [],
            )
            return as_json(
                {
                    "databases": rows,
                    "count": len(rows),
                }
            )
        except Exception as exc:
            raise ResourceError(f"Failed to fetch databases: {exc}") from exc

    @mcp.resource(
        uri="postgres://schemas",
        name="Schemas",
        description="Visible schemas with object counts",
        mime_type="application/json",
    )
    async def list_schemas() -> str:
        """Return visible schemas with table/view/function counts."""
        try:
            where, params = schema_predicate("n.nspname", settings)
            rows = await db().fetch_all(
                f"""
                SELECT
                    n.nspname AS name,
                    count(*) FILTER (WHERE c.relkind = 'r') AS tables,
                    count(*) FILTER (WHERE c.relkind = 'v') AS views,
                    count(*) FILTER (WHERE c.relkind = 'm') AS mviews,
                    count(*) FILTER (WHERE c.relkind = 'i') AS indexes
                FROM pg_namespace n
                LEFT JOIN pg_class c ON c.relnamespace = n.oid
                WHERE n.nspname NOT LIKE 'pg_%%' AND n.nspname NOT IN ('information_schema', 'pg_toast')
                {where}
                GROUP BY n.nspname, n.oid
                ORDER BY n.nspname
                """,
                params,
            )
            return as_json(
                {
                    "schemas": rows,
                    "count": len(rows),
                }
            )
        except SchemaNotAllowedError as exc:
            raise ResourceError(str(exc)) from exc
        except Exception as exc:
            raise ResourceError(f"Failed to fetch schemas: {exc}") from exc

    @mcp.resource(
        uri="postgres://{schema}/tables",
        name="Tables in Schema",
        description="Tables in the specified schema with row counts and sizes",
        mime_type="application/json",
    )
    async def list_tables(schema: str) -> str:
        """Return tables in schema with row estimates and sizes."""
        try:
            validated_schema = validate_schema(schema, settings)
            rows = await db().fetch_all(
                """
                SELECT
                    c.relname AS name,
                    c.relkind AS kind,
                    COALESCE(s.n_live_tup, 0)::bigint AS estimated_rows,
                    pg_total_relation_size(c.oid)::bigint AS total_bytes,
                    pg_relation_size(c.oid)::bigint AS heap_bytes,
                    obj_description(c.oid, 'pg_class') AS description
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                LEFT JOIN pg_stat_user_tables s ON s.relid = c.oid
                WHERE n.nspname = %s
                AND c.relkind IN ('r', 'p')
                ORDER BY c.relname
                """,
                [validated_schema],
            )
            return as_json(
                {
                    "schema": validated_schema,
                    "tables": rows,
                    "count": len(rows),
                }
            )
        except SchemaNotAllowedError as exc:
            raise ResourceNotFoundError(str(exc)) from exc
        except Exception as exc:
            raise ResourceError(f"Failed to fetch tables: {exc}") from exc

    @mcp.resource(
        uri="postgres://{schema}/{table}/columns",
        name="Table Columns",
        description="Column definitions for a specific table",
        mime_type="application/json",
    )
    async def describe_columns(schema: str, table: str) -> str:
        """Return column definitions for a table."""
        try:
            validated_schema = validate_schema(schema, settings)
            rows = await db().fetch_all(
                """
                SELECT
                    a.attname AS name,
                    format_type(a.atttypid, a.atttypmod) AS type,
                    a.attnotnull AS not_null,
                    a.atthasdef AS has_default,
                    pg_get_expr(d.adbin, d.adrelid) AS default_expr,
                    col_description(t.oid, a.attnum) AS description
                FROM pg_attribute a
                JOIN pg_class t ON t.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                LEFT JOIN pg_attrdef d ON d.adrelid = t.oid AND d.adnum = a.attnum
                WHERE n.nspname = %s
                AND t.relname = %s
                AND a.attnum > 0
                AND NOT a.attisdropped
                ORDER BY a.attnum
                """,
                [validated_schema, table],
            )
            if not rows:
                raise ResourceNotFoundError(f"table {validated_schema}.{table} not found")
            return as_json(
                {
                    "schema": validated_schema,
                    "table": table,
                    "columns": rows,
                    "count": len(rows),
                }
            )
        except SchemaNotAllowedError as exc:
            raise ResourceNotFoundError(str(exc)) from exc
        except ResourceNotFoundError:
            raise
        except Exception as exc:
            raise ResourceError(f"Failed to fetch columns: {exc}") from exc

    @mcp.resource(
        uri="postgres://{schema}/{table}/ddl",
        name="Table DDL",
        description="Reconstructed CREATE TABLE statement",
        mime_type="text/plain",
    )
    async def table_ddl(schema: str, table: str) -> str:
        """Return best-effort CREATE TABLE reconstruction."""
        try:
            validated_schema = validate_schema(schema, settings)

            # Check if table exists
            exists_result = await db().fetch_one(
                """
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = %s AND c.relname = %s
                AND c.relkind IN ('r', 'p')
                """,
                [validated_schema, table],
            )
            if not exists_result:
                raise ResourceNotFoundError(f"table {validated_schema}.{table} not found")

            # Construct DDL
            table_ident = ident(validated_schema, table).as_string(None)
            ddl_parts = [f"CREATE TABLE {table_ident} ("]

            # Get columns
            columns_rows = await db().fetch_all(
                """
                SELECT
                    a.attname,
                    format_type(a.atttypid, a.atttypmod) AS format_type,
                    a.attnotnull,
                    pg_get_expr(d.adbin, d.adrelid) AS default_expr
                FROM pg_attribute a
                JOIN pg_class t ON t.oid = a.attrelid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                LEFT JOIN pg_attrdef d ON d.adrelid = t.oid AND d.adnum = a.attnum
                WHERE n.nspname = %s AND t.relname = %s
                AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
                """,
                [validated_schema, table],
            )

            col_defs = []
            for row in columns_rows:
                col_name = f'"{row["attname"]}"'
                col_type = row["format_type"]
                col_def = f"  {col_name} {col_type}"

                if row["attnotnull"]:
                    col_def += " NOT NULL"
                if row["default_expr"]:
                    col_def += f" DEFAULT {row['default_expr']}"

                col_defs.append(col_def)

            ddl_parts.append(",\n".join(col_defs))
            ddl_parts.append(");")

            return "\n".join(ddl_parts)
        except SchemaNotAllowedError as exc:
            raise ResourceNotFoundError(str(exc)) from exc
        except ResourceNotFoundError:
            raise
        except Exception as exc:
            raise ResourceError(f"Failed to generate DDL: {exc}") from exc

    @mcp.resource(
        uri="postgres://database/overview",
        name="Database Overview",
        description="Compact schema map with tables and columns (bounded)",
        mime_type="application/json",
    )
    async def database_overview() -> str:
        """Return a bounded whole-database map of all visible schemas and their tables/columns."""
        try:
            where, params = schema_predicate("n.nspname", settings)

            # Get schemas
            schemas_rows = await db().fetch_all(
                f"""
                SELECT n.oid, n.nspname
                FROM pg_namespace n
                WHERE n.nspname NOT LIKE 'pg_%%'
                AND n.nspname NOT IN ('information_schema', 'pg_toast')
                {where}
                ORDER BY n.nspname
                """,
                params,
            )

            overview: dict[str, Any] = {
                "database": await db().fetch_one("SELECT current_database() AS name", []),
                "schemas": [],
                "truncated": False,
            }

            # Cap tables per schema and columns per table
            max_tables_per_schema = 50
            max_columns_per_table = 100
            total_tables = 0
            max_total_tables = 200

            for schema_row in schemas_rows:
                schema_oid = schema_row["oid"]
                schema_name = schema_row["nspname"]

                # Get tables in this schema
                tables_rows = await db().fetch_all(
                    """
                    SELECT c.oid, c.relname
                    FROM pg_class c
                    WHERE c.relnamespace = %s
                    AND c.relkind IN ('r', 'p')
                    ORDER BY c.relname
                    LIMIT %s
                    """,
                    [schema_oid, max_tables_per_schema],
                )

                schema_entry: dict[str, Any] = {
                    "name": schema_name,
                    "tables": [],
                    "truncated": len(tables_rows) >= max_tables_per_schema,
                }

                for table_row in tables_rows:
                    if total_tables >= max_total_tables:
                        overview["truncated"] = True
                        break

                    table_oid = table_row["oid"]
                    table_name = table_row["relname"]

                    # Get columns
                    columns_rows = await db().fetch_all(
                        """
                        SELECT
                            a.attname AS name,
                            format_type(a.atttypid, a.atttypmod) AS type
                        FROM pg_attribute a
                        WHERE a.attrelid = %s
                        AND a.attnum > 0
                        AND NOT a.attisdropped
                        ORDER BY a.attnum
                        LIMIT %s
                        """,
                        [table_oid, max_columns_per_table],
                    )

                    table_entry: dict[str, Any] = {
                        "name": table_name,
                        "columns": columns_rows,
                        "truncated": len(columns_rows) >= max_columns_per_table,
                    }

                    schema_entry["tables"].append(table_entry)
                    total_tables += 1

                overview["schemas"].append(schema_entry)

                if overview["truncated"]:
                    break

            return as_json(overview)
        except SchemaNotAllowedError as exc:
            raise ResourceError(str(exc)) from exc
        except Exception as exc:
            raise ResourceError(f"Failed to fetch database overview: {exc}") from exc
