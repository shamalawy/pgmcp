"""Catalog introspection toolset for pgmcp.

Tools for exploring PostgreSQL schemas, tables, columns, indexes, views,
functions, types, sequences, and the complete data model.
"""

from __future__ import annotations

from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations
from psycopg import sql

from ..config import Settings
from ..errors import PgMcpError
from ._helpers import (
    as_json,
    db,
    ident,
    regclass,
    schema_predicate,
    show,
    tool,
    validate_schema,
)

READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True)


def register(mcp: MCPServer, settings: Settings) -> None:
    """Register catalog introspection tools on the server."""

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def list_schemas(format: str | None = None) -> str:
        """List all schemas with owner, size, and object counts.

        Returns schema name, owner, size on disk, counts of tables, views,
        functions, and any schema comment.

        Args:
            format: "json" (default), "markdown", or "csv".
        """
        query_text = """
            SELECT
                n.nspname AS schema_name,
                pg_catalog.pg_get_userbyid(n.nspowner) AS owner,
                pg_catalog.pg_size_pretty(
                    COALESCE(sum(pg_catalog.pg_total_relation_size(c.oid)), 0)
                ) AS size_pretty,
                COALESCE(sum(pg_catalog.pg_total_relation_size(c.oid)), 0) AS size_bytes,
                COALESCE(sum(CASE WHEN c.relkind = 'r' THEN 1 ELSE 0 END), 0) AS table_count,
                COALESCE(sum(CASE WHEN c.relkind = 'v' THEN 1 ELSE 0 END), 0) AS view_count,
                COALESCE(sum(CASE WHEN c.relkind = 'm' THEN 1 ELSE 0 END), 0) AS matview_count,
                COALESCE(sum(CASE WHEN c.relkind = 'i' THEN 1 ELSE 0 END), 0) AS index_count,
                COALESCE(count(DISTINCT p.oid), 0) AS function_count,
                d.description AS comment
            FROM pg_catalog.pg_namespace n
            LEFT JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid
                AND c.relkind IN ('r', 'v', 'm', 'i', 'S')
            LEFT JOIN pg_catalog.pg_proc p ON p.pronamespace = n.oid
            LEFT JOIN pg_catalog.pg_description d ON d.objoid = n.oid
                AND d.classoid = 'pg_namespace'::regclass
            WHERE true
        """
        where, params = schema_predicate("n.nspname", settings)
        query_text += where
        query_text += " GROUP BY n.oid, n.nspname, n.nspowner, d.description ORDER BY n.nspname"

        result = await db().fetch(sql.SQL(query_text), params)
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def list_tables(
        schema_name: str = "public",
        include_sizes: bool = True,
        pattern: str | None = None,
        limit: int | None = None,
        format: str | None = None,
    ) -> str:
        """List tables, views, and materialized views in a schema.

        Returns relation name, kind (table/partitioned/foreign/matview/view),
        owner, estimated live rows, sizes (total/table/index), comment, and
        whether row-level security is enabled.

        Args:
            schema_name: Schema to inspect. Defaults to "public".
            include_sizes: Whether to compute storage sizes (may be slow on large tables).
            pattern: Optional ILIKE pattern to filter names.
            limit: Maximum rows. Server cap applies.
            format: "json" (default), "markdown", or "csv".
        """
        schema = validate_schema(schema_name, settings)

        query_text = """
            SELECT
                c.relname AS table_name,
                CASE c.relkind
                    WHEN 'r' THEN 'table'
                    WHEN 'p' THEN 'partitioned'
                    WHEN 'f' THEN 'foreign'
                    WHEN 'm' THEN 'matview'
                    WHEN 'v' THEN 'view'
                    ELSE c.relkind::text
                END AS kind,
                pg_catalog.pg_get_userbyid(c.relowner) AS owner,
                CASE WHEN c.relkind IN ('v', 'm') THEN NULL
                    ELSE COALESCE(c.reltuples::bigint, 0)
                END AS est_live_rows,
                CASE WHEN c.relkind IN ('v', 'm')
                    THEN NULL
                    ELSE pg_catalog.pg_size_pretty(pg_catalog.pg_total_relation_size(c.oid))
                END AS total_size_pretty,
                CASE WHEN c.relkind IN ('v', 'm')
                    THEN NULL
                    ELSE pg_catalog.pg_total_relation_size(c.oid)
                END AS total_size_bytes,
                CASE WHEN c.relkind IN ('v', 'm')
                    THEN NULL
                    ELSE pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(c.oid, 'main'))
                END AS table_size_pretty,
                CASE WHEN c.relkind IN ('v', 'm')
                    THEN NULL
                    ELSE pg_catalog.pg_relation_size(c.oid, 'main')
                END AS table_size_bytes,
                CASE WHEN c.relkind IN ('v', 'm')
                    THEN NULL
                    ELSE pg_catalog.pg_size_pretty(pg_catalog.pg_indexes_size(c.oid))
                END AS index_size_pretty,
                CASE WHEN c.relkind IN ('v', 'm')
                    THEN NULL
                    ELSE pg_catalog.pg_indexes_size(c.oid)
                END AS index_size_bytes,
                d.description AS comment,
                c.relrowsecurity AS rls_enabled
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid
                AND d.classoid = 'pg_class'::regclass
            WHERE n.nspname = %s
                AND c.relkind IN ('r', 'p', 'f', 'm', 'v')
        """
        params = [schema]

        if pattern:
            query_text += " AND c.relname ILIKE %s"
            params.append(pattern)

        query_text += " ORDER BY c.relname"

        result = await db().fetch(sql.SQL(query_text), params, limit=limit)
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def describe_table(table_name: str, schema_name: str = "public", format: str | None = None) -> str:
        """Describe a table in detail, returning structure and metadata.

        Returns a comprehensive JSON document with relation kind, owner, size,
        estimated rows, comment; columns with types, nullability, defaults,
        identity, and generated expressions; primary key; foreign keys;
        unique, check, and exclusion constraints; indexes; triggers; RLS
        policies; partition info; inheritance; and sequences owned by columns.

        This is the flagship tool for understanding table structure. Must work
        for tables, views, materialized views, partitioned tables, and foreign
        tables.

        Args:
            table_name: Table to describe (may include schema as "schema.table").
            schema_name: Default schema if table_name is unqualified. Defaults to "public".
            format: "json" (default) or "markdown".
        """
        schema = validate_schema(schema_name, settings)

        relname_clause = regclass(schema, table_name)
        rel_query = """
            SELECT
                c.oid,
                c.relname,
                c.relkind,
                n.nspname,
                CASE c.relkind
                    WHEN 'r' THEN 'table'
                    WHEN 'p' THEN 'partitioned'
                    WHEN 'f' THEN 'foreign'
                    WHEN 'm' THEN 'matview'
                    WHEN 'v' THEN 'view'
                    ELSE c.relkind::text
                END AS kind,
                pg_catalog.pg_get_userbyid(c.relowner) AS owner,
                CASE WHEN c.relkind IN ('v', 'm') THEN NULL
                    ELSE pg_catalog.pg_size_pretty(pg_catalog.pg_total_relation_size(c.oid))
                END AS size_pretty,
                CASE WHEN c.relkind IN ('v', 'm') THEN NULL
                    ELSE pg_catalog.pg_total_relation_size(c.oid)
                END AS size_bytes,
                CASE WHEN c.relkind IN ('v', 'm') THEN NULL
                    ELSE COALESCE(c.reltuples::bigint, 0)
                END AS est_live_rows,
                d.description AS comment,
                c.relrowsecurity AS rls_enabled
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid
                AND d.classoid = 'pg_class'::regclass
            WHERE c.oid = to_regclass(%s)
        """

        rel_result = await db().fetch(sql.SQL(rel_query), [relname_clause])
        if not rel_result.rows:
            raise PgMcpError(
                f"relation {schema}.{table_name} not found; use list_tables to see available relations"
            )

        rel = rel_result.rows[0]
        rel_oid = rel["oid"]
        relkind = rel["relkind"]

        doc: dict = {
            "relation": {
                "name": rel["relname"],
                "schema": rel["nspname"],
                "kind": rel["kind"],
                "owner": rel["owner"],
                "size_pretty": rel["size_pretty"],
                "size_bytes": rel["size_bytes"],
                "est_live_rows": rel["est_live_rows"],
                "comment": rel["comment"],
                "rls_enabled": rel["rls_enabled"],
            }
        }

        if relkind in ("r", "p", "f"):
            columns_query = """
                SELECT
                    a.attnum,
                    a.attname,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
                    NOT a.attnotnull AS is_nullable,
                    d.adbin IS NOT NULL OR a.attidentity != '' AS has_default,
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_expr,
                    a.attidentity AS identity_type,
                    a.attgenerated AS generated_type,
                    g.adbin::text AS generated_expr,
                    a.attcollation,
                    col_d.description AS comment
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid
                    AND d.adnum = a.attnum AND d.adbin IS NOT NULL
                LEFT JOIN pg_catalog.pg_attrdef g ON g.adrelid = a.attrelid
                    AND g.adnum = a.attnum
                LEFT JOIN pg_catalog.pg_description col_d ON col_d.objoid = a.attrelid
                    AND col_d.objsubid = a.attnum
                WHERE a.attrelid = %s
                    AND a.attnum > 0
                    AND NOT a.attisdropped
                ORDER BY a.attnum
            """
            cols_result = await db().fetch(sql.SQL(columns_query), [rel_oid])
            doc["columns"] = [
                {
                    "position": col["attnum"],
                    "name": col["attname"],
                    "type": col["type"],
                    "nullable": col["is_nullable"],
                    "default": col["default_expr"],
                    "identity": col["identity_type"] if col["identity_type"] else None,
                    "generated": col["generated_expr"],
                    "collation": col["attcollation"] if col["attcollation"] != 0 else None,
                    "comment": col["comment"],
                }
                for col in cols_result.rows
            ]

            pk_query = """
                SELECT a.attname
                FROM pg_catalog.pg_index i
                JOIN pg_catalog.pg_attribute a ON a.attrelid = i.indrelid
                    AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s AND i.indisprimary
                ORDER BY a.attnum
            """
            pk_result = await db().fetch(sql.SQL(pk_query), [rel_oid])
            if pk_result.rows:
                doc["primary_key"] = [row["attname"] for row in pk_result.rows]

            fk_query = """
                SELECT
                    c.conname,
                    array_agg(a1.attname ORDER BY a1.attnum) AS from_columns,
                    c.confrelid::regclass AS to_table,
                    array_agg(a2.attname ORDER BY a2.attnum) AS to_columns,
                    c.confupdtype,
                    c.confdeltype
                FROM pg_catalog.pg_constraint c
                JOIN pg_catalog.pg_attribute a1 ON a1.attrelid = c.conrelid
                    AND a1.attnum = ANY(c.conkey)
                JOIN pg_catalog.pg_attribute a2 ON a2.attrelid = c.confrelid
                    AND a2.attnum = ANY(c.confkey)
                WHERE c.conrelid = %s AND c.contype = 'f'
                GROUP BY c.oid, c.conname, c.confrelid, c.confupdtype, c.confdeltype
                ORDER BY c.conname
            """
            fk_result = await db().fetch(sql.SQL(fk_query), [rel_oid])
            if fk_result.rows:
                doc["foreign_keys"] = [
                    {
                        "name": row["conname"],
                        "columns": row["from_columns"],
                        "referenced_table": str(row["to_table"]),
                        "referenced_columns": row["to_columns"],
                        "on_update": row["confupdtype"],
                        "on_delete": row["confdeltype"],
                    }
                    for row in fk_result.rows
                ]

            uq_query = """
                SELECT c.conname, array_agg(a.attname ORDER BY a.attnum) AS columns
                FROM pg_catalog.pg_constraint c
                JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid
                WHERE c.conrelid = %s AND c.contype = 'u'
                GROUP BY c.oid, c.conname
                ORDER BY c.conname
            """
            uq_result = await db().fetch(sql.SQL(uq_query), [rel_oid])
            if uq_result.rows:
                doc["unique_constraints"] = [
                    {"name": row["conname"], "columns": row["columns"]} for row in uq_result.rows
                ]

            ck_query = """
                SELECT c.conname, pg_catalog.pg_get_constraintdef(c.oid) AS definition
                FROM pg_catalog.pg_constraint c
                WHERE c.conrelid = %s AND c.contype = 'c'
                ORDER BY c.conname
            """
            ck_result = await db().fetch(sql.SQL(ck_query), [rel_oid])
            if ck_result.rows:
                doc["check_constraints"] = [
                    {"name": row["conname"], "expression": row["definition"]} for row in ck_result.rows
                ]

            ex_query = """
                SELECT c.conname, pg_catalog.pg_get_constraintdef(c.oid) AS definition
                FROM pg_catalog.pg_constraint c
                WHERE c.conrelid = %s AND c.contype = 'x'
                ORDER BY c.conname
            """
            ex_result = await db().fetch(sql.SQL(ex_query), [rel_oid])
            if ex_result.rows:
                doc["exclusion_constraints"] = [
                    {"name": row["conname"], "definition": row["definition"]} for row in ex_result.rows
                ]

            idx_query = """
                SELECT
                    i.relname,
                    pg_catalog.pg_get_indexdef(x.indexrelid) AS definition,
                    x.indisunique,
                    x.indisprimary,
                    x.indisvalid,
                    pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(i.oid)) AS size_pretty,
                    pg_catalog.pg_relation_size(i.oid) AS size_bytes,
                    COALESCE(s.idx_scan, 0) AS index_scans
                FROM pg_catalog.pg_index x
                JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
                LEFT JOIN pg_catalog.pg_stat_user_indexes s ON s.indexrelid = x.indexrelid
                WHERE x.indrelid = %s
                ORDER BY i.relname
            """
            idx_result = await db().fetch(sql.SQL(idx_query), [rel_oid])
            if idx_result.rows:
                doc["indexes"] = [
                    {
                        "name": row["relname"],
                        "definition": row["definition"],
                        "unique": row["indisunique"],
                        "primary": row["indisprimary"],
                        "valid": row["indisvalid"],
                        "size_pretty": row["size_pretty"],
                        "size_bytes": row["size_bytes"],
                        "index_scans": row["index_scans"],
                    }
                    for row in idx_result.rows
                ]

            trg_query = """
                SELECT
                    t.tgname,
                    pg_catalog.pg_get_triggerdef(t.oid) AS definition,
                    CASE WHEN t.tgtype & 1 = 1 THEN 'BEFORE' ELSE 'AFTER' END AS timing,
                    CASE WHEN t.tgtype & 2 = 2 THEN 'INSERT' ELSE '' END ||
                    CASE WHEN t.tgtype & 4 = 4 THEN ' UPDATE' ELSE '' END ||
                    CASE WHEN t.tgtype & 8 = 8 THEN ' DELETE' ELSE '' END AS events,
                    p.proname AS function_name
                FROM pg_catalog.pg_trigger t
                JOIN pg_catalog.pg_proc p ON p.oid = t.tgfoid
                WHERE t.tgrelid = %s AND NOT t.tgisinternal
                ORDER BY t.tgname
            """
            trg_result = await db().fetch(sql.SQL(trg_query), [rel_oid])
            if trg_result.rows:
                doc["triggers"] = [
                    {
                        "name": row["tgname"],
                        "timing": row["timing"].strip(),
                        "events": row["events"].strip().split(),
                        "function": row["function_name"],
                    }
                    for row in trg_result.rows
                ]

            rls_query = """
                SELECT
                    p.polname,
                    CASE p.polcmd
                        WHEN 'r' THEN 'SELECT'
                        WHEN 'w' THEN 'UPDATE'
                        WHEN 'd' THEN 'DELETE'
                        WHEN 'a' THEN 'INSERT'
                        WHEN '*' THEN 'ALL'
                        ELSE p.polcmd::text
                    END AS command,
                    -- polroles = {0} means PUBLIC, which has no pg_roles row, so the
                    -- LEFT JOIN yields NULL. Report it as "public" rather than null.
                    array_agg(COALESCE(r.rolname, 'public') ORDER BY r.rolname) AS roles,
                    pg_catalog.pg_get_expr(p.polqual, p.polrelid) AS using_expr,
                    pg_catalog.pg_get_expr(p.polwithcheck, p.polrelid) AS with_check_expr
                FROM pg_catalog.pg_policy p
                LEFT JOIN pg_catalog.pg_roles r ON r.oid = ANY(p.polroles)
                WHERE p.polrelid = %s
                GROUP BY p.oid, p.polname, p.polcmd, p.polqual, p.polwithcheck, p.polrelid
                ORDER BY p.polname
            """
            rls_result = await db().fetch(sql.SQL(rls_query), [rel_oid])
            if rls_result.rows:
                doc["rls_policies"] = [
                    {
                        "name": row["polname"],
                        "command": row["command"],
                        "roles": row["roles"],
                        "using": row["using_expr"],
                        "with_check": row["with_check_expr"],
                    }
                    for row in rls_result.rows
                ]

            if relkind == "p":
                part_query = """
                    SELECT
                        pg_catalog.pg_get_partkeydef(%s) AS key,
                        (SELECT relpartbound FROM pg_catalog.pg_class WHERE oid = %s) AS bounds
                    LIMIT 1
                """
                part_result = await db().fetch(sql.SQL(part_query), [rel_oid, rel_oid])
                if part_result.rows:
                    part = part_result.rows[0]
                    doc["partition_info"] = {
                        "strategy": "range",
                        "key": part["key"],
                        "bounds": part["bounds"],
                    }

            inh_query = """
                SELECT c.relname, n.nspname
                FROM pg_catalog.pg_inherits
                JOIN pg_catalog.pg_class c ON pg_inherits.inhparent = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE pg_inherits.inhrelid = %s
                ORDER BY c.relname
            """
            inh_result = await db().fetch(sql.SQL(inh_query), [rel_oid])
            if inh_result.rows:
                doc["inheritance_parents"] = [f"{row['nspname']}.{row['relname']}" for row in inh_result.rows]

            child_query = """
                SELECT c.relname, n.nspname
                FROM pg_catalog.pg_inherits
                JOIN pg_catalog.pg_class c ON pg_inherits.inhrelid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE pg_inherits.inhparent = %s
                ORDER BY c.relname
            """
            child_result = await db().fetch(sql.SQL(child_query), [rel_oid])
            if child_result.rows:
                doc["inheritance_children"] = [
                    f"{row['nspname']}.{row['relname']}" for row in child_result.rows
                ]

            seq_query = """
                SELECT s.relname, n.nspname, a.attname
                FROM pg_catalog.pg_class s
                JOIN pg_catalog.pg_namespace n ON s.relnamespace = n.oid
                JOIN pg_catalog.pg_depend d ON d.objid = s.oid
                JOIN pg_catalog.pg_attribute a ON a.attrelid = d.refobjid
                    AND a.attnum = d.refobjsubid
                WHERE d.refobjid = %s AND s.relkind = 'S'
                ORDER BY s.relname
            """
            seq_result = await db().fetch(sql.SQL(seq_query), [rel_oid])
            if seq_result.rows:
                doc["owned_sequences"] = [
                    {
                        "sequence": f"{row['nspname']}.{row['relname']}",
                        "column": row["attname"],
                    }
                    for row in seq_result.rows
                ]

        elif relkind in ("v", "m"):
            view_def_query = """
                SELECT pg_catalog.pg_get_viewdef(%s, TRUE) AS definition
            """
            view_def_result = await db().fetch(sql.SQL(view_def_query), [rel_oid])
            if view_def_result.rows:
                doc["view_definition"] = view_def_result.rows[0]["definition"]

        return as_json(doc, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def get_table_ddl(table_name: str, schema_name: str = "public") -> str:
        """Reconstruct CREATE TABLE/VIEW and related DDL statements.

        Returns a best-effort reconstruction of the CREATE TABLE statement
        including columns with types, defaults, identity, generated expressions,
        constraints (PK, unique, check, foreign keys), plus CREATE INDEX
        statements and COMMENT ON statements.

        For views and materialized views, emits CREATE VIEW using
        pg_get_viewdef. This is a reconstruction, not a pg_dump export, and
        may not be identical to the original CREATE statement.

        Args:
            table_name: Table to reconstruct (may include schema as "schema.table").
            schema_name: Default schema if table_name is unqualified. Defaults to "public".
        """
        schema = validate_schema(schema_name, settings)

        relname_clause = regclass(schema, table_name)
        rel_query = """
            SELECT c.oid, c.relname, c.relkind, n.nspname
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            WHERE c.oid = to_regclass(%s)
        """
        rel_result = await db().fetch(sql.SQL(rel_query), [relname_clause])
        if not rel_result.rows:
            raise PgMcpError(
                f"relation {schema}.{table_name} not found; use list_tables to see available relations"
            )

        rel = rel_result.rows[0]
        rel_oid = rel["oid"]
        relkind = rel["relkind"]
        rel_name = rel["relname"]
        rel_schema = rel["nspname"]

        ddl_lines = []

        if relkind in ("v", "m"):
            view_def_query = """
                SELECT pg_catalog.pg_get_viewdef(%s, TRUE) AS definition
            """
            view_def_result = await db().fetch(sql.SQL(view_def_query), [rel_oid])
            if view_def_result.rows:
                view_type = "MATERIALIZED VIEW" if relkind == "m" else "VIEW"
                view_ident = ident(rel_schema, rel_name).as_string(None)
                view_def = view_def_result.rows[0]["definition"]
                ddl_lines.append(f"CREATE {view_type} {view_ident} AS\n{view_def};")
        else:
            col_query = """
                SELECT
                    a.attname,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS type,
                    a.attnotnull,
                    pg_catalog.pg_get_expr(d.adbin, d.adrelid) AS default_expr,
                    a.attidentity,
                    a.attgenerated
                FROM pg_catalog.pg_attribute a
                LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid
                    AND d.adnum = a.attnum
                WHERE a.attrelid = %s AND a.attnum > 0 AND NOT a.attisdropped
                ORDER BY a.attnum
            """
            col_result = await db().fetch(sql.SQL(col_query), [rel_oid])

            col_defs = []
            for col in col_result.rows:
                col_def = f"  {ident(col['attname']).as_string(None)} {col['type']}"
                if col["attidentity"]:
                    if col["attidentity"] == "a":
                        col_def += " GENERATED ALWAYS AS IDENTITY"
                    elif col["attidentity"] == "d":
                        col_def += " GENERATED BY DEFAULT AS IDENTITY"
                elif col["attgenerated"]:
                    if col["default_expr"]:
                        col_def += f" GENERATED ALWAYS AS ({col['default_expr']}) STORED"
                elif col["default_expr"]:
                    col_def += f" DEFAULT {col['default_expr']}"

                if col["attnotnull"]:
                    col_def += " NOT NULL"
                col_defs.append(col_def)

            pk_query = """
                SELECT pg_catalog.pg_get_indexdef(x.indexrelid) AS definition
                FROM pg_catalog.pg_index x
                WHERE x.indrelid = %s AND x.indisprimary
            """
            pk_result = await db().fetch(sql.SQL(pk_query), [rel_oid])
            if pk_result.rows:
                pk_def = pk_result.rows[0]["definition"]
                rel_ident = ident(rel_schema, rel_name).as_string(None)
                pk_part = pk_def.split("USING")[0].replace(rel_ident + " USING", "").strip()
                col_defs.append(f"  {pk_part}")

            uq_query = """
                SELECT c.conname, array_agg(a.attname ORDER BY a.attnum) AS columns
                FROM pg_catalog.pg_constraint c
                JOIN pg_catalog.pg_attribute a ON a.attrelid = c.conrelid
                WHERE c.conrelid = %s AND c.contype = 'u'
                GROUP BY c.oid, c.conname
                ORDER BY c.conname
            """
            uq_result = await db().fetch(sql.SQL(uq_query), [rel_oid])
            for row in uq_result.rows:
                col_names = ", ".join(ident(c).as_string(None) for c in row["columns"])
                uq_constraint = f"  CONSTRAINT {ident(row['conname']).as_string(None)} UNIQUE ({col_names})"
                col_defs.append(uq_constraint)

            ck_query = """
                SELECT c.conname, pg_catalog.pg_get_constraintdef(c.oid) AS definition
                FROM pg_catalog.pg_constraint c
                WHERE c.conrelid = %s AND c.contype = 'c'
                ORDER BY c.conname
            """
            ck_result = await db().fetch(sql.SQL(ck_query), [rel_oid])
            for row in ck_result.rows:
                col_defs.append(f"  {row['definition']}")

            fk_query = """
                SELECT pg_catalog.pg_get_constraintdef(c.oid) AS definition
                FROM pg_catalog.pg_constraint c
                WHERE c.conrelid = %s AND c.contype = 'f'
                ORDER BY c.conname
            """
            fk_result = await db().fetch(sql.SQL(fk_query), [rel_oid])
            for row in fk_result.rows:
                col_defs.append(f"  {row['definition']}")

            rel_ident = ident(rel_schema, rel_name).as_string(None)
            col_defs_str = ",\n".join(col_defs)
            create_stmt = f"CREATE TABLE {rel_ident} (\n{col_defs_str}\n);"
            ddl_lines.append(create_stmt)

            idx_query = """
                SELECT pg_catalog.pg_get_indexdef(x.indexrelid) AS definition
                FROM pg_catalog.pg_index x
                JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
                WHERE x.indrelid = %s AND NOT x.indisprimary
                ORDER BY i.relname
            """
            idx_result = await db().fetch(sql.SQL(idx_query), [rel_oid])
            for row in idx_result.rows:
                ddl_lines.append(f"{row['definition']};")

        cmt_query = """
            SELECT d.description AS comment
            FROM pg_catalog.pg_description d
            WHERE d.objoid = %s AND d.classoid = 'pg_class'::regclass
        """
        cmt_result = await db().fetch(sql.SQL(cmt_query), [rel_oid])
        if cmt_result.rows and cmt_result.rows[0].get("comment"):
            comment = cmt_result.rows[0]["comment"]
            quoted_comment = "'" + comment.replace("'", "''") + "'"
            rel_ident = ident(rel_schema, rel_name).as_string(None)
            ddl_lines.append(f"COMMENT ON TABLE {rel_ident} IS {quoted_comment};")

        return "\n\n".join(ddl_lines)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def list_views(
        schema_name: str = "public",
        include_definition: bool = False,
        include_materialized: bool = True,
        format: str | None = None,
    ) -> str:
        """List views and materialized views in a schema.

        Returns view name, whether it is materialized, owner, and optionally
        the view definition.

        Args:
            schema_name: Schema to inspect. Defaults to "public".
            include_definition: Whether to include the view SQL definition.
            include_materialized: Whether to include materialized views.
            format: "json" (default), "markdown", or "csv".
        """
        schema = validate_schema(schema_name, settings)

        query_text = """
            SELECT
                c.relname,
                c.relkind = 'm' AS is_materialized,
                pg_catalog.pg_get_userbyid(c.relowner) AS owner,
        """
        if include_definition:
            query_text += "pg_catalog.pg_get_viewdef(c.oid, TRUE) AS definition,"

        query_text += """
                d.description AS comment
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_catalog.pg_description d ON d.objoid = c.oid
                AND d.classoid = 'pg_class'::regclass
            WHERE n.nspname = %s
                AND c.relkind IN ('v'
        """
        if include_materialized:
            query_text += ", 'm'"
        query_text += """
                )
            ORDER BY c.relname
        """

        result = await db().fetch(sql.SQL(query_text), [schema], limit=None)
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def list_indexes(
        schema_name: str = "public", table_name: str | None = None, format: str | None = None
    ) -> str:
        """List indexes in a schema, optionally filtering to a specific table.

        Returns index name, definition, whether unique/primary/valid, size,
        and scan count.

        Args:
            schema_name: Schema to inspect. Defaults to "public".
            table_name: Optional table name to filter indexes.
            format: "json" (default), "markdown", or "csv".
        """
        schema = validate_schema(schema_name, settings)

        query_text = """
            SELECT
                i.relname AS index_name,
                t.relname AS table_name,
                pg_catalog.pg_get_indexdef(x.indexrelid) AS definition,
                x.indisunique,
                x.indisprimary,
                x.indisvalid,
                pg_catalog.pg_size_pretty(pg_catalog.pg_relation_size(i.oid)) AS size_pretty,
                pg_catalog.pg_relation_size(i.oid) AS size_bytes,
                COALESCE(s.idx_scan, 0) AS index_scans,
                array_agg(a.attname ORDER BY a.attnum) AS columns
            FROM pg_catalog.pg_index x
            JOIN pg_catalog.pg_class i ON i.oid = x.indexrelid
            JOIN pg_catalog.pg_class t ON t.oid = x.indrelid
            JOIN pg_catalog.pg_namespace n ON t.relnamespace = n.oid
            LEFT JOIN pg_catalog.pg_stat_user_indexes s ON s.indexrelid = x.indexrelid
            LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid
                AND a.attnum = ANY(x.indkey)
            WHERE n.nspname = %s
        """
        params = [schema]

        if table_name:
            query_text += " AND t.relname = %s"
            params.append(table_name)

        query_text += (
            " GROUP BY x.indexrelid, i.oid, i.relname, t.relname, "
            "x.indisunique, x.indisprimary, x.indisvalid, s.idx_scan "
            "ORDER BY i.relname"
        )

        result = await db().fetch(sql.SQL(query_text), params)
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def list_functions(
        schema_name: str = "public",
        include_source: bool = False,
        pattern: str | None = None,
        format: str | None = None,
    ) -> str:
        """List functions, procedures, aggregates and window functions in a schema.

        Returns function name, argument signature, return type, language,
        volatility, kind (function/procedure/aggregate/window), and optionally
        source code.

        Args:
            schema_name: Schema to inspect. Defaults to "public".
            include_source: Whether to include function source code.
            pattern: Optional ILIKE pattern to filter names.
            format: "json" (default), "markdown", or "csv".
        """
        schema = validate_schema(schema_name, settings)

        query_text = """
            SELECT
                p.proname,
                pg_catalog.pg_get_function_arguments(p.oid) AS arguments,
                pg_catalog.pg_get_function_result(p.oid) AS return_type,
                l.lanname AS language,
                p.provolatile AS volatility,
                CASE
                    WHEN p.prokind = 'f' THEN 'function'
                    WHEN p.prokind = 'p' THEN 'procedure'
                    WHEN p.prokind = 'a' THEN 'aggregate'
                    WHEN p.prokind = 'w' THEN 'window'
                    ELSE p.prokind::text
                END AS kind,
                d.description AS comment
        """
        if include_source:
            query_text += ", p.prosrc AS source"

        query_text += """
            FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid
            JOIN pg_catalog.pg_language l ON p.prolang = l.oid
            LEFT JOIN pg_catalog.pg_description d ON d.objoid = p.oid
            WHERE n.nspname = %s
        """
        params = [schema]

        if pattern:
            query_text += " AND p.proname ILIKE %s"
            params.append(pattern)

        query_text += " ORDER BY p.proname, p.oid"

        result = await db().fetch(sql.SQL(query_text), params)
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def list_types(schema_name: str = "public", format: str | None = None) -> str:
        """List all types in a schema: enums, composites, domains, and ranges.

        Returns type name, kind (enum/composite/domain/range), and for enums
        the list of labels, for domains the base type and any constraints.

        Args:
            schema_name: Schema to inspect. Defaults to "public".
            format: "json" (default) or "markdown".
        """
        schema = validate_schema(schema_name, settings)

        doc: dict = {"enums": [], "composites": [], "domains": [], "ranges": []}

        enum_query = """
            SELECT
                t.typname,
                array_agg(e.enumlabel ORDER BY e.enumsortorder) AS labels
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
            LEFT JOIN pg_catalog.pg_enum e ON e.enumtypid = t.oid
            WHERE n.nspname = %s AND t.typtype = 'e'
            GROUP BY t.oid, t.typname
            ORDER BY t.typname
        """
        enum_result = await db().fetch(sql.SQL(enum_query), [schema])
        doc["enums"] = [{"name": row["typname"], "labels": row["labels"]} for row in enum_result.rows]

        comp_query = """
            SELECT
                t.typname,
                array_agg(a.attname ORDER BY a.attnum) AS attributes,
                array_agg(pg_catalog.format_type(a.atttypid, a.atttypmod)
                    ORDER BY a.attnum) AS types
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
            LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = t.typrelid
                AND a.attnum > 0 AND NOT a.attisdropped
            WHERE n.nspname = %s AND t.typtype = 'c'
            GROUP BY t.oid, t.typname
            ORDER BY t.typname
        """
        comp_result = await db().fetch(sql.SQL(comp_query), [schema])
        doc["composites"] = [
            {
                "name": row["typname"],
                "attributes": [
                    {"name": attr, "type": typ}
                    for attr, typ in zip(row["attributes"] or [], row["types"] or [], strict=False)
                ],
            }
            for row in comp_result.rows
        ]

        dom_query = """
            SELECT
                t.typname,
                pg_catalog.format_type(t.typbasetype, t.typtypmod) AS base_type,
                (SELECT pg_catalog.pg_get_expr(conbin, 0)
                 FROM pg_catalog.pg_constraint
                 WHERE contypid = t.oid) AS constraint_expr
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
            WHERE n.nspname = %s AND t.typtype = 'd'
            ORDER BY t.typname
        """
        dom_result = await db().fetch(sql.SQL(dom_query), [schema])
        doc["domains"] = [
            {
                "name": row["typname"],
                "base_type": row["base_type"],
                "constraint": row["constraint_expr"],
            }
            for row in dom_result.rows
        ]

        rng_query = """
            SELECT
                t.typname,
                pg_catalog.format_type(r.rngsubtype, 0) AS subtype
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
            LEFT JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid
            WHERE n.nspname = %s AND t.typtype = 'r'
            ORDER BY t.typname
        """
        rng_result = await db().fetch(sql.SQL(rng_query), [schema])
        doc["ranges"] = [{"name": row["typname"], "subtype": row["subtype"]} for row in rng_result.rows]

        return as_json(doc, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def list_sequences(schema_name: str = "public", format: str | None = None) -> str:
        """List sequences in a schema with their configuration and ownership.

        Returns sequence name, last value, start value, increment, minimum,
        maximum, cycle setting, and the column that owns it if any.

        Args:
            schema_name: Schema to inspect. Defaults to "public".
            format: "json" (default), "markdown", or "csv".
        """
        schema = validate_schema(schema_name, settings)

        query_text = """
            SELECT
                s.relname,
                sq.seqstart AS start_value,
                sq.seqincrement AS increment_by,
                sq.seqmin AS min_value,
                sq.seqmax AS max_value,
                sq.seqcycle AS is_cycled,
                CASE WHEN a.attname IS NOT NULL
                    THEN concat(an.nspname, '.', a.attname)
                    ELSE NULL
                END AS owned_by
            FROM pg_catalog.pg_class s
            JOIN pg_catalog.pg_namespace n ON s.relnamespace = n.oid
            JOIN pg_catalog.pg_sequence sq ON sq.seqrelid = s.oid
            LEFT JOIN pg_catalog.pg_depend d ON d.objid = s.oid
                AND d.classid = 'pg_class'::regclass
            LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid = d.refobjid
                AND a.attnum = d.refobjsubid
            LEFT JOIN pg_catalog.pg_class ac ON ac.oid = d.refobjid
            LEFT JOIN pg_catalog.pg_namespace an ON an.oid = ac.relnamespace
            WHERE n.nspname = %s AND s.relkind = 'S'
            ORDER BY s.relname
        """

        result = await db().fetch(sql.SQL(query_text), [schema])
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def list_extensions(format: str | None = None) -> str:
        """List installed and available PostgreSQL extensions.

        Returns installed extensions with version and schema, plus available
        but not yet installed extensions.

        Args:
            format: "json" (default), "markdown", or "csv".
        """
        query_text = """
            SELECT
                e.extname AS name,
                e.extversion AS version,
                n.nspname AS schema,
                true AS installed
            FROM pg_catalog.pg_extension e
            LEFT JOIN pg_catalog.pg_namespace n ON e.extnamespace = n.oid
            UNION ALL
            SELECT
                a.name,
                a.default_version,
                NULL,
                false
            FROM pg_catalog.pg_available_extensions a
            WHERE a.name NOT IN (SELECT e.extname FROM pg_catalog.pg_extension e)
            ORDER BY name
        """

        result = await db().fetch(sql.SQL(query_text))
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def list_databases(format: str | None = None) -> str:
        """List all databases with owner, encoding, and connection info.

        Returns database name, owner, encoding, collation, size, connection
        limit, template status, and whether new connections are allowed.

        Args:
            format: "json" (default), "markdown", or "csv".
        """
        query_text = """
            SELECT
                d.datname,
                pg_catalog.pg_get_userbyid(d.datdba) AS owner,
                pg_catalog.pg_encoding_to_char(d.encoding) AS encoding,
                d.datcollate AS collation,
                pg_catalog.pg_size_pretty(pg_catalog.pg_database_size(d.oid)) AS size_pretty,
                pg_catalog.pg_database_size(d.oid) AS size_bytes,
                d.datconnlimit,
                d.datistemplate,
                d.datallowconn
            FROM pg_catalog.pg_database d
            ORDER BY d.datname
        """

        result = await db().fetch(sql.SQL(query_text))
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def list_roles(format: str | None = None) -> str:
        """List all database roles and their attributes.

        Returns role name, superuser/createdb/createrole/login/replication/
        bypassrls attributes, connection limit, validity period, and role
        memberships. Never exposes password hashes.

        Args:
            format: "json" (default), "markdown", or "csv".
        """
        query_text = """
            SELECT
                r.rolname,
                r.rolsuper,
                r.rolcreatedb,
                r.rolcreaterole,
                r.rolcanlogin,
                r.rolreplication,
                r.rolbypassrls,
                r.rolconnlimit,
                r.rolvaliduntil,
                array_agg(m.rolname ORDER BY m.rolname) FILTER (WHERE m.rolname IS NOT NULL) AS memberships
            FROM pg_catalog.pg_roles r
            LEFT JOIN pg_catalog.pg_auth_members a ON a.member = r.oid
            LEFT JOIN pg_catalog.pg_roles m ON m.oid = a.roleid
            GROUP BY r.oid, r.rolname, r.rolsuper, r.rolcreatedb, r.rolcreaterole,
                     r.rolcanlogin, r.rolreplication, r.rolbypassrls, r.rolconnlimit,
                     r.rolvaliduntil
            ORDER BY r.rolname
        """

        result = await db().fetch(sql.SQL(query_text))
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def search_objects(
        pattern: str,
        object_types: list[str] | None = None,
        schema_name: str | None = None,
        limit: int | None = None,
        format: str | None = None,
    ) -> str:
        """Search for database objects by name pattern.

        Case-insensitive search across tables, columns, views, functions,
        indexes, types, and constraints.

        Args:
            pattern: ILIKE pattern to search for (required).
            object_types: Optional list to filter to specific types like
                ["table", "column", "view", "function", "index", "type"].
            schema_name: Optional schema to restrict search.
            limit: Maximum rows. Server cap applies.
            format: "json" (default), "markdown", or "csv".
        """
        if not pattern:
            raise PgMcpError("pattern argument is required")

        if schema_name:
            validate_schema(schema_name, settings)

        type_filters = []
        if object_types:
            valid_types = {"table", "view", "column", "function", "index", "type", "constraint"}
            for ot in object_types:
                if ot.lower() not in valid_types:
                    raise PgMcpError(f"unknown object_type: {ot!r}")
            type_filters = [t.lower() for t in object_types]

        query_text = """
            WITH search AS (
                SELECT 'table' AS object_type, n.nspname, c.relname AS object_name,
                       NULL AS parent_relation, 'Table in schema ' || n.nspname AS detail
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relkind IN ('r', 'p', 'f') AND c.relname ILIKE %s
        """
        params: list[Any] = [pattern]

        if schema_name:
            query_text += " AND n.nspname = %s"
            params.append(schema_name)

        query_text += """
                UNION ALL
                SELECT 'view', n.nspname, c.relname, NULL,
                       'View in schema ' || n.nspname
                FROM pg_catalog.pg_class c
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relkind IN ('v', 'm') AND c.relname ILIKE %s
        """
        params.append(pattern)
        if schema_name:
            query_text += " AND n.nspname = %s"
            params.append(schema_name)

        query_text += """
                UNION ALL
                SELECT 'column', n.nspname, a.attname, c.relname,
                       'Column in table ' || n.nspname || '.' || c.relname
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
                JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
                WHERE a.attnum > 0 AND NOT a.attisdropped
                  AND a.attname ILIKE %s
        """
        params.append(pattern)
        if schema_name:
            query_text += " AND n.nspname = %s"
            params.append(schema_name)

        query_text += """
                UNION ALL
                SELECT 'function', n.nspname, p.proname, NULL,
                       'Function in schema ' || n.nspname
                FROM pg_catalog.pg_proc p
                JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid
                WHERE p.proname ILIKE %s
        """
        params.append(pattern)
        if schema_name:
            query_text += " AND n.nspname = %s"
            params.append(schema_name)

        query_text += """
                UNION ALL
                SELECT 'index', n.nspname, i.relname, t.relname,
                       'Index on table ' || n.nspname || '.' || t.relname
                FROM pg_catalog.pg_class i
                JOIN pg_catalog.pg_index x ON i.oid = x.indexrelid
                JOIN pg_catalog.pg_class t ON x.indrelid = t.oid
                JOIN pg_catalog.pg_namespace n ON i.relnamespace = n.oid
                WHERE i.relname ILIKE %s
        """
        params.append(pattern)
        if schema_name:
            query_text += " AND n.nspname = %s"
            params.append(schema_name)

        query_text += """
                UNION ALL
                SELECT 'type', n.nspname, t.typname, NULL,
                       'Type in schema ' || n.nspname
                FROM pg_catalog.pg_type t
                JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid
                WHERE t.typname ILIKE %s
        """
        params.append(pattern)
        if schema_name:
            query_text += " AND n.nspname = %s"
            params.append(schema_name)

        query_text += """
                UNION ALL
                SELECT 'constraint', n.nspname, c.conname, r.relname,
                       'Constraint on table ' || n.nspname || '.' || r.relname
                FROM pg_catalog.pg_constraint c
                JOIN pg_catalog.pg_class r ON c.conrelid = r.oid
                JOIN pg_catalog.pg_namespace n ON r.relnamespace = n.oid
                WHERE c.conname ILIKE %s
        """
        params.append(pattern)
        if schema_name:
            query_text += " AND n.nspname = %s"
            params.append(schema_name)

        query_text += """ )
            SELECT object_type, nspname AS schema, object_name, parent_relation, detail
            FROM search
        """

        if type_filters:
            query_text += " WHERE object_type = ANY(%s)"
            params.append(type_filters)

        query_text += " ORDER BY object_type, nspname, object_name"

        result = await db().fetch(sql.SQL(query_text), params, limit=limit)
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def get_relationships(
        schema_name: str = "public", format: str | None = None, as_mermaid: bool = False
    ) -> str:
        """Describe the foreign-key graph and relationships in a schema.

        Returns every foreign key constraint with the from and to
        table/column pairs and constraint properties (ON UPDATE/DELETE actions).
        When as_mermaid=True, returns a mermaid erDiagram visualization.

        Args:
            schema_name: Schema to inspect. Defaults to "public".
            format: "json" (default) or "markdown" for tabular output.
            as_mermaid: If True, return a mermaid erDiagram code block instead of tabular data.
        """
        schema = validate_schema(schema_name, settings)

        query_text = """
            SELECT
                n.nspname,
                r1.relname AS from_table,
                array_agg(a1.attname ORDER BY a1.attnum) AS from_columns,
                n2.nspname AS to_schema,
                r2.relname AS to_table,
                array_agg(a2.attname ORDER BY a2.attnum) AS to_columns,
                c.conname,
                c.confdeltype,
                c.confupdtype
            FROM pg_catalog.pg_constraint c
            JOIN pg_catalog.pg_class r1 ON c.conrelid = r1.oid
            JOIN pg_catalog.pg_namespace n ON r1.relnamespace = n.oid
            JOIN pg_catalog.pg_class r2 ON c.confrelid = r2.oid
            JOIN pg_catalog.pg_namespace n2 ON r2.relnamespace = n2.oid
            JOIN pg_catalog.pg_attribute a1 ON a1.attrelid = r1.oid
                AND a1.attnum = ANY(c.conkey)
            JOIN pg_catalog.pg_attribute a2 ON a2.attrelid = r2.oid
                AND a2.attnum = ANY(c.confkey)
            WHERE n.nspname = %s AND c.contype = 'f'
            GROUP BY c.oid, c.conname, c.confdeltype, c.confupdtype,
                     n.nspname, r1.relname, n2.nspname, r2.relname
            ORDER BY r1.relname, c.conname
        """

        result = await db().fetch(sql.SQL(query_text), [schema])

        if as_mermaid:

            def quote_if_needed(name: str) -> str:
                """Quote table name if it contains spaces or quotes."""
                if " " in name or '"' in name:
                    return f'"{name}"'
                return name

            lines = ["erDiagram"]
            tables_seen = set()

            for row in result.rows:
                from_table = row["from_table"]
                to_table = row["to_table"]
                tables_seen.add(from_table)
                tables_seen.add(to_table)

                relation_str = (
                    f"    {quote_if_needed(from_table)} ||--o| "
                    f'{quote_if_needed(to_table)} : "{row["conname"]}"'
                )
                lines.append(relation_str)

            all_tables_query = """
                SELECT relname FROM pg_catalog.pg_class
                JOIN pg_catalog.pg_namespace ON pg_catalog.pg_class.relnamespace = pg_catalog.pg_namespace.oid
                WHERE pg_catalog.pg_namespace.nspname = %s
                  AND pg_catalog.pg_class.relkind IN ('r', 'p', 'f')
                ORDER BY relname
            """
            all_tables = await db().fetch(sql.SQL(all_tables_query), [schema])
            for table in all_tables.rows:
                name = table["relname"]
                if name not in tables_seen:
                    lines.append(f"    {quote_if_needed(name)}")
                    tables_seen.add(name)

            return "\n".join(lines)

        return as_json(
            [
                {
                    "from_schema": row["nspname"],
                    "from_table": row["from_table"],
                    "from_columns": row["from_columns"],
                    "to_schema": row["to_schema"],
                    "to_table": row["to_table"],
                    "to_columns": row["to_columns"],
                    "constraint_name": row["conname"],
                    "on_delete": row["confdeltype"],
                    "on_update": row["confupdtype"],
                }
                for row in result.rows
            ],
            settings,
        )
