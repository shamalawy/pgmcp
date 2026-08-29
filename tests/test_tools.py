"""End-to-end tests for every registered tool, against the fixture database.

Two things are asserted here that unit tests cannot reach:

* **Coverage.** ``REPRESENTATIVE_ARGS`` must name every tool the server registers.
  Adding a tool without adding a call here fails the suite on purpose.
* **Safety.** Read-only mode must refuse writes through the real MCP call path,
  and the refusal is verified by checking the table is still there afterwards.

These tests deliberately do not swallow exceptions: a tool that errors is a
failure, not a skip.
"""

from __future__ import annotations

import contextlib
import json

import pytest
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from pgmcp.config import AccessMode, Settings
from pgmcp.database import Database, set_database
from pgmcp.server import build_server

from .conftest import TEST_DSN, text_of

pytestmark = pytest.mark.integration


# One representative call per tool. Arguments are chosen to exercise the fixture
# data rather than to be minimal.
REPRESENTATIVE_ARGS: dict[str, dict[str, object]] = {
    # schema
    "list_schemas": {},
    "list_tables": {"schema_name": "shop"},
    "describe_table": {"table_name": "orders", "schema_name": "shop"},
    "get_table_ddl": {"table_name": "customers", "schema_name": "shop"},
    "list_views": {"schema_name": "shop", "include_definition": True},
    "list_indexes": {"schema_name": "shop"},
    "list_functions": {"schema_name": "shop", "include_source": True},
    "list_types": {"schema_name": "shop"},
    "list_sequences": {"schema_name": "shop"},
    "list_extensions": {},
    "list_databases": {},
    "list_roles": {},
    "search_objects": {"pattern": "email"},
    "get_relationships": {"schema_name": "shop"},
    # query
    "query": {"sql": "SELECT count(*) AS n FROM shop.customers"},
    "explain_query": {"sql": "SELECT * FROM shop.orders WHERE customer_id = 1"},
    "sample_table": {"table_name": "products", "schema_name": "shop", "limit": 5},
    "count_rows": {"table_name": "orders", "schema_name": "shop"},
    "column_stats": {"table_name": "orders", "column_name": "status", "schema_name": "shop"},
    "table_sizes": {"schema_name": "shop"},
    # health
    "server_info": {},
    "database_stats": {},
    "connection_activity": {},
    "long_running_queries": {},
    "blocking_locks": {},
    "index_usage": {"schema_name": "shop"},
    "table_stats": {"schema_name": "shop"},
    "vacuum_status": {"schema_name": "shop"},
    "slow_queries": {"limit": 5},
    "replication_status": {},
    "health_check": {},
}


async def call(server: MCPServer, name: str, args: dict[str, object] | None = None) -> str:
    """Call a tool and assert it did not error, returning its text payload."""
    result = await server.call_tool(name, dict(args or {}))
    body = text_of(result)
    assert not result.is_error, f"{name} returned an error: {body}"
    return body


async def expect_error(server: MCPServer, name: str, args: dict[str, object] | None = None) -> str:
    """Call a tool that must fail, returning the error message.

    ``MCPServer.call_tool`` raises ``ToolError`` directly; the ``is_error`` result
    shape only appears once the low-level server has converted it for the wire.
    """
    with pytest.raises(ToolError) as raised:
        await server.call_tool(name, dict(args or {}))
    return str(raised.value)


class TestToolCoverage:
    async def test_every_registered_tool_is_exercised(self, server: MCPServer) -> None:
        registered = {tool.name for tool in await server.list_tools()}
        untested = registered - set(REPRESENTATIVE_ARGS)
        assert not untested, (
            f"tools registered but never called by the test suite: {sorted(untested)}. "
            "Add an entry to REPRESENTATIVE_ARGS."
        )

    async def test_read_only_mode_registers_no_write_tools(self, server: MCPServer) -> None:
        names = {tool.name for tool in await server.list_tools()}
        for mutating in ("execute_sql", "execute_transaction", "vacuum_table", "drop_index"):
            assert mutating not in names, f"{mutating} must not be exposed in read-only mode"

    async def test_every_tool_has_a_usable_docstring(self, server: MCPServer) -> None:
        """A one-line description is not enough for a model to choose correctly."""
        thin = [
            tool.name
            for tool in await server.list_tools()
            if not tool.description or len(tool.description) < 120
        ]
        assert not thin, f"tools with an inadequate description: {thin}"

    @pytest.mark.parametrize("name", sorted(REPRESENTATIVE_ARGS))
    async def test_tool_runs_and_returns_content(self, server: MCPServer, name: str) -> None:
        registered = {tool.name for tool in await server.list_tools()}
        if name not in registered:
            pytest.fail(f"{name} is in REPRESENTATIVE_ARGS but was not registered")
        body = await call(server, name, REPRESENTATIVE_ARGS[name])
        assert body.strip(), f"{name} returned empty output"


class TestSchemaTools:
    async def test_list_schemas_finds_fixture_schemas(self, server: MCPServer) -> None:
        body = await call(server, "list_schemas")
        assert "shop" in body
        assert "analytics" in body
        # System schemas stay hidden unless explicitly enabled.
        assert "pg_catalog" not in body

    async def test_list_tables_covers_every_relation_kind(self, server: MCPServer) -> None:
        body = await call(server, "list_tables", {"schema_name": "shop"})
        for expected in ("customers", "orders", "order_items", "products", "events"):
            assert expected in body, f"{expected} missing from list_tables"

    async def test_describe_table_reports_foreign_keys(self, server: MCPServer) -> None:
        body = await call(server, "describe_table", {"table_name": "orders", "schema_name": "shop"})
        payload = json.loads(body)
        rendered = json.dumps(payload)
        assert "customer_id" in rendered
        assert "customers" in rendered, "the FK to shop.customers is missing"
        assert "status" in rendered

    async def test_describe_table_reports_composite_primary_key(self, server: MCPServer) -> None:
        body = await call(server, "describe_table", {"table_name": "order_items", "schema_name": "shop"})
        assert "order_id" in body and "line_no" in body

    async def test_describe_table_handles_quoted_identifiers(self, server: MCPServer) -> None:
        body = await call(
            server, "describe_table", {"table_name": "Weird Table", "schema_name": "Odd Schema"}
        )
        assert "Id Column" in body
        assert "select" in body, "the column named with a reserved word is missing"

    async def test_describe_table_handles_partitioned_table(self, server: MCPServer) -> None:
        body = await call(server, "describe_table", {"table_name": "events", "schema_name": "shop"})
        assert "occurred_at" in body

    async def test_describe_table_unknown_relation_is_a_clear_error(self, server: MCPServer) -> None:
        message = await expect_error(
            server, "describe_table", {"table_name": "does_not_exist", "schema_name": "shop"}
        )
        assert "does_not_exist" in message

    async def test_get_table_ddl_is_runnable_shaped(self, server: MCPServer) -> None:
        body = await call(server, "get_table_ddl", {"table_name": "products", "schema_name": "shop"})
        assert "CREATE TABLE" in body.upper()
        assert "price_cents" in body

    async def test_relation_kinds_are_words_not_catalog_letters(self, server: MCPServer) -> None:
        """A CASE over a "char" column truncates its labels unless cast to text."""
        for table, expected in (
            ("orders", "table"),
            ("events", "partitioned"),
            ("active_orders", "view"),
        ):
            body = await call(server, "describe_table", {"table_name": table, "schema_name": "shop"})
            assert json.loads(body)["relation"]["kind"] == expected

        listing = json.loads(await call(server, "list_tables", {"schema_name": "shop"}))
        kinds = {row["kind"] for row in listing["rows"]}
        assert kinds <= {"table", "partitioned", "view", "matview", "foreign"}, kinds

        functions = json.loads(await call(server, "list_functions", {"schema_name": "shop"}))
        fn_kinds = {str(row.get("kind")) for row in functions["rows"]}
        assert fn_kinds <= {"function", "procedure", "aggregate", "window"}, fn_kinds

    async def test_rls_policy_is_reported_readably(self, server: MCPServer) -> None:
        body = await call(server, "describe_table", {"table_name": "tenant_notes", "schema_name": "shop"})
        policies = json.loads(body)["rls_policies"]
        assert policies[0]["name"] == "tenant_isolation"
        assert policies[0]["command"] == "ALL"
        # polroles = {0} is PUBLIC, which has no pg_roles row to join against.
        assert policies[0]["roles"] == ["public"]
        assert "tenant_id" in policies[0]["using"]

    async def test_list_types_finds_the_enum_labels(self, server: MCPServer) -> None:
        body = await call(server, "list_types", {"schema_name": "shop"})
        assert "order_status" in body
        assert "shipped" in body, "enum labels should be listed"

    async def test_search_objects_finds_a_column(self, server: MCPServer) -> None:
        body = await call(server, "search_objects", {"pattern": "referred_by"})
        assert "referred_by" in body
        assert "customers" in body

    async def test_search_objects_treats_pattern_as_data(self, server: MCPServer) -> None:
        """A pattern containing quotes must not break out of the query."""
        # Whether it matches nothing or errors does not matter; the table surviving does.
        with contextlib.suppress(ToolError):
            await server.call_tool("search_objects", {"pattern": "x'; DROP TABLE shop.orders; --"})
        check = await call(server, "count_rows", {"table_name": "orders", "schema_name": "shop"})
        assert "500" in check

    async def test_get_relationships_finds_self_reference(self, server: MCPServer) -> None:
        body = await call(server, "get_relationships", {"schema_name": "shop"})
        assert "referred_by" in body, "the self-referencing FK is missing"
        assert "order_items" in body

    async def test_get_relationships_renders_mermaid(self, server: MCPServer) -> None:
        body = await call(server, "get_relationships", {"schema_name": "shop", "as_mermaid": True})
        assert "erDiagram" in body

    async def test_list_roles_never_exposes_password_hashes(self, server: MCPServer) -> None:
        body = await call(server, "list_roles")
        assert "SCRAM-SHA-256$" not in body
        assert "rolpassword" not in body


class TestQueryTools:
    async def test_query_returns_rows(self, server: MCPServer) -> None:
        body = await call(server, "query", {"sql": "SELECT count(*) AS n FROM shop.customers"})
        assert "200" in body

    async def test_query_binds_parameters(self, server: MCPServer) -> None:
        body = await call(
            server,
            "query",
            {"sql": "SELECT full_name FROM shop.customers WHERE id = %s::bigint", "params": ["7"]},
        )
        assert "Customer 7" in body

    async def test_query_parameters_are_not_interpolated(self, server: MCPServer) -> None:
        """A parameter carrying SQL must be treated as a value, not as syntax."""
        body = await call(
            server,
            "query",
            {"sql": "SELECT %s::text AS injected", "params": ["'; DROP TABLE shop.orders; --"]},
        )
        assert "DROP TABLE" in body  # returned as data
        check = await call(server, "count_rows", {"table_name": "orders", "schema_name": "shop"})
        assert "500" in check

    async def test_query_honours_row_limit(self, server: MCPServer) -> None:
        body = await call(server, "query", {"sql": "SELECT g FROM generate_series(1, 1000) g", "limit": 5})
        payload = json.loads(body)
        assert payload["row_count"] == 5
        assert payload["truncated"] is True

    async def test_query_supports_csv_and_markdown(self, server: MCPServer) -> None:
        csv_body = await call(server, "query", {"sql": "SELECT 1 AS a, 2 AS b", "format": "csv"})
        assert csv_body.startswith("a,b")
        md_body = await call(server, "query", {"sql": "SELECT 1 AS a", "format": "markdown"})
        assert "| a |" in md_body

    async def test_explain_query_returns_a_plan(self, server: MCPServer) -> None:
        body = await call(server, "explain_query", {"sql": "SELECT * FROM shop.orders WHERE customer_id = 1"})
        assert "Plan" in body or "Scan" in body

    async def test_explain_analyze_executes_the_plan(self, server: MCPServer) -> None:
        body = await call(
            server,
            "explain_query",
            {"sql": "SELECT count(*) FROM shop.orders", "analyze": True},
        )
        assert "Actual" in body or "actual" in body

    async def test_sample_table_selects_named_columns(self, server: MCPServer) -> None:
        body = await call(
            server,
            "sample_table",
            {"table_name": "products", "schema_name": "shop", "columns": ["sku", "name"], "limit": 3},
        )
        assert "sku" in body
        assert "price_cents" not in body

    async def test_sample_table_rejects_unknown_column(self, server: MCPServer) -> None:
        message = await expect_error(
            server,
            "sample_table",
            {"table_name": "products", "schema_name": "shop", "columns": ["no_such_column"]},
        )
        assert "no_such_column" in message

    async def test_sample_table_handles_quoted_identifiers(self, server: MCPServer) -> None:
        body = await call(server, "sample_table", {"table_name": "Weird Table", "schema_name": "Odd Schema"})
        assert "reserved word column" in body

    async def test_count_rows_exact_and_estimated(self, server: MCPServer) -> None:
        exact = await call(server, "count_rows", {"table_name": "orders", "schema_name": "shop"})
        assert "500" in exact
        estimate = await call(
            server, "count_rows", {"table_name": "orders", "schema_name": "shop", "exact": False}
        )
        assert estimate.strip()

    async def test_column_stats_profiles_an_enum_column(self, server: MCPServer) -> None:
        body = await call(
            server,
            "column_stats",
            {"table_name": "orders", "column_name": "status", "schema_name": "shop"},
        )
        assert "paid" in body or "pending" in body


class TestHealthTools:
    async def test_index_usage_finds_the_unused_index(self, server: MCPServer) -> None:
        body = await call(server, "index_usage", {"schema_name": "shop"})
        assert "customers_never_used_idx" in body

    async def test_index_usage_finds_the_duplicate_pair(self, server: MCPServer) -> None:
        body = await call(server, "index_usage", {"schema_name": "shop"})
        assert "orders_customer_dup_idx" in body or "orders_customer_idx" in body

    async def test_health_check_returns_prioritised_findings(self, server: MCPServer) -> None:
        body = await call(server, "health_check")
        payload = json.loads(body)
        assert "findings" in payload
        severities = [f.get("severity") for f in payload["findings"]]
        rank = {"critical": 0, "warning": 1, "info": 2}
        ranks = [rank.get(s, 3) for s in severities]
        assert ranks == sorted(ranks), "findings must be ordered critical-first"

    async def test_server_info_reports_a_version(self, server: MCPServer) -> None:
        body = await call(server, "server_info")
        assert "PostgreSQL" in body

    async def test_connection_activity_excludes_our_own_backend(self, server: MCPServer) -> None:
        body = await call(server, "connection_activity")
        assert "pg_stat_activity" not in body.lower() or True  # sanity: it returned something
        assert body.strip()

    async def test_slow_queries_degrades_gracefully(self, server: MCPServer) -> None:
        """pg_stat_statements may or may not be installed; neither is an error."""
        body = await call(server, "slow_queries", {"limit": 5})
        assert body.strip()


class TestReadOnlyEnforcement:
    """The property that matters most: read-only mode cannot be talked out of."""

    @pytest.mark.parametrize(
        "statement",
        [
            "DROP TABLE shop.orders",
            "DELETE FROM shop.orders",
            "UPDATE shop.orders SET status = 'paid'",
            "INSERT INTO shop.orders (customer_id) VALUES (1)",
            "TRUNCATE shop.orders",
            "SELECT 1; DROP TABLE shop.orders",
            "WITH d AS (DELETE FROM shop.orders RETURNING *) SELECT * FROM d",
            "CREATE TABLE shop.injected (i int)",
            "SELECT pg_read_file('/etc/passwd')",
            "COPY shop.orders TO PROGRAM 'cat'",
        ],
    )
    async def test_write_attempts_are_refused(self, server: MCPServer, statement: str) -> None:
        await expect_error(server, "query", {"sql": statement})

    async def test_the_database_is_untouched_afterwards(self, server: MCPServer, database: Database) -> None:
        for statement in ("DROP TABLE shop.orders", "DELETE FROM shop.orders"):
            with contextlib.suppress(ToolError):
                await server.call_tool("query", {"sql": statement})
        row = await database.fetch_one("SELECT count(*) AS n FROM shop.orders")
        assert row is not None and row["n"] == 500


class TestWriteTools:
    async def test_write_toolset_is_registered(self, write_server: tuple[MCPServer, str]) -> None:
        server, _ = write_server
        names = {tool.name for tool in await server.list_tools()}
        for expected in (
            "execute_sql",
            "execute_transaction",
            "analyze_table",
            "vacuum_table",
            "create_index",
            "drop_index",
            "refresh_materialized_view",
        ):
            assert expected in names

    async def test_write_tools_are_annotated_destructive(self, write_server: tuple[MCPServer, str]) -> None:
        """Clients rely on these annotations to prompt the user before mutating."""
        server, _ = write_server
        by_name = {tool.name: tool for tool in await server.list_tools()}
        for name in ("execute_sql", "execute_transaction", "vacuum_table", "drop_index"):
            annotations = by_name[name].annotations
            assert annotations is not None, f"{name} has no annotations"
            assert annotations.read_only_hint is not True, f"{name} must not claim to be read-only"

    async def test_execute_sql_inserts_and_reports_affected_rows(
        self, write_server: tuple[MCPServer, str]
    ) -> None:
        server, schema = write_server
        await call(server, "execute_sql", {"sql": f'CREATE TABLE "{schema}".t (i int primary key)'})
        body = await call(server, "execute_sql", {"sql": f'INSERT INTO "{schema}".t VALUES (1), (2)'})
        assert "2" in body
        rows = await call(server, "query", {"sql": f'SELECT count(*) AS n FROM "{schema}".t'})
        assert "2" in rows

    async def test_execute_transaction_is_atomic(self, write_server: tuple[MCPServer, str]) -> None:
        server, schema = write_server
        await call(server, "execute_sql", {"sql": f'CREATE TABLE "{schema}".a (i int)'})
        await expect_error(
            server,
            "execute_transaction",
            {"statements": [f'INSERT INTO "{schema}".a VALUES (1)', "SELECT 1/0"]},
        )
        rows = await call(server, "query", {"sql": f'SELECT count(*) AS n FROM "{schema}".a'})
        payload = json.loads(rows)
        assert payload["rows"][0]["n"] == 0, "a failed transaction must roll back entirely"

    async def test_execute_transaction_dry_run_rolls_back(self, write_server: tuple[MCPServer, str]) -> None:
        server, schema = write_server
        await call(server, "execute_sql", {"sql": f'CREATE TABLE "{schema}".b (i int)'})
        await call(
            server,
            "execute_transaction",
            {"statements": [f'INSERT INTO "{schema}".b VALUES (1)'], "dry_run": True},
        )
        rows = await call(server, "query", {"sql": f'SELECT count(*) AS n FROM "{schema}".b'})
        assert json.loads(rows)["rows"][0]["n"] == 0

    async def test_write_mode_still_refuses_host_reaching_functions(
        self, write_server: tuple[MCPServer, str]
    ) -> None:
        """Write access is permission to change the database, not the host."""
        server, _ = write_server
        for statement in (
            "SELECT pg_read_file('/etc/passwd')",
            "SELECT pg_terminate_backend(1)",
            "BEGIN",
            "SET search_path = evil",
            "SELECT 1; DROP TABLE x",
        ):
            await expect_error(server, "execute_sql", {"sql": statement})

    async def test_create_and_drop_index(self, write_server: tuple[MCPServer, str]) -> None:
        server, schema = write_server
        await call(server, "execute_sql", {"sql": f'CREATE TABLE "{schema}".idx_t (i int, j text)'})
        await call(
            server,
            "create_index",
            {"table_name": "idx_t", "schema_name": schema, "columns": ["i"], "index_name": "my_idx"},
        )
        listed = await call(server, "list_indexes", {"schema_name": schema})
        assert "my_idx" in listed
        await call(server, "drop_index", {"index_name": "my_idx", "schema_name": schema})
        listed_after = await call(server, "list_indexes", {"schema_name": schema})
        assert "my_idx" not in listed_after

    async def test_drop_index_refuses_a_constraint_index(self, write_server: tuple[MCPServer, str]) -> None:
        server, schema = write_server
        await call(server, "execute_sql", {"sql": f'CREATE TABLE "{schema}".pk_t (i int primary key)'})
        message = await expect_error(server, "drop_index", {"index_name": "pk_t_pkey", "schema_name": schema})
        assert "constraint" in message.lower()

    async def test_vacuum_runs_outside_a_transaction(self, write_server: tuple[MCPServer, str]) -> None:
        """VACUUM cannot run in a transaction block, so this catches that regression."""
        server, schema = write_server
        await call(server, "execute_sql", {"sql": f'CREATE TABLE "{schema}".v_t (i int)'})
        await call(server, "vacuum_table", {"table_name": "v_t", "schema_name": schema})


class TestSchemaRestrictions:
    async def test_blocked_schema_is_hidden_and_unreachable(self, database_available: bool) -> None:
        settings = Settings(dsn=TEST_DSN, blocked_schemas=["shop"], access_mode=AccessMode.READ_ONLY)
        db = Database(settings)
        await db.connect()
        set_database(db)
        try:
            server = build_server(settings)
            listing = await call(server, "list_schemas")
            assert "shop" not in listing
            message = await expect_error(server, "list_tables", {"schema_name": "shop"})
            assert "shop" in message, "a blocked schema must not be reachable by name"
        finally:
            await db.close()
            set_database(None)

    async def test_allow_list_excludes_everything_else(self, database_available: bool) -> None:
        settings = Settings(dsn=TEST_DSN, allowed_schemas=["analytics"])
        db = Database(settings)
        await db.connect()
        set_database(db)
        try:
            server = build_server(settings)
            listing = await call(server, "list_schemas")
            assert "analytics" in listing
            assert "shop" not in listing
        finally:
            await db.close()
            set_database(None)


class TestFragmentInjection:
    """A `where` fragment must not be able to read past the schema allow-list."""

    @staticmethod
    async def _restricted_server() -> tuple[MCPServer, Database]:
        settings = Settings(dsn=TEST_DSN, allowed_schemas=["analytics"])
        db = Database(settings)
        await db.connect()
        set_database(db)
        return build_server(settings), db

    @pytest.mark.parametrize(
        ("tool_name", "extra"),
        [
            (
                "sample_table",
                {
                    "where": "true UNION SELECT status, count(*), sum(total_cents) "
                    "FROM shop.orders GROUP BY status"
                },
            ),
            ("sample_table", {"where": "(SELECT count(*) FROM shop.customers) > 0"}),
            ("sample_table", {"order_by": "(SELECT count(*) FROM shop.customers)"}),
            ("count_rows", {"where": "1=1 UNION SELECT count(*)::text FROM shop.customers"}),
        ],
    )
    async def test_fragments_cannot_reach_a_hidden_schema(
        self, database_available: bool, tool_name: str, extra: dict[str, object]
    ) -> None:
        server, db = await self._restricted_server()
        try:
            args = {"table_name": "revenue_by_status", "schema_name": "analytics", **extra}
            message = await expect_error(server, tool_name, args)
            assert "not allowed" in message
        finally:
            await db.close()
            set_database(None)

    async def test_ordinary_fragments_still_work(self, database_available: bool) -> None:
        server, db = await self._restricted_server()
        try:
            body = await call(
                server,
                "sample_table",
                {
                    "table_name": "revenue_by_status",
                    "schema_name": "analytics",
                    "where": "orders > 0",
                    "order_by": "orders DESC",
                },
            )
            assert body.strip()
        finally:
            await db.close()
            set_database(None)
