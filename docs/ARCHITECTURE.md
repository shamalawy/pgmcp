# Architecture

How pgmcp is put together, and why it is put together that way.

## Layers

```
   MCP client
       │
       ▼
┌──────────────────────────────────────────────────────────┐
│ server.py        builds MCPServer, owns the lifespan     │
├──────────────────────────────────────────────────────────┤
│ tools/           schema · query · health · write         │  ← one register() each
│ resources.py     postgres:// URIs                        │
│ prompts.py       user-invocable templates                │
├──────────────────────────────────────────────────────────┤
│ tools/_helpers.py    format resolution, schema filters,  │
│                      identifier quoting, error wrapping  │
├──────────────────────────────────────────────────────────┤
│ database.py      pool registry, guarded execution        │  ← the only place SQL runs
│ sql_guard.py     static statement analysis               │
│ formatting.py    QueryResult → json / markdown / csv     │
│ config.py        Settings, access mode, toolsets         │
│ errors.py        exception hierarchy, DSN redaction      │
└──────────────────────────────────────────────────────────┘
       │
       ▼
   PostgreSQL
```

Dependencies point downward only. A tool module imports from `_helpers` and `database`; nothing in
the core imports a tool module. `server.py` imports the tool modules lazily inside `register_all`,
so `import pgmcp.server` works without a database and without paying for every catalog query module
at import time.

## Design decisions

### Read-only is enforced by PostgreSQL, not by pgmcp

Every read runs inside a transaction opened with `SET TRANSACTION READ ONLY`. The server then
rejects any write with SQLSTATE `25006`, whatever the statement looks like.

This ordering is deliberate. A string-matching guard is a filter with an unbounded set of bypasses;
a read-only transaction is a property of the session that holds no matter what SQL arrives. The
static guard in `sql_guard.py` runs *first* only so that the caller gets a clear, actionable message
instead of a driver error — it is a usability layer that happens to also be a security layer, not
the other way round.

The guard does carry one genuinely load-bearing responsibility: a read-only transaction does **not**
stop functions that act outside MVCC. `pg_read_file`, `lo_export`, `dblink`, `pg_terminate_backend`
and `COPY … FROM PROGRAM` all run happily in a read-only transaction, because none of them modify
the database — they touch the server's filesystem, other sessions, or the network. Those are blocked
statically, in every access mode, because there is no lower layer that would catch them.

### Multi-statement input is refused

psycopg's simple-query path accepts `SELECT 1; DROP TABLE users` in a single `execute()` when no
parameters are bound. Splitting on `;` is not sufficient to detect this — semicolons appear inside
string literals, dollar-quoted function bodies and quoted identifiers. `sql_guard.split_statements`
uses sqlparse's lexer, which tokenises those correctly, and anything yielding more than one
statement is rejected before it reaches the driver.

### Row limits over-fetch instead of rewriting SQL

Wrapping user SQL in `SELECT * FROM (…) LIMIT n` breaks statements that already have a `LIMIT`, an
`ORDER BY … FOR UPDATE`, or a `RETURNING` clause, and changes the plan the user is trying to
inspect. Instead `Database.fetch` requests `limit + 1` rows and reports `truncated: true` when it
gets them. The semantics of the submitted statement are never altered, and the truncation flag is
exact rather than inferred.

### Timeouts use `SET LOCAL`

`statement_timeout`, `lock_timeout` and `idle_in_transaction_session_timeout` are set inside the
transaction with `SET LOCAL`, which reverts at transaction end. A pooled connection is therefore
never handed to its next borrower carrying a modified timeout — a bug that is easy to introduce with
plain `SET` and very hard to notice afterwards.

### One pool per database

`Database` keeps a registry of `AsyncConnectionPool` keyed by database name, created lazily and
guarded by an `asyncio.Lock` with double-checked locking. Targeting another database rebuilds the
DSN through `psycopg.conninfo` rather than string-splicing, so an unusual password or host cannot
corrupt the result. Cross-database access is gated by `allow_cross_database` and
`allowed_databases`.

### The database handle is a module singleton

Tool functions call `get_database()` rather than reaching into
`ctx.request_context.lifespan_context`. The lifespan context is only available inside a live MCP
request, which would make every tool untestable without constructing a session. The singleton is
installed by the server lifespan and cleared on shutdown; tests install it directly.

### Tools return strings

Returning `str` rather than a Pydantic model keeps the tool JSON schemas small — worth real context
budget across 30+ tools — and renders correctly in clients that ignore `structuredContent`. The
`format` argument (`json` / `markdown` / `csv`) lets the caller choose the shape at the point of
use, and all three go through the same byte budget in `formatting.render`.

### Errors become `ToolError`

The `@tool` decorator in `tools/_helpers.py` converts any `PgMcpError` into the SDK's `ToolError`,
which reaches the model as `is_error=True` with readable text it can act on, and is logged
server-side at INFO without a traceback. Anything that is *not* a `PgMcpError` is left to propagate:
an unexpected exception is a bug, and the SDK deliberately withholds its text from the client while
logging the traceback.

## Adding a tool

1. Pick the module matching its toolset (`tools/schema.py`, `query.py`, `health.py`, `write.py`).
2. Define it inside that module's `register(mcp, settings)`:

```python
@mcp.tool(annotations=ToolAnnotations(read_only_hint=True, destructive_hint=False))
@tool
async def my_tool(schema_name: str = "public", format: str | None = None) -> str:
    """One line the model reads when choosing a tool.

    A paragraph on when to use this and what to do with the result.

    Args:
        schema_name: Schema to inspect.
        format: "json" (default), "markdown", or "csv".
    """
    validate_schema(schema_name, settings)
    where, params = schema_predicate("n.nspname", settings)
    result = await db().fetch(f"SELECT … WHERE true {where}", params)
    return show(result, format, settings)
```

`@mcp.tool` must be the outermost decorator. The docstring is the model-facing documentation, not a
note to future maintainers — write it for something deciding whether to call this tool.

3. Bind every client value as `%s`. Where PostgreSQL will not accept a bind parameter, use
   `ident()` with `psycopg.sql.SQL(...).format(...)`, or bind `regclass(schema, table)` to
   `to_regclass(%s)`. The only thing that may be f-stringed into SQL is a fragment produced by
   `schema_predicate` or a column name your own code chose from a fixed allow-list.

4. Add a test in `tests/test_tools.py` that asserts on real values from the fixture database, not
   just that the call succeeded.

## Test fixtures

`tests/fixtures/seed.sql` builds a database that deliberately exercises the awkward parts of the
catalog: multiple schemas including one needing quotes, composite and self-referencing foreign keys,
range partitioning, table inheritance, generated and identity columns, enums, domains, composite
types, ranges, arrays, jsonb, row-level security, triggers, materialized views, and indexes that are
variously partial, expression-based, duplicated and never used.

If a change to an introspection tool does not break anything, it is worth asking whether the fixture
covers the case at all.
