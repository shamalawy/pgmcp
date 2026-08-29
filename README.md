# pgmcp

A fully featured [Model Context Protocol](https://modelcontextprotocol.io) server for PostgreSQL.

It gives an AI assistant a real understanding of your database: the catalog, the data, the query
plans, and the health of the server — without giving it the ability to break anything.

```
┌──────────────┐        MCP         ┌─────────┐      libpq      ┌────────────┐
│ Claude Code  │ ◄────────────────► │  pgmcp  │ ◄─────────────► │ PostgreSQL │
│ Claude Desktop│  stdio / HTTP     │         │  read-only txn  │            │
│ Cursor, ...  │                    └─────────┘                 └────────────┘
└──────────────┘
```

**Read-only by default.** Every statement runs inside a PostgreSQL `READ ONLY` transaction, so
writes are refused by the server itself — not merely by a string check that a cleverly worded
prompt can talk its way around.

---

## Contents

- [Why pgmcp](#why-pgmcp)
- [Quick start](#quick-start)
- [Client configuration](#client-configuration)
- [Tools](#tools)
- [Resources](#resources)
- [Prompts](#prompts)
- [Configuration](#configuration)
- [Security model](#security-model)
- [Docker](#docker)
- [Development](#development)

---

## Why pgmcp

Most database MCP servers give you `list_tables` and `run_query` and stop there. That is enough for
a demo and not enough for real work — the model still guesses at column names, has no idea why a
query is slow, and cannot tell you that your database is one unvacuumed table away from trouble.

pgmcp covers the whole surface:

| Area | What you get |
|---|---|
| **Introspection** | Schemas, tables, columns, constraints, indexes, triggers, RLS policies, partitions, inheritance, enums, domains, composite types, sequences, functions, extensions, roles |
| **Understanding** | `describe_table` returns everything about a relation in one call; `get_table_ddl` reconstructs runnable DDL; `get_relationships` renders the foreign-key graph as a Mermaid ERD |
| **Querying** | Parameterised SELECTs, `EXPLAIN (ANALYZE)` with plan analysis, table sampling, column profiling against `pg_stats` |
| **Operations** | Cache hit ratios, connection activity, blocking-lock graphs, unused and duplicate indexes, bloat and vacuum status, `pg_stat_statements`, replication lag, and a single prioritised `health_check` |
| **Writes** | Off by default. When enabled: guarded statements, atomic multi-statement transactions with a `dry_run` mode, and safe index/vacuum helpers |

Everything is bounded. Row counts, result bytes, statement timeouts and lock timeouts all have
limits you control, so a careless `SELECT *` cannot exhaust a context window or pin a connection.

---

## Quick start

### Run it without installing

```bash
uvx pgmcp --dsn postgresql://user:password@localhost:5432/mydb
```

### Install

```bash
uv tool install pgmcp     # or: pipx install pgmcp / pip install pgmcp
```

### Check your connection before wiring up a client

```bash
pgmcp --dsn postgresql://user:password@localhost:5432/mydb --check
```

```json
{
  "pgmcp_version": "0.1.0",
  "settings": { "dsn": "postgresql://user:***@localhost:5432/mydb", "access_mode": "read-only", ... },
  "connection": { "ok": true, "database": "mydb", "user": "readonly_ai", "in_recovery": false },
  "visible_schemas": ["public", "shop"]
}
```

The password is redacted everywhere pgmcp prints a DSN — in `--check`, in logs, and in error
messages.

---

## Client configuration

### Claude Code

```bash
claude mcp add postgres -- uvx pgmcp --dsn "postgresql://user:password@localhost:5432/mydb"
```

### Claude Desktop / Cursor / any `mcp.json`

```json
{
  "mcpServers": {
    "postgres": {
      "command": "uvx",
      "args": ["pgmcp"],
      "env": {
        "PGMCP_DSN": "postgresql://user:password@localhost:5432/mydb",
        "PGMCP_ACCESS_MODE": "read-only"
      }
    }
  }
}
```

Passing the DSN through `env` rather than `args` keeps the password out of your process list.

### Remote / containerised (HTTP)

```bash
pgmcp --transport streamable-http --host 0.0.0.0 --port 8000
```

```json
{
  "mcpServers": {
    "postgres": { "type": "http", "url": "http://localhost:8000/mcp" }
  }
}
```

`--transport sse` is also available for clients that predate streamable HTTP.

---

## Tools

Grouped into four toolsets. Enable a subset with `--toolsets schema,query` when you want a smaller
tool list in the model's context.

### `schema` — catalog introspection

| Tool | What it answers |
|---|---|
| `list_schemas` | What schemas exist, who owns them, how big they are |
| `list_tables` | Tables in a schema with row estimates, sizes, comments, RLS status |
| `describe_table` | **Everything** about one relation: columns, types, defaults, identity and generated expressions, primary key, foreign keys, unique and check constraints, indexes, triggers, RLS policies, partition bounds, inheritance |
| `get_table_ddl` | Best-effort runnable `CREATE TABLE` + indexes + comments |
| `list_views` | Views and materialized views, optionally with definitions |
| `list_indexes` | Index definitions, uniqueness, validity, size, scan counts |
| `list_functions` | Functions, procedures, aggregates with signatures and volatility |
| `list_types` | Enums with labels, composites with attributes, domains, ranges |
| `list_sequences` | Sequences with current value and owning column |
| `list_extensions` | Installed and available extensions |
| `list_databases` | Databases with sizes, encodings, connection limits |
| `list_roles` | Roles and their attributes (never password hashes) |
| `search_objects` | Find tables, columns, functions, indexes, types by pattern |
| `get_relationships` | The foreign-key graph, optionally as a Mermaid ERD |

### `query` — reading data

| Tool | What it answers |
|---|---|
| `query` | Run a parameterised SELECT |
| `explain_query` | `EXPLAIN (ANALYZE, BUFFERS)` with a summary of cost, timing and estimate-vs-actual mismatches |
| `sample_table` | Preview rows, with column selection, filtering and random sampling |
| `count_rows` | Exact count, or the instant `reltuples` estimate for huge tables |
| `column_stats` | Profile a column: nulls, cardinality, min/max, most common values, histogram |
| `table_sizes` | What is taking up the space |

### `health` — operations

| Tool | What it answers |
|---|---|
| `health_check` | **Start here.** Every check below, run concurrently, returned as prioritised findings with severities |
| `server_info` | Version, uptime, notable settings, connection saturation |
| `database_stats` | Cache hit ratio, commit ratio, deadlocks, temp file usage |
| `connection_activity` | Who is connected and what they are running |
| `long_running_queries` | Queries and idle-in-transaction sessions past a threshold |
| `blocking_locks` | The lock waiting graph — who is blocking whom |
| `index_usage` | Unused, duplicate and low-value indexes, plus missing-index candidates and reclaimable bytes |
| `table_stats` | Scan patterns, dead tuples, vacuum and analyze history |
| `vacuum_status` | Tables needing vacuum, transaction-ID wraparound risk, vacuums in progress |
| `slow_queries` | Top queries from `pg_stat_statements` |
| `replication_status` | Primary or standby, replica lag, replication slots |

### `write` — mutations (opt in)

Registered only when the server is started with `--access-mode write`.

| Tool | What it does |
|---|---|
| `execute_sql` | One INSERT/UPDATE/DELETE/MERGE/DDL statement |
| `execute_transaction` | Several statements atomically, with `dry_run` to validate without committing |
| `analyze_table` | Refresh planner statistics |
| `vacuum_table` | Reclaim space (warns about the `FULL` lock) |
| `create_index` / `drop_index` | Index management, `CONCURRENTLY` by default |
| `refresh_materialized_view` | Refresh, with a `CONCURRENTLY` precondition check |

---

## Resources

Addressable context a client can attach without spending a tool call:

| URI | Contents |
|---|---|
| `postgres://server/info` | Version, uptime, size, access mode |
| `postgres://databases` | Databases with sizes |
| `postgres://schemas` | Visible schemas with object counts |
| `postgres://{schema}/tables` | Tables with row estimates and sizes |
| `postgres://{schema}/{table}/columns` | Column definitions |
| `postgres://{schema}/{table}/ddl` | Reconstructed DDL |
| `postgres://database/overview` | A bounded map of the whole database |

## Prompts

User-invocable templates that embed live schema context at call time:

| Prompt | Purpose |
|---|---|
| `write_sql` | Write a correct query for a stated goal against the real schema |
| `optimize_query` | Diagnose a slow query and propose concrete indexes or rewrites |
| `explain_schema` | Explain a data model in plain language |
| `database_health_report` | Produce a prioritised operational report with remediation |
| `data_quality_audit` | Profile a table and report nulls, orphans, duplicates and outliers |

---

## Configuration

Every setting is available as a CLI flag and as a `PGMCP_`-prefixed environment variable.
Precedence is: **CLI flag → environment variable → `.env` file → default.**

### Connection

| Variable | Flag | Default | Purpose |
|---|---|---|---|
| `PGMCP_DSN` | `--dsn` | — | libpq connection string. Falls back to `DATABASE_URL`, then `PGHOST`/`PGUSER`/`PGDATABASE`/… |
| `PGMCP_APPLICATION_NAME` | `--application-name` | `pgmcp` | Shows up in `pg_stat_activity` |
| `PGMCP_POOL_MIN_SIZE` | `--pool-min-size` | `1` | Pool floor |
| `PGMCP_POOL_MAX_SIZE` | `--pool-max-size` | `5` | Pool ceiling |
| `PGMCP_CONNECT_TIMEOUT` | `--connect-timeout` | `10` | Seconds |

### Safety

| Variable | Flag | Default | Purpose |
|---|---|---|---|
| `PGMCP_ACCESS_MODE` | `--access-mode` | `read-only` | `read-only` hides every mutating tool; `write` enables them |
| `PGMCP_STATEMENT_TIMEOUT_MS` | `--statement-timeout` | `30000` | Per-statement timeout, applied with `SET LOCAL` |
| `PGMCP_MAX_ROWS` | `--max-rows` | `500` | Hard cap on rows per call |
| `PGMCP_MAX_RESULT_BYTES` | `--max-result-bytes` | `1000000` | Byte budget for a serialised result |
| `PGMCP_ALLOWED_SCHEMAS` | `--allowed-schemas` | all | Comma-separated allow-list |
| `PGMCP_BLOCKED_SCHEMAS` | `--blocked-schemas` | — | Comma-separated deny-list |
| `PGMCP_INCLUDE_SYSTEM_SCHEMAS` | `--include-system-schemas` | `false` | Expose `pg_catalog` / `information_schema` |
| `PGMCP_ALLOWED_DATABASES` | `--allowed-databases` | all | Restrict cross-database access |
| `PGMCP_ALLOW_CROSS_DATABASE` | `--no-cross-database` | `true` | Allow tools to target another database |
| `PGMCP_ENABLED_TOOLSETS` | `--toolsets` | all | `schema,query,health,write` |

### Output and transport

| Variable | Flag | Default | Purpose |
|---|---|---|---|
| `PGMCP_DEFAULT_FORMAT` | `--format` | `json` | `json`, `markdown` or `csv` |
| `PGMCP_DEFAULT_ROW_LIMIT` | `--default-row-limit` | `100` | Limit used when a call omits one |
| `PGMCP_TRANSPORT` | `--transport` | `stdio` | `stdio`, `streamable-http`, `sse` |
| `PGMCP_HOST` / `PGMCP_PORT` | `--host` / `--port` | `127.0.0.1:8000` | HTTP bind address |
| `PGMCP_MOUNT_PATH` | `--mount-path` | `/mcp` | HTTP endpoint path |
| `PGMCP_LOG_LEVEL` | `--log-level` | `INFO` | Logs go to stderr, never stdout |

See [`.env.example`](.env.example) for a complete annotated file.

---

## Security model

pgmcp is built on the assumption that the model on the other end is not adversarial but *is*
fallible, and that the prompt reaching it may not be trustworthy.

**Layer 1 — the database enforces read-only.** Reads run inside a transaction opened with
`SET TRANSACTION READ ONLY`. PostgreSQL rejects any write with SQLSTATE `25006` regardless of how
the statement is phrased. This is the layer that actually matters: it holds even if every check
above it is wrong.

**Layer 2 — static analysis.** Statements are parsed before execution to reject multi-statement
input (`SELECT 1; DROP TABLE users`), data-modifying CTEs (`WITH x AS (DELETE …) SELECT …`),
transaction and session control on pooled connections, and functions that reach *outside* the
transaction's protection — `pg_read_file`, `lo_export`, `dblink`, `pg_terminate_backend`,
`COPY … FROM PROGRAM`. Those last ones are the important case: a read-only transaction does not
stop them, so the guard must.

**Layer 3 — bounded execution.** `statement_timeout`, `lock_timeout` and
`idle_in_transaction_session_timeout` are set per transaction with `SET LOCAL`, so nothing leaks
onto the next borrower of a pooled connection. Row and byte caps are applied to every result.

**Layer 4 — parameterisation.** Client values are always bound as parameters. Identifiers that
cannot be bound go through psycopg's `sql.Identifier` quoting, never string formatting.

**Layer 5 — configuration.** Schema allow/deny lists, database allow-lists, and toolset selection
narrow the surface before anything runs.

### Use a least-privileged role anyway

Defence in depth is not a substitute for correct database permissions. Give pgmcp its own role:

```sql
CREATE ROLE readonly_ai LOGIN PASSWORD 'change-me';
GRANT CONNECT ON DATABASE mydb TO readonly_ai;
GRANT USAGE ON SCHEMA public TO readonly_ai;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly_ai;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_ai;

-- Keep the model out of anything it has no business reading.
REVOKE ALL ON TABLE public.user_credentials FROM readonly_ai;
```

If your data is sensitive, combine this with `--allowed-schemas` and consider row-level security.

### Write mode

`--access-mode write` lets a language model change your data. Use it deliberately: scope it with a
restricted role and `--allowed-schemas`, prefer `execute_transaction` with `dry_run` first, and do
not point it at production without a backup you have actually tested.

---

## Docker

```bash
docker compose up -d
```

Brings up `pgmcp_postgres` (seeded with the demo schema, published on host port
`5433` so it does not collide with a local PostgreSQL) and `pgmcp_server` serving
streamable HTTP on `http://localhost:8000/mcp`.

Standalone:

```bash
docker run --rm -p 8000:8000 \
  -e PGMCP_DSN="postgresql://user:password@host.docker.internal:5432/mydb" \
  ghcr.io/shamalawy/pgmcp:latest
```

---

## Development

```bash
git clone https://github.com/shamalawy/pgmcp && cd pgmcp
uv sync

docker compose up -d postgres           # a PostgreSQL with the fixture schema
export PGMCP_TEST_DSN=postgresql://postgres:postgres@localhost:5433/pgmcp_demo

uv run pytest                           # tests
uv run ruff check . && uv run mypy      # lint and types
```

Further reading:

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — how the layers fit together and why
- [docs/RECIPES.md](docs/RECIPES.md) — worked examples and troubleshooting
- [CONTRIBUTING.md](CONTRIBUTING.md) — the tool-authoring contract and release process
- [SECURITY.md](SECURITY.md) — threat model and vulnerability reporting

## License

MIT — see [LICENSE](LICENSE).
