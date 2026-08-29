# Recipes

Things worth asking once pgmcp is connected, and what happens under the hood.

## Understanding an unfamiliar database

> "What's in this database? Give me an overview of the data model."

The model calls `list_schemas`, then `list_tables` per schema, then `get_relationships` to see how
they join. Asking for a diagram gets you `get_relationships(as_mermaid=True)`, which renders as an
ERD in any client that supports Mermaid.

> "Explain the shop schema to me."

Use the `explain_schema` prompt — it embeds the real tables, columns and foreign keys before asking
for the explanation, so the answer describes your schema rather than a plausible-sounding one.

## Writing queries that are correct the first time

> "How many orders did each customer place last month?"

A well-behaved model calls `describe_table` first, sees that `shop.orders.customer_id` references
`shop.customers.id` and that `placed_at` is `timestamptz`, then writes the query with real column
names and runs it through `query`.

If you want to force that order explicitly, use the `write_sql` prompt.

**Parameterise values.** `query` takes a `params` list bound by the driver:

```
query(sql="SELECT * FROM shop.orders WHERE customer_id = %s::bigint AND status = %s",
      params=["42", "paid"])
```

Values arrive as strings, so cast in the SQL when the column is not text.

## Making a slow query fast

> "This query takes 8 seconds. Why?"

Use the `optimize_query` prompt, or ask directly. The model runs `explain_query(sql, analyze=True,
buffers=True)`, which returns the plan plus a summary highlighting nodes where the planner's row
estimate diverged sharply from reality — usually the actual cause. It then checks `index_usage` for
missing-index candidates and `table_stats` for whether the table is being sequentially scanned, and
proposes the `CREATE INDEX` statement.

Verify before you apply: `explain_query` on the rewritten query, and `create_index(...)` only in
write mode.

## Operational triage

> "The database feels slow right now."

```
health_check()
```

One call, run concurrently across every check, returning prioritised findings — cache hit ratio,
connection saturation, idle-in-transaction sessions, blocked sessions, dead tuples, transaction-ID
wraparound risk, invalid indexes, deadlocks. Critical findings come first.

> "Something is stuck."

```
blocking_locks()
```

Returns the waiting graph: which session is blocked, on what relation and lock mode, and which
session is holding it — with both queries side by side. pgmcp will not call `pg_terminate_backend`
itself; it gives you the pid to terminate.

> "What's using all the disk?"

`table_sizes()` for the largest relations, then `index_usage()` for reclaimable bytes tied up in
unused and duplicate indexes, then `vacuum_status()` for space that vacuum has not reclaimed yet.

## Data quality

> "Audit the customers table."

The `data_quality_audit` prompt embeds the real columns and constraints, then has the model profile
each one with `column_stats` — looking for nulls in fields that should be required, orphaned foreign
keys, duplicate business keys, outliers and inconsistent formatting — and report each finding with
the SQL that demonstrates it.

`column_stats` combines planner statistics from `pg_stats` (cardinality, most common values,
histogram) with a live aggregate query, so it is fast on large tables and still exact where it
matters.

## Working across databases

Tools take an optional `database` argument. pgmcp opens a separate pool per database, and you can
restrict which are reachable:

```bash
pgmcp --dsn postgresql://user:pw@host/app --allowed-databases app,reporting
```

Or refuse it entirely with `--no-cross-database`.

## Keeping the tool list small

Every tool costs context. If you only need introspection:

```bash
pgmcp --toolsets schema,query
```

## Making changes (write mode)

```bash
pgmcp --access-mode write --allowed-schemas staging
```

Validate before committing:

```
execute_transaction(
  statements=["ALTER TABLE staging.orders ADD COLUMN note text",
              "UPDATE staging.orders SET note = 'backfill' WHERE note IS NULL"],
  dry_run=True)
```

`dry_run` runs everything and then rolls back, so you see the real affected-row counts and any
constraint violation without committing. Run it again with `dry_run=False` to apply.

Write mode still refuses transaction control, session `SET`, multi-statement input, and the
filesystem functions — write access means permission to change *the database*, not the host.

## Troubleshooting

**"No database connection configured"** — set `PGMCP_DSN` or `DATABASE_URL`, or pass `--dsn`.
Check it with `pgmcp --check`.

**"refused in read-only mode"** — working as intended. Restart with `--access-mode write` if you
mean it.

**"canceling statement due to statement timeout"** — the query exceeded
`PGMCP_STATEMENT_TIMEOUT_MS` (30s by default). Raise it, or narrow the query.

**Results say `truncated: true`** — you hit `max_rows`. Page with `LIMIT`/`OFFSET` rather than
raising the cap; the cap exists to protect the context window.

**`slow_queries` says the extension is missing** — `pg_stat_statements` needs to be in
`shared_preload_libraries` (a server restart) and then `CREATE EXTENSION pg_stat_statements`.

**Nothing appears in the client** — pgmcp logs to stderr, never stdout, because stdout is the stdio
transport. Run `pgmcp --check` directly to see connection errors, and `--log-level DEBUG` for more.
