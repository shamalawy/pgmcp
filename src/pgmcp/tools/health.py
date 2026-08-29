"""Server and database observability tools: activity, locks, bloat, index usage."""

from __future__ import annotations

import asyncio
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations
from psycopg import sql

from ..config import Settings
from ._helpers import as_json, db, schema_predicate, show, tool, validate_schema

READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True)


def register(mcp: MCPServer, settings: Settings) -> None:
    """Register observability tools on the server."""

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def server_info() -> str:
        """Get server version, uptime, configuration and connection limits.

        Returns PostgreSQL version (numeric and text), database and user names,
        uptime since postmaster start, recovery status, data checksums, database
        and total cluster size, connection limits and usage, and critical tuning
        parameters (shared_buffers, work_mem, cache settings, WAL, autovacuum).
        """
        row = await db().fetch_one(
            sql.SQL("""
            SELECT
                version() AS version,
                current_setting('server_version_num')::int AS server_version_num,
                current_database() AS database,
                current_user AS user,
                now() - pg_postmaster_start_time() AS uptime,
                pg_is_in_recovery() AS in_recovery,
                current_setting('data_checksums') AS data_checksums,
                (SELECT count(*) FROM pg_database) AS num_databases,
                pg_database_size(current_database()) AS database_size_bytes,
                sum(pg_database_size(datname)) OVER () AS total_size_bytes,
                current_setting('max_connections')::int AS max_connections,
                (SELECT count(*) FROM pg_stat_activity) AS current_connections,
                current_setting('shared_buffers') AS shared_buffers,
                current_setting('work_mem') AS work_mem,
                current_setting('maintenance_work_mem') AS maintenance_work_mem,
                current_setting('effective_cache_size') AS effective_cache_size,
                current_setting('max_wal_size') AS max_wal_size,
                current_setting('random_page_cost') AS random_page_cost,
                current_setting('default_statistics_target')::int AS default_statistics_target,
                current_setting('autovacuum') AS autovacuum,
                current_setting('wal_level') AS wal_level
            FROM pg_database
            LIMIT 1
        """)
        )
        return as_json(row, settings) if row else as_json({}, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def database_stats(database: str | None = None, format: str | None = None) -> str:
        """Show database-level statistics from pg_stat_database.

        Returns size, connection count, commit/rollback counts with commit ratio,
        blocks read vs cache hits with cache hit ratio as a percentage, tuple
        stats (returned/fetched/inserted/updated/deleted), conflicts, deadlocks,
        and temp file activity. Flags cache hit ratio below 95% as a concern.

        Args:
            database: Database name. Defaults to current database.
            format: "json" (default), "markdown", or "csv".
        """
        where_clause = "" if database is None else "WHERE datname = %s"
        params = [] if database is None else [database]

        result = await db().fetch(
            sql.SQL(f"""
            SELECT
                datname,
                pg_database_size(datname) AS size_bytes,
                numbackends AS backends,
                xact_commit,
                xact_rollback,
                ROUND(((xact_commit::float / NULLIF(xact_commit + xact_rollback, 0)) * 100)::numeric, 2)
                    AS commit_ratio_percent,
                blks_read,
                blks_hit,
                ROUND(((blks_hit::float / NULLIF(blks_hit + blks_read, 0)) * 100)::numeric, 2)
                    AS cache_hit_ratio_percent,
                tup_returned,
                tup_fetched,
                tup_inserted,
                tup_updated,
                tup_deleted,
                conflicts,
                deadlocks,
                temp_files,
                temp_bytes,
                stats_reset
            FROM pg_stat_database
            {where_clause}
            ORDER BY datname
        """),
            params,
            limit=settings.max_rows,
        )
        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def connection_activity(
        state: str | None = None,
        database: str | None = None,
        include_idle: bool = False,
        limit: int | None = None,
        format: str | None = None,
    ) -> str:
        """Show active connections and their queries from pg_stat_activity.

        Returns pid, user, application name, client address, state, wait events,
        and query text (truncated, with "insufficient privilege" noted when query
        is NULL). Excludes the server's own backend and background workers unless
        idle connections are explicitly included. Includes a state summary with
        counts per state.

        Args:
            state: Filter by state (e.g., "active", "idle", "idle in transaction").
            database: Limit to this database name.
            include_idle: Include idle connections and background workers.
            limit: Maximum rows to return.
            format: "json" (default), "markdown", or "csv".
        """
        where_clauses = ["pid <> pg_backend_pid()"]
        params: list = []

        if not include_idle:
            where_clauses.append("state IS NOT NULL")
            where_clauses.append("backend_type = 'client backend'")

        if state:
            where_clauses.append("state = %s")
            params.append(state)

        if database:
            where_clauses.append("datname = %s")
            params.append(database)

        where_fragment = " AND ".join(where_clauses)

        result = await db().fetch(
            sql.SQL(f"""
            SELECT
                pid,
                usename AS user,
                application_name,
                client_addr,
                state,
                wait_event_type,
                wait_event,
                now() - backend_start AS backend_age,
                now() - xact_start AS xact_age,
                now() - query_start AS query_age,
                CASE
                    WHEN query IS NULL THEN 'insufficient privilege'
                    WHEN length(query) > 200 THEN substr(query, 1, 200) || '...'
                    ELSE query
                END AS query_text
            FROM pg_stat_activity
            WHERE {where_fragment}
            ORDER BY query_start ASC
        """),
            params,
            limit=limit,
        )

        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def long_running_queries(
        min_duration_seconds: int = 30,
        include_idle_in_transaction: bool = True,
        limit: int | None = None,
        format: str | None = None,
    ) -> str:
        """Show long-running active queries from pg_stat_activity.

        Returns pid, duration, state, wait event, user, database and query text.
        By default includes idle-in-transaction sessions, which hold locks and
        block vacuum; set include_idle_in_transaction=False to exclude them.

        Args:
            min_duration_seconds: Only queries older than this many seconds.
            include_idle_in_transaction: Include idle-in-transaction sessions.
            limit: Maximum rows to return.
            format: "json" (default), "markdown", or "csv".
        """
        where_clauses = [
            "pid <> pg_backend_pid()",
            "query_start IS NOT NULL",
            "(now() - query_start) > (interval '1 second' * %s)",
        ]
        params = [min_duration_seconds]

        if not include_idle_in_transaction:
            where_clauses.append("state <> 'idle in transaction'")

        where_fragment = " AND ".join(where_clauses)

        result = await db().fetch(
            sql.SQL(f"""
            SELECT
                pid,
                EXTRACT(EPOCH FROM (now() - query_start))::int AS duration_seconds,
                state,
                wait_event_type,
                wait_event,
                usename AS user,
                datname AS database,
                CASE
                    WHEN query IS NULL THEN 'insufficient privilege'
                    WHEN length(query) > 200 THEN substr(query, 1, 200) || '...'
                    ELSE query
                END AS query_text
            FROM pg_stat_activity
            WHERE {where_fragment}
            ORDER BY query_start ASC
        """),
            params,
            limit=limit,
        )

        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def blocking_locks(limit: int | None = None, format: str | None = None) -> str:
        """Show the lock waiting graph: which sessions block which.

        For each blocked session, returns blocked pid/user/query/duration AND the
        blocking pid/user/query, plus the relation and lock mode involved. This
        shows the database is stuck and which session is holding the lock. To
        unblock, kill the blocking session with pg_terminate_backend (pgmcp
        itself does not permit this to prevent accidental service disruption).

        Args:
            limit: Maximum blocking relationships to return.
            format: "json" (default), "markdown", or "csv".
        """
        result = await db().fetch(
            sql.SQL("""
            SELECT
                blocked.pid AS blocked_pid,
                blocked.usename AS blocked_user,
                CASE
                    WHEN blocked.query IS NULL THEN 'insufficient privilege'
                    WHEN length(blocked.query) > 150 THEN substr(blocked.query, 1, 150) || '...'
                    ELSE blocked.query
                END AS blocked_query,
                EXTRACT(EPOCH FROM (now() - blocked.query_start))::int
                    AS blocked_duration_seconds,
                blocking.pid AS blocking_pid,
                blocking.usename AS blocking_user,
                CASE
                    WHEN blocking.query IS NULL THEN 'insufficient privilege'
                    WHEN length(blocking.query) > 150 THEN substr(blocking.query, 1, 150) || '...'
                    ELSE blocking.query
                END AS blocking_query,
                EXTRACT(EPOCH FROM (now() - blocking.query_start))::int
                    AS blocking_duration_seconds,
                l.relation::regclass::text AS relation,
                l.mode AS lock_mode
            FROM pg_locks l
            JOIN pg_stat_activity blocked ON l.pid = blocked.pid
            CROSS JOIN UNNEST(pg_blocking_pids(blocked.pid)) AS blocking_pid(pid)
            JOIN pg_stat_activity blocking ON blocking.pid = blocking_pid.pid
            WHERE NOT l.granted
            ORDER BY blocked.query_start ASC
        """),
            limit=limit,
        )

        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def index_usage(schema_name: str | None = None) -> str:
        """Comprehensive index health report: unused, duplicate, and missing indexes.

        Returns a JSON document with sections for:
        - unused_indexes: idx_scan = 0 (excluding constraint-backing indexes) with size
        - duplicate_indexes: same table with identical or prefix-matching columns
        - low_usage_indexes: low scan counts on large indexes
        - missing_index_candidates: tables with high seq scans relative to index scans
        Plus total reclaimable_bytes from unused indexes.

        Args:
            schema_name: Restrict to this schema. Defaults to all non-system schemas.
        """
        if schema_name:
            validate_schema(schema_name, settings)

        # Unused indexes
        schema_filter = "" if schema_name is None else "AND schemaname = %s"
        params_unused = [] if schema_name is None else [schema_name]

        unused = await db().fetch_all(
            sql.SQL(f"""
            SELECT
                schemaname,
                relname AS tablename,
                indexrelname AS indexname,
                idx_scan,
                pg_size_pretty(pg_relation_size(indexrelid)) AS size,
                pg_relation_size(indexrelid) AS size_bytes
            FROM pg_stat_user_indexes
            WHERE idx_scan = 0
            AND NOT EXISTS (
                SELECT 1 FROM pg_constraint c WHERE c.conindid = pg_stat_user_indexes.indexrelid
            )
            {schema_filter}
            ORDER BY pg_relation_size(indexrelid) DESC
        """),
            params_unused,
        )

        # Duplicate indexes (same table, same columns, excluding partial indexes)
        schema_filter_dup = "" if schema_name is None else "AND nspname = %s"
        params_dup = [] if schema_name is None else [schema_name]

        duplicates = await db().fetch_all(
            sql.SQL(f"""
            WITH idx_detail AS (
                SELECT
                    n.nspname,
                    t.relname AS tablename,
                    i.relname AS indexname,
                    array_agg(a.attname ORDER BY a.attnum) AS column_set,
                    ix.indpred IS NOT NULL AS is_partial
                FROM pg_index ix
                JOIN pg_class i ON ix.indexrelid = i.oid
                JOIN pg_class t ON ix.indrelid = t.oid
                JOIN pg_namespace n ON t.relnamespace = n.oid
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                    AND a.attnum > 0 AND NOT a.attisdropped
                WHERE NOT ix.indisprimary AND NOT ix.indisunique
                GROUP BY n.nspname, t.relname, i.relname, ix.indpred
            )
            SELECT
                nspname,
                tablename,
                array_agg(indexname ORDER BY indexname) AS duplicate_indexes,
                string_agg(DISTINCT col, ', ' ORDER BY col) AS columns
            FROM idx_detail, LATERAL UNNEST(column_set) AS col
            WHERE true {schema_filter_dup}
                AND NOT is_partial
            GROUP BY nspname, tablename, column_set
            HAVING count(*) > 1
            ORDER BY nspname, tablename
        """),
            params_dup,
        )

        # Low usage indexes
        low_usage = await db().fetch_all(
            sql.SQL(f"""
            SELECT
                schemaname,
                relname AS tablename,
                indexrelname AS indexname,
                idx_scan,
                pg_relation_size(indexrelid) AS size_bytes,
                pg_size_pretty(pg_relation_size(indexrelid)) AS size
            FROM pg_stat_user_indexes
            WHERE idx_scan < 10
            AND pg_relation_size(indexrelid) > 1000000
            {schema_filter}
            ORDER BY pg_relation_size(indexrelid) DESC
        """),
            params_unused,
        )

        # Missing index candidates
        missing = await db().fetch_all(
            sql.SQL(f"""
            SELECT
                schemaname,
                relname AS tablename,
                seq_scan,
                idx_scan,
                n_live_tup,
                ROUND(((seq_scan::float / NULLIF(idx_scan + seq_scan, 0)) * 100)::numeric, 2)
                    AS seq_scan_ratio_percent
            FROM pg_stat_user_tables
            WHERE seq_scan > 1000
            AND idx_scan < seq_scan / 10
            AND n_live_tup > 1000
            {schema_filter}
            ORDER BY seq_scan DESC
        """),
            params_unused,
        )

        total_unused_bytes = sum(r["size_bytes"] for r in unused)

        return as_json(
            {
                "unused_indexes": [
                    {
                        "schema": r["schemaname"],
                        "table": r["tablename"],
                        "index": r["indexname"],
                        "size_bytes": r["size_bytes"],
                        "size": r["size"],
                    }
                    for r in unused
                ],
                "duplicate_indexes": [
                    {
                        "schema": r["nspname"],
                        "table": r["tablename"],
                        "indexes": r["duplicate_indexes"],
                        "columns": r["columns"],
                    }
                    for r in duplicates
                ],
                "low_usage_indexes": [
                    {
                        "schema": r["schemaname"],
                        "table": r["tablename"],
                        "index": r["indexname"],
                        "scans": r["idx_scan"],
                        "size_bytes": r["size_bytes"],
                        "size": r["size"],
                    }
                    for r in low_usage
                ],
                "missing_index_candidates": [
                    {
                        "schema": r["schemaname"],
                        "table": r["tablename"],
                        "seq_scans": r["seq_scan"],
                        "index_scans": r["idx_scan"],
                        "live_rows": r["n_live_tup"],
                        "seq_ratio_percent": r["seq_scan_ratio_percent"],
                    }
                    for r in missing
                ],
                "reclaimable_bytes": total_unused_bytes,
            },
            settings,
        )

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def table_stats(
        schema_name: str | None = None,
        table_name: str | None = None,
        limit: int | None = None,
        format: str | None = None,
    ) -> str:
        """Show table-level statistics from pg_stat_user_tables.

        Returns sequential vs index scan counts, tuple stats (inserted/updated/
        deleted/hot-updated), live and dead tuple counts, dead-tuple percentage,
        vacuum history and timing, and autovacuum/autoanalyze activity.

        Args:
            schema_name: Restrict to this schema.
            table_name: Restrict to this table name.
            limit: Maximum rows to return.
            format: "json" (default), "markdown", or "csv".
        """
        where, params = schema_predicate("schemaname", settings)
        if schema_name:
            validate_schema(schema_name, settings)
            where = f"AND schemaname = %s {where}"
            params.insert(0, schema_name)
        if table_name:
            where = f"{where} AND relname = %s"
            params.append(table_name)

        result = await db().fetch(
            sql.SQL(f"""
            SELECT
                schemaname,
                relname AS table_name,
                seq_scan,
                seq_tup_read,
                idx_scan,
                idx_tup_fetch,
                n_tup_ins,
                n_tup_upd,
                n_tup_del,
                n_tup_hot_upd,
                n_live_tup,
                n_dead_tup,
                ROUND(((n_dead_tup::float / NULLIF(n_live_tup + n_dead_tup, 0)) * 100)::numeric, 2)
                    AS dead_ratio_percent,
                last_vacuum,
                last_autovacuum,
                last_analyze,
                last_autoanalyze,
                vacuum_count,
                autovacuum_count,
                analyze_count,
                autoanalyze_count
            FROM pg_stat_user_tables
            WHERE 1=1 {where}
            ORDER BY schemaname, relname
        """),
            params,
            limit=limit,
        )

        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def vacuum_status(
        schema_name: str | None = None,
        dead_tuple_threshold: int = 1000,
    ) -> str:
        """Show tables most in need of vacuum: dead tuples, age, and progress.

        Returns tables exceeding dead_tuple_threshold, sorted by dead-tuple count.
        Includes dead tuples, dead ratio, time since last vacuum/analyze, and
        transaction-ID age with wraparound warnings (age > autovacuum_freeze_max_age).
        Also reports any vacuum currently running from pg_stat_progress_vacuum (PG9.6+)
        with its phase, percentage complete and heap blocks scanned.

        Args:
            schema_name: Restrict to this schema.
            dead_tuple_threshold: Minimum dead tuples to include.
        """
        if schema_name:
            validate_schema(schema_name, settings)
            where_clause = "WHERE n.nspname = %s AND t.n_dead_tup > %s"
            params = [schema_name, dead_tuple_threshold]
        else:
            where_clause = "WHERE t.n_dead_tup > %s"
            params = [dead_tuple_threshold]

        tables = await db().fetch_all(
            sql.SQL(f"""
            SELECT
                n.nspname AS schemaname,
                t.relname,
                t.n_dead_tup,
                ROUND(((t.n_dead_tup::float / NULLIF(t.n_live_tup + t.n_dead_tup, 0)) * 100)::numeric, 2)
                    AS dead_ratio_percent,
                now() - COALESCE(t.last_vacuum, '1900-01-01') AS time_since_vacuum,
                now() - COALESCE(t.last_autovacuum, '1900-01-01') AS time_since_autovacuum,
                now() - COALESCE(t.last_analyze, '1900-01-01') AS time_since_analyze,
                now() - COALESCE(t.last_autoanalyze, '1900-01-01') AS time_since_autoanalyze,
                age(c.relfrozenxid) AS xid_age,
                current_setting('autovacuum_freeze_max_age')::int AS freeze_max_age
            FROM pg_stat_user_tables t
            JOIN pg_class c ON c.oid = t.relid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            {where_clause}
            ORDER BY t.n_dead_tup DESC
        """),
            params,
        )

        # Check for in-progress vacuum
        vacuums = await db().fetch_all(
            sql.SQL("""
            SELECT
                pv.pid,
                pv.datname,
                n.nspname AS schemaname,
                c.relname,
                pv.phase,
                pv.heap_blks_scanned,
                pv.heap_blks_total,
                ROUND(((pv.heap_blks_scanned::float / NULLIF(pv.heap_blks_total, 0)) * 100)::numeric, 2)
                    AS progress_percent
            FROM pg_stat_progress_vacuum pv
            JOIN pg_class c ON c.oid = pv.relid
            JOIN pg_namespace n ON c.relnamespace = n.oid
        """)
        )

        return as_json(
            {
                "tables_needing_vacuum": [
                    {
                        "schema": r["schemaname"],
                        "table": r["relname"],
                        "dead_tuples": r["n_dead_tup"],
                        "dead_ratio_percent": r["dead_ratio_percent"],
                        "time_since_vacuum": str(r["time_since_vacuum"]),
                        "time_since_autovacuum": str(r["time_since_autovacuum"]),
                        "time_since_analyze": str(r["time_since_analyze"]),
                        "time_since_autoanalyze": str(r["time_since_autoanalyze"]),
                        "xid_age": str(r["xid_age"]),
                        "xid_age_critical": r["xid_age"] > r["freeze_max_age"],
                    }
                    for r in tables
                ],
                "in_progress_vacuum": [
                    {
                        "pid": r["pid"],
                        "database": r["datname"],
                        "schema": r["schemaname"],
                        "table": r["relname"],
                        "phase": r["phase"],
                        "blocks_scanned": r["heap_blks_scanned"],
                        "blocks_total": r["heap_blks_total"],
                        "progress_percent": r["progress_percent"],
                    }
                    for r in vacuums
                ],
            },
            settings,
        )

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def slow_queries(
        limit: int = 20,
        order_by: str = "total_time",
        min_calls: int = 1,
        format: str | None = None,
    ) -> str:
        """Show slowest queries from pg_stat_statements extension.

        Returns query text, call count, total/mean/max/stddev times, and rows.
        order_by can be "total_time" (default), "mean_time", "calls" or "rows".
        Requires pg_stat_statements extension enabled in shared_preload_libraries
        and via CREATE EXTENSION.

        Args:
            limit: Maximum queries to return.
            order_by: Sort by "total_time", "mean_time", "calls" or "rows".
            min_calls: Only queries with this many calls.
            format: "json" (default), "markdown", or "csv".
        """
        # Map order_by to actual column names, handling PG13+ vs older versions
        order_map = {
            "total_time": "total_exec_time",
            "mean_time": "mean_exec_time",
            "calls": "calls",
            "rows": "rows",
        }

        order_col = order_map.get(order_by.strip().lower(), "total_exec_time")

        # Attempt to fetch from pg_stat_statements; if missing, return clear message
        try:
            result = await db().fetch(
                sql.SQL(f"""
                SELECT
                    query,
                    calls,
                    total_exec_time AS total_time_ms,
                    mean_exec_time AS mean_time_ms,
                    max_exec_time AS max_time_ms,
                    stddev_exec_time AS stddev_time_ms,
                    rows
                FROM pg_stat_statements
                WHERE calls >= %s
                ORDER BY {order_col} DESC
                LIMIT %s
            """),
                [min_calls, limit],
            )
        except Exception:
            # Extension likely not installed; return helpful message
            return as_json(
                {
                    "error": "pg_stat_statements extension not available",
                    "hint": (
                        "Enable it by: (1) add 'pg_stat_statements' to "
                        "shared_preload_libraries in postgresql.conf, "
                        "(2) restart PostgreSQL, (3) run: CREATE EXTENSION pg_stat_statements"
                    ),
                },
                settings,
            )

        return show(result, format, settings)

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def replication_status() -> str:
        """Show replication configuration and status: primary, standby, slots and lag.

        On a primary: pg_stat_replication (connected replicas, their LSN positions,
        lag in bytes and interval, sync state) and replication slot status.
        On a standby: replay lag and receive lag.
        Returns a clear message when replication is not configured.
        """
        in_recovery = await db().fetch_one(sql.SQL("SELECT pg_is_in_recovery()"))
        is_standby = in_recovery.get("pg_is_in_recovery", False) if in_recovery else False

        if is_standby:
            # Standby replication info
            standby_info = await db().fetch_one(
                sql.SQL("""
                SELECT
                    pg_last_wal_receive_lsn() AS receive_lsn,
                    pg_last_wal_replay_lsn() AS replay_lsn,
                    now() - pg_last_xact_replay_timestamp() AS replay_lag
            """)
            )
            return as_json(
                {
                    "node_role": "standby",
                    "receive_lsn": standby_info.get("receive_lsn") if standby_info else None,
                    "replay_lsn": standby_info.get("replay_lsn") if standby_info else None,
                    "replay_lag": str(standby_info.get("replay_lag")) if standby_info else None,
                },
                settings,
            )
        else:
            # Primary: show connected replicas and slots
            replicas = await db().fetch_all(
                sql.SQL("""
                SELECT
                    usename,
                    application_name,
                    client_addr,
                    state,
                    sent_lsn,
                    write_lsn,
                    flush_lsn,
                    replay_lsn,
                    backend_start,
                    write_lag,
                    flush_lag,
                    replay_lag,
                    sync_state,
                    sync_priority
                FROM pg_stat_replication
                ORDER BY client_addr
            """)
            )

            slots = await db().fetch_all(
                sql.SQL("""
                SELECT
                    slot_name,
                    slot_type,
                    restart_lsn,
                    confirmed_flush_lsn,
                    safe_wal_size
                FROM pg_replication_slots
                ORDER BY slot_name
            """)
            )

            return as_json(
                {
                    "node_role": "primary",
                    "replicas": [
                        {
                            "usename": r["usename"],
                            "application_name": r["application_name"],
                            "client_addr": r["client_addr"],
                            "state": r["state"],
                            "sent_lsn": r["sent_lsn"],
                            "write_lsn": r["write_lsn"],
                            "flush_lsn": r["flush_lsn"],
                            "replay_lsn": r["replay_lsn"],
                            "backend_start": str(r["backend_start"]),
                            "sync_state": r["sync_state"],
                            "sync_priority": r["sync_priority"],
                            "write_lag": str(r["write_lag"]) if r.get("write_lag") else None,
                            "flush_lag": str(r["flush_lag"]) if r.get("flush_lag") else None,
                            "replay_lag": str(r["replay_lag"]) if r.get("replay_lag") else None,
                        }
                        for r in replicas
                    ],
                    "replication_slots": [
                        {
                            "slot_name": r["slot_name"],
                            "slot_type": r["slot_type"],
                            "restart_lsn": r["restart_lsn"],
                            "confirmed_flush_lsn": r["confirmed_flush_lsn"],
                            "safe_wal_size": r["safe_wal_size"],
                        }
                        for r in slots
                    ],
                }
                if replicas or slots
                else {
                    "node_role": "primary",
                    "replicas": [],
                    "replication_slots": [],
                    "message": "No replication configured on this primary",
                },
                settings,
            )

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def health_check() -> str:
        """Run the important health checks concurrently and return a prioritised report.

        Returns a list of findings (critical, warning, info severity), each with
        check name and actionable message, plus an overall verdict and summary
        counting findings per severity. Critical findings appear first.

        Checks: cache hit ratio, connection saturation, idle-in-transaction
        sessions, long-running queries, blocked sessions, table bloat,
        transaction-ID wraparound risk, unused-index reclaimable space, tables
        never analyzed, invalid indexes, and deadlocks since stats reset.
        """

        async def check_cache_hit_ratio() -> dict | None:
            """Check cache hit ratio across all databases."""
            try:
                rows = await db().fetch_all(
                    sql.SQL("""
                    SELECT
                        datname,
                        blks_hit,
                        blks_read,
                        ROUND(((blks_hit::float / NULLIF(blks_hit + blks_read, 0)) * 100)::numeric, 2)
                            AS cache_hit_ratio
                    FROM pg_stat_database
                    WHERE datname NOT IN ('template0', 'template1')
                """)
                )
                low_hit_dbs = [r for r in rows if r.get("cache_hit_ratio", 100) < 95]
                if low_hit_dbs:
                    return {
                        "severity": "warning",
                        "check": "cache_hit_ratio",
                        "message": f"{len(low_hit_dbs)} database(s) with cache hit ratio < 95%",
                        "detail": "; ".join(
                            f"{r['datname']}: {r['cache_hit_ratio']}%" for r in low_hit_dbs[:5]
                        ),
                    }
            except Exception as e:
                return {
                    "severity": "info",
                    "check": "cache_hit_ratio",
                    "message": f"Could not check cache hit ratio: {str(e)[:100]}",
                    "detail": None,
                }
            return None

        async def check_connection_saturation() -> dict | None:
            """Check if approaching max_connections."""
            try:
                row = await db().fetch_one(
                    sql.SQL("""
                    SELECT
                        current_setting('max_connections')::int AS max_conn,
                        (SELECT count(*) FROM pg_stat_activity) AS current_conn
                """)
                )
                if row:
                    pct = (row["current_conn"] / row["max_conn"]) * 100
                    if pct > 80:
                        current = row["current_conn"]
                        maximum = row["max_conn"]
                        return {
                            "severity": "critical" if pct > 90 else "warning",
                            "check": "connection_saturation",
                            "message": (f"Using {pct:.1f}% of max_connections ({current}/{maximum})"),
                            "detail": None,
                        }
            except Exception as e:
                return {
                    "severity": "info",
                    "check": "connection_saturation",
                    "message": f"Could not check connections: {str(e)[:100]}",
                    "detail": None,
                }
            return None

        async def check_idle_in_transaction() -> dict | None:
            """Check for idle-in-transaction sessions."""
            try:
                idle_count = await db().fetch_one(
                    sql.SQL("""
                    SELECT count(*) AS idle_in_xact
                    FROM pg_stat_activity
                    WHERE state = 'idle in transaction'
                    AND pid <> pg_backend_pid()
                """)
                )
                if idle_count and idle_count.get("idle_in_xact", 0) > 0:
                    idle_count_val = idle_count["idle_in_xact"]
                    return {
                        "severity": "warning",
                        "check": "idle_in_transaction",
                        "message": (f"{idle_count_val} session(s) idle in transaction (holding locks)"),
                        "detail": None,
                    }
            except Exception as e:
                return {
                    "severity": "info",
                    "check": "idle_in_transaction",
                    "message": f"Could not check idle transactions: {str(e)[:100]}",
                    "detail": None,
                }
            return None

        async def check_long_running() -> dict | None:
            """Check for long-running queries."""
            try:
                long_count = await db().fetch_one(
                    sql.SQL("""
                    SELECT count(*) AS long_running
                    FROM pg_stat_activity
                    WHERE query_start < now() - interval '1 hour'
                    AND pid <> pg_backend_pid()
                """)
                )
                if long_count and long_count.get("long_running", 0) > 0:
                    return {
                        "severity": "warning",
                        "check": "long_running_queries",
                        "message": f"{long_count['long_running']} query(ies) running > 1 hour",
                        "detail": None,
                    }
            except Exception as e:
                return {
                    "severity": "info",
                    "check": "long_running_queries",
                    "message": f"Could not check long-running queries: {str(e)[:100]}",
                    "detail": None,
                }
            return None

        async def check_blocking() -> dict | None:
            """Check for blocked sessions."""
            try:
                blocked_count = await db().fetch_one(
                    sql.SQL("""
                    SELECT count(*) AS blocked
                    FROM pg_locks l
                    JOIN pg_stat_activity a ON l.pid = a.pid
                    WHERE NOT l.granted
                """)
                )
                if blocked_count and blocked_count.get("blocked", 0) > 0:
                    return {
                        "severity": "critical",
                        "check": "blocking_locks",
                        "message": f"{blocked_count['blocked']} session(s) blocked waiting for locks",
                        "detail": "Run blocking_locks() for details; kill blocker with pg_terminate_backend",
                    }
            except Exception as e:
                return {
                    "severity": "info",
                    "check": "blocking_locks",
                    "message": f"Could not check locks: {str(e)[:100]}",
                    "detail": None,
                }
            return None

        async def check_bloat() -> dict | None:
            """Check for high dead-tuple ratios."""
            try:
                bloated = await db().fetch_one(
                    sql.SQL("""
                    SELECT count(*) AS bloated_tables
                    FROM pg_stat_user_tables
                    WHERE n_dead_tup > 10000
                    AND (n_dead_tup::float / NULLIF(n_live_tup + n_dead_tup, 0)) > 0.2
                """)
                )
                if bloated and bloated.get("bloated_tables", 0) > 0:
                    return {
                        "severity": "warning",
                        "check": "table_bloat",
                        "message": f"{bloated['bloated_tables']} table(s) with > 20% dead tuples",
                        "detail": "Run vacuum_status() for details",
                    }
            except Exception as e:
                return {
                    "severity": "info",
                    "check": "table_bloat",
                    "message": f"Could not check bloat: {str(e)[:100]}",
                    "detail": None,
                }
            return None

        async def check_xid_wraparound() -> dict | None:
            """Check transaction-ID age."""
            try:
                xid_check = await db().fetch_one(
                    sql.SQL("""
                    SELECT
                        max(age(relfrozenxid)) AS max_xid_age,
                        current_setting('autovacuum_freeze_max_age')::int AS freeze_max_age
                    FROM pg_class
                """)
                )
                if xid_check:
                    ratio = (xid_check["max_xid_age"] / xid_check["freeze_max_age"]) * 100
                    if ratio > 75:
                        xid_age = xid_check["max_xid_age"]
                        return {
                            "severity": "critical" if ratio > 90 else "warning",
                            "check": "xid_wraparound_risk",
                            "message": (f"Transaction-ID age {xid_age} ({ratio:.1f}% of limit)"),
                            "detail": (
                                "Vacuum is behind; database will fail to accept writes near 2M XID age"
                            ),
                        }
            except Exception as e:
                return {
                    "severity": "info",
                    "check": "xid_wraparound_risk",
                    "message": f"Could not check XID age: {str(e)[:100]}",
                    "detail": None,
                }
            return None

        async def check_unused_indexes() -> dict | None:
            """Check for reclaimable unused index space."""
            try:
                unused = await db().fetch_one(
                    sql.SQL("""
                    SELECT
                        count(*) AS unused_count,
                        sum(pg_relation_size(indexrelid)) AS unused_bytes
                    FROM pg_stat_user_indexes
                    WHERE idx_scan = 0
                    AND NOT EXISTS (
                        SELECT 1 FROM pg_constraint c
                        WHERE c.conindid = pg_stat_user_indexes.indexrelid
                    )
                """)
                )
                if unused and unused.get("unused_bytes", 0) > 10_000_000:
                    count = unused["unused_count"]
                    mb = unused["unused_bytes"] / 1024 / 1024
                    return {
                        "severity": "info",
                        "check": "unused_indexes",
                        "message": (f"{count} unused index(es) consuming {mb:.1f} MB"),
                        "detail": "Run index_usage() to see which ones",
                    }
            except Exception as e:
                return {
                    "severity": "info",
                    "check": "unused_indexes",
                    "message": f"Could not check indexes: {str(e)[:100]}",
                    "detail": None,
                }
            return None

        async def check_never_analyzed() -> dict | None:
            """Check for tables never analyzed."""
            try:
                unanalyzed = await db().fetch_one(
                    sql.SQL("""
                    SELECT count(*) AS unanalyzed
                    FROM pg_stat_user_tables
                    WHERE last_analyze IS NULL
                    AND n_live_tup > 1000
                """)
                )
                if unanalyzed and unanalyzed.get("unanalyzed", 0) > 0:
                    return {
                        "severity": "info",
                        "check": "never_analyzed",
                        "message": f"{unanalyzed['unanalyzed']} table(s) > 1000 rows never analyzed",
                        "detail": "Query plans will be suboptimal; run ANALYZE",
                    }
            except Exception as e:
                return {
                    "severity": "info",
                    "check": "never_analyzed",
                    "message": f"Could not check analyzed tables: {str(e)[:100]}",
                    "detail": None,
                }
            return None

        async def check_invalid_indexes() -> dict | None:
            """Check for invalid indexes."""
            try:
                invalid = await db().fetch_one(
                    sql.SQL("""
                    SELECT count(*) AS invalid
                    FROM pg_index
                    WHERE NOT indisvalid
                """)
                )
                if invalid and invalid.get("invalid", 0) > 0:
                    return {
                        "severity": "warning",
                        "check": "invalid_indexes",
                        "message": f"{invalid['invalid']} invalid index(es)",
                        "detail": "These were created CONCURRENTLY and failed; drop and recreate",
                    }
            except Exception as e:
                return {
                    "severity": "info",
                    "check": "invalid_indexes",
                    "message": f"Could not check index validity: {str(e)[:100]}",
                    "detail": None,
                }
            return None

        async def check_deadlocks() -> dict | None:
            """Check for deadlocks since stats reset."""
            try:
                deadlocks = await db().fetch_one(
                    sql.SQL("""
                    SELECT sum(deadlocks) AS total_deadlocks
                    FROM pg_stat_database
                """)
                )
                if deadlocks and deadlocks.get("total_deadlocks", 0) > 0:
                    return {
                        "severity": "warning",
                        "check": "deadlocks",
                        "message": f"{deadlocks['total_deadlocks']} deadlock(s) since stats reset",
                        "detail": "Review your transaction isolation levels and locking patterns",
                    }
            except Exception as e:
                return {
                    "severity": "info",
                    "check": "deadlocks",
                    "message": f"Could not check deadlocks: {str(e)[:100]}",
                    "detail": None,
                }
            return None

        # Run all checks concurrently
        results = await asyncio.gather(
            check_cache_hit_ratio(),
            check_connection_saturation(),
            check_idle_in_transaction(),
            check_long_running(),
            check_blocking(),
            check_bloat(),
            check_xid_wraparound(),
            check_unused_indexes(),
            check_never_analyzed(),
            check_invalid_indexes(),
            check_deadlocks(),
            return_exceptions=True,
        )

        # Flatten results, filter out None and exceptions that are info level
        findings: list[dict[str, Any]] = []
        for result in results:
            if result is None:
                continue
            if isinstance(result, BaseException):
                findings.append(
                    {
                        "severity": "info",
                        "check": "unknown",
                        "message": f"Check error: {str(result)[:100]}",
                        "detail": None,
                    }
                )
            else:
                findings.append(result)

        # Sort by severity (critical > warning > info)
        severity_order = {"critical": 0, "warning": 1, "info": 2}
        findings.sort(key=lambda f: severity_order.get(str(f.get("severity", "")), 3))

        # Count by severity
        summary: dict[str, int] = {}
        for severity in ["critical", "warning", "info"]:
            count = sum(1 for f in findings if f.get("severity") == severity)
            if count > 0:
                summary[severity] = count

        # Overall verdict
        if any(f.get("severity") == "critical" for f in findings):
            overall = "critical"
        elif any(f.get("severity") == "warning" for f in findings):
            overall = "warning"
        else:
            overall = "healthy"

        return as_json(
            {
                "findings": findings,
                "overall": overall,
                "summary": summary,
            },
            settings,
        )
