"""Static analysis of SQL submitted through tools.

This module is *defence in depth*, not the primary control.  The primary
control is that every read path runs inside a ``READ ONLY`` transaction, which
PostgreSQL itself enforces (``ReadOnlySqlTransaction``, SQLSTATE 25006).  The
guard exists to fail fast with a clear message, to stop multi-statement
smuggling that psycopg's simple-query path would otherwise allow, and to block
a handful of functions that read or write the *server's filesystem* — those run
happily inside a read-only transaction because they do not touch MVCC state.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum

import sqlparse
from sqlparse import tokens as sql_tokens
from sqlparse.sql import Comment, Statement

from .errors import UnsafeQueryError

__all__ = [
    "StatementInfo",
    "StatementKind",
    "analyze",
    "assert_read_only",
    "assert_single_statement",
    "split_statements",
    "validate_fragment",
]


class StatementKind(str, Enum):
    """Coarse classification used to decide what a statement is allowed to do."""

    READ = "read"
    """SELECT / TABLE / VALUES / SHOW / EXPLAIN without ANALYZE."""

    DML = "dml"
    """INSERT / UPDATE / DELETE / MERGE, including data-modifying CTEs."""

    DDL = "ddl"
    """CREATE / ALTER / DROP / TRUNCATE / GRANT / COMMENT / REINDEX ..."""

    MAINTENANCE = "maintenance"
    """VACUUM / ANALYZE / CLUSTER / CHECKPOINT — no data change, but not read-only."""

    TRANSACTION = "transaction"
    """BEGIN / COMMIT / ROLLBACK / SAVEPOINT — rejected: pgmcp owns transactions."""

    SESSION = "session"
    """SET / RESET / DISCARD — rejected on shared pooled connections."""

    CALL = "call"
    """CALL / DO — arbitrary procedural code, treated as a write."""

    COPY = "copy"
    """COPY in either direction; only COPY (query) TO STDOUT is read-only."""

    EMPTY = "empty"
    """Nothing but whitespace and comments."""

    UNKNOWN = "unknown"
    """Unrecognised leading keyword; treated as unsafe."""


_READ_KEYWORDS = {"SELECT", "TABLE", "VALUES", "WITH", "SHOW", "EXPLAIN"}
_DML_KEYWORDS = {"INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "REPLACE"}
_DDL_KEYWORDS = {
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "GRANT",
    "REVOKE",
    "COMMENT",
    "REINDEX",
    "REFRESH",
    "SECURITY",
    "IMPORT",
    "LOCK",
    "LISTEN",
    "UNLISTEN",
    "NOTIFY",
    "PREPARE",
    "EXECUTE",
    "DEALLOCATE",
    "CLUSTER",
    "LOAD",
}
_MAINTENANCE_KEYWORDS = {"VACUUM", "ANALYZE", "ANALYSE", "CHECKPOINT"}
_TRANSACTION_KEYWORDS = {"BEGIN", "START", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE", "ABORT"}
_SESSION_KEYWORDS = {"SET", "RESET", "DISCARD"}
_CALL_KEYWORDS = {"CALL", "DO"}

# Statements that mutate, expressed as whole words, for the CTE scan below.
_MUTATING_WORD_RE = re.compile(
    r"(?is)\b(INSERT\s+INTO|UPDATE\s+\w|DELETE\s+FROM|MERGE\s+INTO|TRUNCATE\b|"
    r"CREATE\s+|ALTER\s+|DROP\s+|GRANT\s+|REVOKE\s+)"
)

# Functions that escape the read-only transaction guarantee: they touch the
# server's filesystem, spawn processes, open outbound connections, or terminate
# other sessions.  A read-only transaction does not stop any of these.
_DANGEROUS_FUNCTIONS: tuple[str, ...] = (
    "pg_read_file",
    "pg_read_binary_file",
    "pg_ls_dir",
    "pg_stat_file",
    "pg_file_write",
    "pg_file_rename",
    "pg_file_unlink",
    "pg_logdir_ls",
    "lo_import",
    "lo_export",
    "lo_put",
    "dblink",
    "dblink_connect",
    "dblink_exec",
    "pg_terminate_backend",
    "pg_cancel_backend",
    "pg_reload_conf",
    "pg_rotate_logfile",
    "pg_promote",
    "pg_create_restore_point",
    "pg_drop_replication_slot",
    "pg_create_physical_replication_slot",
    "pg_create_logical_replication_slot",
    "pg_replication_origin_create",
    "pg_replication_origin_drop",
    "pg_import_system_collations",
    "set_config",
    "pg_sleep",
    "pg_sleep_for",
    "pg_sleep_until",
    "pg_advisory_lock",
    "pg_advisory_xact_lock",
    "system",
)
_DANGEROUS_FUNCTION_RE = re.compile(
    r"(?i)(?<![\w.])(" + "|".join(re.escape(name) for name in _DANGEROUS_FUNCTIONS) + r")\s*\(",
)

# ``COPY ... FROM/TO PROGRAM`` executes a shell command as the server user.
_COPY_PROGRAM_RE = re.compile(r"(?is)\bCOPY\b.*?\b(FROM|TO)\s+PROGRAM\b")
# Only ``COPY (subquery) TO STDOUT`` is a read; every other COPY form reads or
# writes a server-side file or a table.
_COPY_READ_RE = re.compile(r"(?is)^\s*COPY\s*\(.*\)\s*TO\s+STDOUT")

_READ_ONLY_KINDS = frozenset({StatementKind.READ})


@dataclass(frozen=True)
class StatementInfo:
    """The outcome of analysing one SQL statement."""

    sql: str
    kind: StatementKind
    leading_keyword: str
    is_read_only: bool
    reason: str | None = None

    @property
    def mutates(self) -> bool:
        return self.kind in {StatementKind.DML, StatementKind.DDL, StatementKind.CALL}


def _strip_comments(statement: Statement) -> str:
    """Return the statement text with comments removed and whitespace collapsed."""
    parts: list[str] = []
    for token in statement.flatten():
        if token.ttype in sql_tokens.Comment or isinstance(token.parent, Comment):
            continue
        parts.append(token.value)
    return " ".join("".join(parts).split())


def split_statements(sql: str) -> list[str]:
    """Split ``sql`` into individual statements, dropping empty/comment-only ones.

    Semicolons inside string literals, dollar-quoted bodies and identifiers are
    handled by sqlparse's lexer, so this is safe against the naive
    ``sql.split(";")`` bypass.
    """
    return [text for text in (_strip_comments(s) for s in sqlparse.parse(sql or "")) if text]


def _leading_keyword(text: str) -> str:
    """The first DML/DDL keyword of a statement, in upper case.

    Skips over an opening parenthesis (``(SELECT ...) UNION ...``) and over the
    ``EXPLAIN``/``WITH`` prefixes handled separately by the caller.
    """
    for statement in sqlparse.parse(text):
        for token in statement.flatten():
            if token.ttype in sql_tokens.Comment or token.is_whitespace:
                continue
            if token.ttype is sql_tokens.Punctuation and token.value == "(":
                continue
            if token.ttype in sql_tokens.Keyword:
                return token.value.upper()
            if token.ttype in sql_tokens.Name:
                return token.value.upper()
            return token.value.upper()
    return ""


def _classify_keyword(keyword: str) -> StatementKind:
    if keyword in _DML_KEYWORDS:
        return StatementKind.DML
    if keyword in _MAINTENANCE_KEYWORDS:
        return StatementKind.MAINTENANCE
    if keyword in _DDL_KEYWORDS:
        return StatementKind.DDL
    if keyword in _TRANSACTION_KEYWORDS:
        return StatementKind.TRANSACTION
    if keyword in _SESSION_KEYWORDS:
        return StatementKind.SESSION
    if keyword in _CALL_KEYWORDS:
        return StatementKind.CALL
    if keyword == "COPY":
        return StatementKind.COPY
    if keyword in _READ_KEYWORDS:
        return StatementKind.READ
    return StatementKind.UNKNOWN


def analyze(sql: str) -> StatementInfo:
    """Classify a single SQL statement.

    Raises :class:`UnsafeQueryError` if ``sql`` contains more than one
    statement — callers that want to run several should use the explicit
    transaction tool, where each statement is analysed on its own.
    """
    statements = split_statements(sql)
    if not statements:
        return StatementInfo(
            sql="",
            kind=StatementKind.EMPTY,
            leading_keyword="",
            is_read_only=False,
            reason="the statement is empty",
        )
    if len(statements) > 1:
        raise UnsafeQueryError(
            f"expected a single SQL statement but found {len(statements)}. "
            "Multiple statements in one call are refused; use the transaction tool instead."
        )
    return _analyze_one(statements[0])


def _analyze_one(text: str) -> StatementInfo:
    keyword = _leading_keyword(text)
    kind = _classify_keyword(keyword)

    if _COPY_PROGRAM_RE.search(text):
        return StatementInfo(
            text,
            StatementKind.DDL,
            keyword,
            False,
            "COPY ... PROGRAM executes a shell command on the database server",
        )

    dangerous = _DANGEROUS_FUNCTION_RE.search(text)
    if dangerous:
        return StatementInfo(
            text,
            kind,
            keyword,
            False,
            f"{dangerous.group(1)}() can act outside the read-only transaction "
            "(server filesystem, other sessions, or outbound connections)",
        )

    if kind is StatementKind.COPY:
        if _COPY_READ_RE.match(text):
            return StatementInfo(text, StatementKind.READ, keyword, True)
        return StatementInfo(
            text,
            StatementKind.COPY,
            keyword,
            False,
            "only COPY (query) TO STDOUT is a read-only form of COPY",
        )

    if keyword == "EXPLAIN":
        # EXPLAIN ANALYZE *executes* the plan, so it inherits the safety of the
        # statement being explained rather than being read-only by itself.
        if re.search(r"(?i)\bANALYZE\b|\bANALYSE\b", _explain_options(text)):
            inner = _explain_body(text)
            if inner:
                nested = _analyze_one(inner)
                if not nested.is_read_only:
                    return StatementInfo(
                        text,
                        nested.kind,
                        keyword,
                        False,
                        nested.reason or "EXPLAIN ANALYZE runs the statement, which is not read-only",
                    )
                return StatementInfo(text, StatementKind.READ, keyword, True)
        return StatementInfo(text, StatementKind.READ, keyword, True)

    if keyword == "WITH":
        # A data-modifying CTE (`WITH x AS (DELETE ...) SELECT ...`) is a write
        # even though the statement reads as a SELECT.
        if _MUTATING_WORD_RE.search(text):
            return StatementInfo(
                text, StatementKind.DML, keyword, False, "the WITH clause contains a data-modifying statement"
            )
        return StatementInfo(text, StatementKind.READ, keyword, True)

    if kind is StatementKind.TRANSACTION:
        return StatementInfo(
            text,
            kind,
            keyword,
            False,
            "transaction control is managed by pgmcp and cannot be issued directly",
        )

    if kind is StatementKind.SESSION:
        return StatementInfo(
            text, kind, keyword, False, "session-level SET/RESET/DISCARD is refused on pooled connections"
        )

    if kind is StatementKind.UNKNOWN:
        return StatementInfo(
            text, kind, keyword, False, f"unrecognised statement type {keyword or '<empty>'!r}"
        )

    return StatementInfo(text, kind, keyword, kind in _READ_ONLY_KINDS)


def assert_single_statement(sql: str) -> str:
    """Return the single normalised statement in ``sql`` or raise."""
    return analyze(sql).sql


def assert_read_only(sql: str) -> StatementInfo:
    """Raise :class:`UnsafeQueryError` unless ``sql`` is a single read-only statement."""
    info = analyze(sql)
    if not info.is_read_only:
        detail = info.reason or f"{info.leading_keyword or 'this statement'} modifies the database"
        raise UnsafeQueryError(
            f"refused in read-only mode: {detail}. "
            "Start the server with --access-mode=write to enable mutating statements."
        )
    return info


def _explain_options(text: str) -> str:
    """The option list of an EXPLAIN, i.e. everything before the inner statement."""
    match = re.match(r"(?is)^\s*EXPLAIN\s*(\([^)]*\))?", text)
    if not match:
        return ""
    head = match.group(0)
    rest = text[match.end() :].lstrip()
    # Bare `EXPLAIN ANALYZE SELECT ...` puts the keyword outside parentheses.
    leading_words = re.match(r"(?is)^((?:ANALYZE|ANALYSE|VERBOSE|COSTS|BUFFERS|TIMING)\s+)*", rest)
    return head + (leading_words.group(0) if leading_words else "")


def _explain_body(text: str) -> str:
    """The statement an EXPLAIN wraps, with the EXPLAIN prefix removed."""
    options = _explain_options(text)
    return text[len(options) :].strip() if options else ""


# ---------------------------------------------------------------- fragments --
#
# A few tools accept a bare SQL *expression* rather than a whole statement --
# ``sample_table(where=...)`` and ``count_rows(where=...)``, for instance. Those
# get spliced into a statement pgmcp builds, so the read-only transaction still
# covers them and they cannot write. They can, however, restructure the query:
# ``true UNION SELECT ... FROM secret.t`` or ``(SELECT ... FROM secret.t) > 0``
# reads a schema the allow-list was meant to hide. A fragment is therefore held
# to a much narrower grammar than a statement: a self-contained expression, with
# no subqueries and no set operations.

_FORBIDDEN_FRAGMENT_KEYWORDS = frozenset(
    {
        "SELECT",
        "FROM",
        "WHERE",
        "JOIN",
        "UNION",
        "INTERSECT",
        "EXCEPT",
        "WITH",
        "INSERT",
        "UPDATE",
        "DELETE",
        "MERGE",
        "TRUNCATE",
        "CREATE",
        "ALTER",
        "DROP",
        "GRANT",
        "REVOKE",
        "INTO",
        "RETURNING",
        "LATERAL",
        "VALUES",
        "TABLE",
        "WINDOW",
        "GROUP",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "FETCH",
        "DECLARE",
        "CALL",
        "DO",
        "COPY",
        "SET",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "EXECUTE",
        "PREPARE",
    }
)


def validate_fragment(fragment: str, *, kind: str = "WHERE") -> str:
    """Return a normalised SQL expression fragment, or raise :class:`UnsafeQueryError`.

    ``kind`` names the clause for error messages (``"WHERE"``, ``"ORDER BY"``).

    Accepts a single self-contained expression: comparisons, boolean operators,
    literals, column references, function calls, ``ORDER BY`` direction keywords.
    Rejects anything that could change the *shape* of the surrounding statement --
    subqueries, set operations, additional clauses, statement terminators -- and
    the same host-reaching functions a full statement is checked against.
    """
    text = (fragment or "").strip()
    if not text:
        raise UnsafeQueryError(f"the {kind} fragment is empty")

    statements = sqlparse.parse(text)
    # A ';' inside a string literal is harmless, so test for a real punctuation
    # token rather than for the character.
    has_terminator = any(
        token.ttype is sql_tokens.Punctuation and token.value == ";"
        for statement in statements
        for token in statement.flatten()
    )
    if len(statements) > 1 or has_terminator:
        raise UnsafeQueryError(f"the {kind} fragment must be a single expression and cannot contain ';'")

    # Comments are stripped rather than rejected: a trailing ``--`` would otherwise
    # comment out the rest of the statement pgmcp builds around this fragment.
    cleaned = _strip_comments(statements[0])
    if not cleaned:
        raise UnsafeQueryError(f"the {kind} fragment contains nothing but comments")

    for token in sqlparse.parse(cleaned)[0].flatten():
        if token.ttype not in sql_tokens.Keyword:
            continue
        # sqlparse emits compound keywords as one token ("GROUP BY", "UNION ALL"),
        # so check each word rather than the token value as a whole.
        for word in token.value.upper().split():
            if word in _FORBIDDEN_FRAGMENT_KEYWORDS:
                raise UnsafeQueryError(
                    f"{word} is not allowed in a {kind} fragment: it would change the "
                    "structure of the query rather than filter it. Use the `query` tool "
                    "for anything that needs a subquery or a set operation."
                )

    dangerous = _DANGEROUS_FUNCTION_RE.search(cleaned)
    if dangerous:
        raise UnsafeQueryError(
            f"{dangerous.group(1)}() is not allowed in a {kind} fragment: it can act "
            "outside the read-only transaction."
        )

    if cleaned.count("(") != cleaned.count(")"):
        raise UnsafeQueryError(
            f"the {kind} fragment has unbalanced parentheses, which would corrupt the surrounding query"
        )

    return cleaned
