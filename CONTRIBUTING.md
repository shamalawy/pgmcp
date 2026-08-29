# Contributing to pgmcp

Thanks for your interest in contributing! This document provides guidelines for development, testing, and the release process.

## Development Setup

### Prerequisites

- Python 3.10+
- PostgreSQL 13+ (for local testing)
- `uv` for dependency management (install via `curl -LsSf https://astral-sh.github.io/uv/install.sh | sh`)

### Setting Up Your Environment

```bash
# Clone the repository
git clone https://github.com/shamalawy/pgmcp.git
cd pgmcp

# Create a virtual environment and install dependencies
uv sync

# Verify installation
uv run pgmcp --version
```

## Running the Test Suite

### Using Docker Compose (Recommended)

The easiest way to run tests is with docker-compose, which spins up a PostgreSQL service automatically:

```bash
# Start PostgreSQL and seed the fixture database
docker compose up -d pgmcp_postgres

# Run tests against the containerized database
PGMCP_TEST_DSN=postgresql://postgres:postgres@localhost:5432/pgmcp_demo uv run pytest

# Or with coverage
PGMCP_TEST_DSN=postgresql://postgres:postgres@localhost:5432/pgmcp_demo uv run pytest --cov=src/pgmcp

# Clean up
docker compose down -v
```

### Using a Local PostgreSQL Instance

If you have PostgreSQL 17 running locally:

```bash
# Create test databases
createdb pgmcp_demo
createdb pgmcp_write

# Seed fixture data
psql -d pgmcp_demo < tests/fixtures/seed.sql

# Run tests
PGMCP_TEST_DSN=postgresql://postgres:postgres@localhost/pgmcp_demo uv run pytest
```

## Code Quality

### Linting

```bash
# Run ruff linter
uv run ruff check .

# Auto-fix formatting issues
uv run ruff format .
```

### Type Checking

```bash
# Run mypy type checker
uv run mypy
```

### All Checks

```bash
# Run all checks (lint, format, mypy, tests)
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest
```

## Adding a New Tool

All MCP tools are registered in modules under `src/pgmcp/tools/`. Each module must expose a `register(mcp, settings)` function.

### Template

```python
from __future__ import annotations

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

from ..config import Settings
from ._helpers import as_json, db, ident, regclass, schema_predicate, show, tool, validate_schema

# Declare this tool's annotations (read_only_hint, destructive_hint, idempotent_hint)
READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True)


def register(mcp: MCPServer, settings: Settings) -> None:
    """Register tools from this module."""

    @mcp.tool(annotations=READ_ONLY)
    @tool
    async def my_tool(schema_name: str = "public", format: str | None = None) -> str:
        """One-line summary the LLM reads to decide whether to use this tool.

        Longer paragraph: what it returns, when to use it, what to call next.

        Args:
            schema_name: Schema to inspect.
            format: "json" (default), "markdown", or "csv".
        """
        # Always validate schema names
        validate_schema(schema_name, settings)

        # Build WHERE clause using schema_predicate for safety
        where, params = schema_predicate("n.nspname", settings)

        # Execute query with parameterized inputs
        result = await db().fetch(f"SELECT ... WHERE true {where}", params, limit=settings.max_rows)

        # Render output via show() to respect settings and byte budgets
        return show(result, format, settings)
```

### Key Rules

**Non-negotiables:**
- `@mcp.tool(annotations=...)` goes ON TOP of `@tool` (the order matters)
- All tools are `async` and return `str`
- Tool parameters must be JSON-schema-friendly: `str`, `int`, `bool`, `float`, `list[str]`, or `| None`
- Never accept `Context` or dict-typed parameters
- The docstring IS the LLM-facing documentation — write for an AI choosing whether to call this tool

**SQL Safety (CRITICAL):**
- Bind ALL client-supplied values as `%s` parameters
- For identifiers (schema/table/column names), use `ident(...)` or the `regclass` helper
- NEVER f-string a user value into SQL
- Use `schema_predicate(...)` for safe schema filtering
- Example:

```python
# Safe: bind user input as parameter
result = await db().fetch("SELECT * FROM %s.orders WHERE id = %s", [regclass(schema, table), order_id])

# Safe: use ident() for identifiers in structured queries
from psycopg.sql import SQL, Identifier

query = SQL("SELECT * FROM {table}").format(table=Identifier(schema, table))
result = await db().fetch(query)

# UNSAFE: never f-string user input
query = f"SELECT * FROM {schema}.{table} WHERE id = {order_id}"  # DON'T DO THIS
```

**Respect Settings:**
- Validate schema arguments with `validate_schema(schema_name, settings)`
- Pass `limit=settings.max_rows` to `db().fetch()`
- Render output via `show(result, format, settings)` for formatting and byte budgets
- Honor `settings.access_mode` (read vs. write)
- Honor `settings.allowed_schemas` via `schema_predicate`

**PostgreSQL Compatibility:**
- Target PostgreSQL 13+ (latest is 17)
- Guard version-dependent features:
  - `pg_stat_statements.total_exec_time` (PG13+; was `total_time` in PG12)
  - `pg_stat_activity.query_id` (PG14+)
  - `pg_stat_statements` may not be installed at all
- Degrade gracefully with a clear message instead of raising

**Style:**
- `from __future__ import annotations`
- Full type hints throughout
- Line length <= 110
- Double quotes for strings
- Comments explain WHY, not what
- No emoji

## Commit Conventions

Follow conventional commits:

- `feat: add new tool for listing widgets` — new feature
- `fix: handle null values in formatting` — bug fix
- `refactor: simplify schema predicate logic` — non-functional change
- `docs: update CONTRIBUTING guide` — documentation
- `test: add coverage for edge cases` — test additions
- `chore: update dependencies` — maintenance

Example commit message:

```
feat: add list_constraints tool for schema introspection

Add a new MCP tool to list and describe all constraints (primary key,
foreign key, unique, check) in a schema. Supports filtering by table
and output format selection.
```

## Testing Guidelines

- Write tests in `tests/` using pytest
- Use the fixture database seeded by `tests/fixtures/seed.sql`
- Mark integration tests with `@pytest.mark.integration`
- Test edge cases: quoted identifiers, partitioned tables, inheritance, etc.
- Verify actual VALUES, not just that code doesn't crash
- Run against multiple PostgreSQL versions in CI

## Release Process

Releases are triggered by pushing a git tag with the format `v*.*.*` (e.g., `v0.2.0`):

```bash
# Update version in pyproject.toml
# Update CHANGELOG.md with release notes
git add pyproject.toml CHANGELOG.md
git commit -m "chore: prepare v0.2.0 release"

# Create tag
git tag -a v0.2.0 -m "Release v0.2.0"

# Push to trigger CI/CD
git push origin main
git push origin v0.2.0
```

The GitHub Actions workflows will:
1. Build and test on all Python versions and PostgreSQL versions
2. Lint and type-check
3. Build wheel and sdist with `uv build`
4. Verify with `twine check`
5. Publish to PyPI via Trusted Publishing (OIDC, no secrets required)
6. Build Docker image and push to GitHub Container Registry
7. Create a GitHub Release with artifacts

## Questions?

- Open an issue for bugs or features
- Join discussions for questions or ideas
- See SECURITY.md for security concerns

Thanks for contributing!
