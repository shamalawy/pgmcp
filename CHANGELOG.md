# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-08-29

### Added

- Initial release of pgmcp: a production-quality Model Context Protocol server for PostgreSQL
- Full database introspection tools (schemas, tables, columns, indexes, constraints, etc.)
- Safe query execution with read-only mode by default (configurable)
- SQL safety guard: protection against common injection patterns
- Multiple output formats: JSON (default), Markdown, CSV
- Docker and docker-compose support for easy deployment
- Comprehensive test suite with fixture database and multiple PostgreSQL versions
- CI/CD with GitHub Actions (lint, type check, test)
- Automated releases to PyPI and GitHub Container Registry
- Support for PostgreSQL 13+
- Version guards for PostgreSQL features (pg_stat_statements, query_id, etc.)
- Access control: allowlist for schemas and databases
- Byte-budget aware formatting to respect MCP token limits
- Full type hints and mypy strict mode compliance
- Detailed docstrings for all MCP tools (model-facing documentation)

[0.1.0]: https://github.com/shamalawy/pgmcp/releases/tag/v0.1.0
