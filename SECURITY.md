# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in pgmcp, please report it responsibly using GitHub Security Advisories instead of opening a public issue.

**To report a vulnerability:**
1. Go to https://github.com/shamalawy/pgmcp/security/advisories
2. Click "Report a vulnerability"
3. Provide details of the vulnerability and steps to reproduce

We take security seriously and will investigate all reports. You should expect:
- An acknowledgment within 48 hours
- Regular updates as we investigate and prepare a fix
- Credit for the discovery if you wish (unless you prefer anonymity)

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | Yes       |

We provide security updates for the current major.minor release. If you're running an older version, please upgrade.

## Security Model

pgmcp is designed with defence-in-depth for security, but it is **NOT a security boundary by itself**. This section explains the threat model and limitations.

### Read-Only Mode (Default)

pgmcp defaults to `PGMCP_ACCESS_MODE=read`, which enforces read-only transactions at the PostgreSQL level:

```sql
BEGIN TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY;
```

**What this protects against:**
- Accidental or malicious attempts to modify data through MCP tools
- Large writes that could disrupt the database

**What this does NOT protect against:**
- The LLM model attempting to bypass the restriction
- Side-channel attacks (e.g., inference based on query performance)
- Denial of service via expensive queries

### Write Mode

If you enable `PGMCP_ACCESS_MODE=write`, pgmcp allows CREATE/ALTER/DROP operations. In this mode:

**Do not grant pgmcp a role with unrestricted permissions.** Instead:

1. Create a dedicated least-privileged role:

```sql
-- Connect as superuser or the role owner
CREATE ROLE pgmcp_writer WITH LOGIN;
GRANT USAGE ON SCHEMA public TO pgmcp_writer;
GRANT CREATE ON SCHEMA public TO pgmcp_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO pgmcp_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pgmcp_writer;

-- Restrict to specific tables if possible
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM pgmcp_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.orders TO pgmcp_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.customers TO pgmcp_writer;
```

2. Configure pgmcp to use this role:

```bash
export PGMCP_DSN="postgresql://pgmcp_writer:password@host/database"
export PGMCP_ACCESS_MODE=write
export PGMCP_ALLOWED_SCHEMAS=public
```

3. Use `PGMCP_ALLOWED_SCHEMAS` to further restrict which schemas the LLM can access:

```bash
export PGMCP_ALLOWED_SCHEMAS=public  # LLM can only see/modify this schema
```

### Best Practices

1. **Always use a least-privileged database role** — create a dedicated role with the minimum permissions needed, regardless of access mode.

2. **Use `PGMCP_ALLOWED_SCHEMAS`** to restrict which schemas pgmcp exposes. This minimizes the blast radius in case the LLM is compromised or behaves unexpectedly.

3. **In write mode:**
   - Restrict to specific tables using role-level permissions
   - Use schema-level separation to isolate sensitive data
   - Monitor `pg_stat_statements` for unexpected queries
   - Consider database-level activity logging

4. **Connection security:**
   - Use TLS for connections to remote databases: `sslmode=require` in the DSN
   - Keep `PGMCP_DSN` in environment variables or a secure secret manager, never in version control
   - Rotate credentials regularly

5. **Access control:**
   - Deploy pgmcp in a network-restricted environment (e.g., within a private VPC)
   - Use authentication on the MCP client side (Claude Desktop, Continue, etc.)
   - Audit MCP tool calls and query results

6. **Monitoring:**
   - Enable PostgreSQL query logging: `log_statement = 'all'` (or at least `mod` for modifications)
   - Monitor `pg_stat_statements` for anomalous patterns
   - Set up alerts for unexpected schema/role changes

### SQL Safety Guard

pgmcp includes a SQL safety guard that detects and rejects common injection patterns:

- Attempts to escape string literals
- Explicit transaction control (BEGIN, COMMIT, ROLLBACK)
- Multi-statement queries
- Comments that appear to bypass restrictions

This guard is **one layer of protection**, not a complete solution. The primary protection is PostgreSQL's own parameterized query API (`psycopg`'s `%s` placeholders), which pgmcp uses exclusively.

### What pgmcp Cannot Protect Against

- **LLM prompt injection:** If an LLM is given malicious data in query results, it may behave unexpectedly. Use caution with user-supplied data.
- **Inference attacks:** An attacker could potentially infer data by observing query performance or side effects.
- **Privilege escalation:** pgmcp cannot elevate privileges beyond what the database role has. If the role has dangerous permissions, pgmcp will too.
- **Network access:** pgmcp does not filter which servers or ports the database server can access (e.g., `COPY FROM PROGRAM`).

## Dependencies

pgmcp depends on:
- `mcp`: Model Context Protocol library (Anthropic)
- `psycopg`: PostgreSQL driver with built-in parameterization
- `pydantic`: Data validation and settings management
- `sqlparse`: SQL parsing for safety checks

All dependencies are reputable, widely-used libraries. Dependencies are pinned to specific versions in `uv.lock`.

## Security Updates

If a security vulnerability is fixed:
1. A patch release is published immediately
2. The vulnerability is disclosed in a GitHub Security Advisory
3. If serious, an announcement is made in the release notes

## Questions?

If you have security concerns or questions, please reach out via a private security advisory rather than opening a public issue.
