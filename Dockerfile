# Build stage
FROM python:3.14-slim AS builder

WORKDIR /build

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

# Copy dependency files and README for layer caching
COPY pyproject.toml uv.lock README.md ./

# Copy source code
COPY src ./src

# Create venv and install dependencies
RUN uv venv /opt/venv --python /usr/local/bin/python3
RUN UV_PROJECT_ENVIRONMENT=/opt/venv uv sync --frozen --no-dev

# Runtime stage
FROM python:3.14-slim

WORKDIR /app

# Copy venv from builder
COPY --from=builder /opt/venv /opt/venv

# Copy source code for editable install (must match .pth file path)
COPY --from=builder /build/src /build/src

# Create non-root user
RUN useradd -m -u 1000 pgmcp

# Set environment variables
# Transport settings live in the environment rather than in CMD: CLI flags take
# precedence over environment variables, so hardcoding them in CMD would make
# `docker run -e PGMCP_TRANSPORT=...` silently do nothing.
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PGMCP_TRANSPORT=streamable-http \
    PGMCP_HOST=0.0.0.0 \
    PGMCP_PORT=8000 \
    PGMCP_MOUNT_PATH=/mcp

# Switch to non-root user
USER pgmcp

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgmcp --check || exit 1

# Expose port
EXPOSE 8000

# stdio would be pointless in a container with no attached client, so the image
# defaults to the HTTP transport configured above. Override any of it with -e.
CMD ["pgmcp"]
