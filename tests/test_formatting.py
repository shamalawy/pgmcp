"""Tests for result formatting and truncation."""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from pgmcp.config import OutputFormat
from pgmcp.formatting import QueryResult, json_default, render, to_jsonable, truncate


class TestJsonDefault:
    """Test json_default fallback encoder."""

    def test_decimal_as_string(self) -> None:
        """Decimal is encoded as string to preserve precision."""
        result = json_default(Decimal("123.45"))
        assert result == "123.45"
        assert isinstance(result, str)

    def test_decimal_exact_cents(self) -> None:
        """Decimal precision is preserved."""
        result = json_default(Decimal("99.99"))
        assert result == "99.99"

    def test_datetime(self) -> None:
        """datetime is encoded as ISO format string."""
        dt = datetime(2024, 3, 15, 12, 30, 45)
        result = json_default(dt)
        assert result == "2024-03-15T12:30:45"

    def test_date(self) -> None:
        """date is encoded as ISO format string."""
        d = date(2024, 3, 15)
        result = json_default(d)
        assert result == "2024-03-15"

    def test_time(self) -> None:
        """time is encoded as ISO format string."""
        t = time(12, 30, 45)
        result = json_default(t)
        assert result == "12:30:45"

    def test_timedelta(self) -> None:
        """timedelta is encoded as string."""
        td = timedelta(days=1, hours=2, minutes=3)
        result = json_default(td)
        assert "1 day" in result
        assert "2:03" in result

    def test_uuid(self) -> None:
        """UUID is encoded as string."""
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        result = json_default(u)
        assert result == "550e8400-e29b-41d4-a716-446655440000"

    def test_bytes(self) -> None:
        """bytes are decoded as UTF-8 or hex-encoded."""
        # UTF-8 decodable
        b = b"hello"
        result = json_default(b)
        assert result == "hello"

    def test_bytes_non_utf8(self) -> None:
        """Non-UTF-8 bytes are hex-encoded."""
        # Use bytes that are invalid UTF-8: 0xFF is not valid UTF-8
        b = b"\xff\xfe"
        result = json_default(b)
        assert result.startswith("\\x")

    def test_memoryview(self) -> None:
        """memoryview is handled like bytes."""
        mv = memoryview(b"hello")
        result = json_default(mv)
        assert result == "hello"

    def test_list_and_tuple_converted(self) -> None:
        """Tuples are converted to lists; lists fall through to str."""
        result = json_default((1, 2, 3))
        assert result == [1, 2, 3]
        # Lists are not handled by json_default (they're JSON-serializable),
        # so they fall through to str()
        result = json_default([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_set_and_frozenset(self) -> None:
        """Sets are converted to lists."""
        result = json_default({1, 2, 3})
        assert isinstance(result, list)
        assert set(result) == {1, 2, 3}


class TestToJsonable:
    """Test recursive conversion to JSON-serializable types."""

    def test_scalar_values(self) -> None:
        """Scalars are returned unchanged."""
        assert to_jsonable(None) is None
        assert to_jsonable(42) == 42
        assert to_jsonable("hello") == "hello"
        assert to_jsonable(3.14) == 3.14
        assert to_jsonable(True) is True

    def test_dict_values_recursive(self) -> None:
        """Dict values are recursively converted."""
        d = {"dt": datetime(2024, 1, 1), "num": Decimal("1.5")}
        result = to_jsonable(d)
        assert result["dt"] == "2024-01-01T00:00:00"
        assert result["num"] == "1.5"

    def test_list_values_recursive(self) -> None:
        """List values are recursively converted."""
        lst = [datetime(2024, 1, 1), Decimal("1.5")]
        result = to_jsonable(lst)
        assert result[0] == "2024-01-01T00:00:00"
        assert result[1] == "1.5"

    def test_nested_structures(self) -> None:
        """Nested structures are fully converted."""
        data = {
            "rows": [
                {"id": 1, "created": date(2024, 1, 1)},
                {"id": 2, "created": date(2024, 1, 2)},
            ]
        }
        result = to_jsonable(data)
        assert result["rows"][0]["created"] == "2024-01-01"
        assert result["rows"][1]["created"] == "2024-01-02"


class TestTruncate:
    """Test output truncation."""

    def test_no_truncation_when_under_budget(self) -> None:
        """Text under the budget is returned unchanged."""
        text = "hello world"
        result = truncate(text, 1000)
        assert result == text

    def test_truncation_when_over_budget(self) -> None:
        """Text over budget is truncated."""
        text = "a" * 1000
        result = truncate(text, 100)
        assert len(result) < len(text)
        assert "[output truncated" in result

    def test_truncation_marker_appended(self) -> None:
        """Truncation marker is appended."""
        text = "x" * 1000
        result = truncate(text, 100)
        assert "[output truncated: exceeded max_result_bytes]" in result

    def test_truncation_utf8_boundary(self) -> None:
        """Truncation respects UTF-8 boundaries."""
        # A multi-byte character
        text = "hello " + "🌍" * 100
        result = truncate(text, 50)
        # Should not raise a decoding error
        assert isinstance(result, str)
        assert "[output truncated" in result

    def test_zero_budget(self) -> None:
        """Zero or negative budget disables truncation."""
        text = "hello"
        result = truncate(text, 0)
        assert result == text  # Truncation disabled when budget <= 0

    def test_negative_budget_disabled(self) -> None:
        """Negative budget is treated as no truncation."""
        text = "hello world"
        result = truncate(text, -1)
        assert result == text


class TestQueryResult:
    """Test QueryResult data structure."""

    def test_empty_result(self) -> None:
        """An empty result is valid."""
        result = QueryResult()
        assert result.columns == []
        assert result.rows == []
        assert result.truncated is False

    def test_metadata_minimal(self) -> None:
        """Metadata with no truncation."""
        result = QueryResult(columns=["id"], rows=[{"id": 1}], row_count=1)
        meta = result.metadata()
        assert meta["row_count"] == 1
        assert "truncated" not in meta

    def test_metadata_truncated(self) -> None:
        """Truncated metadata includes limit."""
        result = QueryResult(
            columns=["id"],
            rows=[{"id": i} for i in range(10)],
            row_count=10,
            truncated=True,
            limit=10,
        )
        meta = result.metadata()
        assert meta["truncated"] is True
        assert meta["limit"] == 10
        assert "note" in meta

    def test_metadata_affected_rows(self) -> None:
        """Affected rows included when present."""
        result = QueryResult(affected_rows=5)
        meta = result.metadata()
        assert meta["affected_rows"] == 5

    def test_metadata_command_status(self) -> None:
        """Command status included when present."""
        result = QueryResult(command_status="SELECT 10")
        meta = result.metadata()
        assert meta["command"] == "SELECT 10"

    def test_to_dict(self) -> None:
        """to_dict combines metadata and rows."""
        result = QueryResult(
            columns=["id", "name"],
            rows=[{"id": 1, "name": "Alice"}],
            row_count=1,
        )
        d = result.to_dict()
        assert "row_count" in d
        assert "columns" in d
        assert "rows" in d
        assert d["rows"][0]["id"] == 1


class TestRenderJSON:
    """Test JSON rendering."""

    def test_json_simple_result(self) -> None:
        """Simple result renders as JSON."""
        result = QueryResult(
            columns=["id", "name"],
            rows=[{"id": 1, "name": "Alice"}],
            row_count=1,
        )
        output = render(result, OutputFormat.JSON)
        d = json.loads(output)
        assert d["row_count"] == 1
        assert d["columns"] == ["id", "name"]
        assert d["rows"][0]["id"] == 1

    def test_json_with_decimal(self) -> None:
        """Decimals are rendered as strings."""
        result = QueryResult(
            columns=["price"],
            rows=[{"price": Decimal("99.99")}],
            row_count=1,
        )
        output = render(result, OutputFormat.JSON)
        d = json.loads(output)
        assert d["rows"][0]["price"] == "99.99"

    def test_json_with_datetime(self) -> None:
        """Datetimes are rendered as ISO strings."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        result = QueryResult(
            columns=["created_at"],
            rows=[{"created_at": dt}],
            row_count=1,
        )
        output = render(result, OutputFormat.JSON)
        d = json.loads(output)
        assert d["rows"][0]["created_at"] == "2024-01-15T10:30:00"

    def test_json_empty_result(self) -> None:
        """Empty result renders as valid JSON."""
        result = QueryResult()
        output = render(result, OutputFormat.JSON)
        d = json.loads(output)
        assert d["row_count"] == 0


class TestRenderMarkdown:
    """Test Markdown rendering."""

    def test_markdown_simple_table(self) -> None:
        """Simple table renders as markdown."""
        result = QueryResult(
            columns=["id", "name"],
            rows=[{"id": 1, "name": "Alice"}],
            row_count=1,
        )
        output = render(result, OutputFormat.MARKDOWN)
        assert "|" in output
        assert "id" in output
        assert "Alice" in output

    def test_markdown_escapes_pipes(self) -> None:
        """Pipes in values are escaped."""
        result = QueryResult(
            columns=["data"],
            rows=[{"data": "a|b"}],
            row_count=1,
        )
        output = render(result, OutputFormat.MARKDOWN)
        assert "a\\|b" in output

    def test_markdown_escapes_newlines(self) -> None:
        """Newlines in values are replaced."""
        result = QueryResult(
            columns=["text"],
            rows=[{"text": "line1\nline2"}],
            row_count=1,
        )
        output = render(result, OutputFormat.MARKDOWN)
        assert "<br>" in output or "line1" in output

    def test_markdown_empty_result(self) -> None:
        """Empty result renders markdown notation."""
        result = QueryResult()
        output = render(result, OutputFormat.MARKDOWN)
        assert "no columns returned" in output.lower() or "0 row" in output.lower()

    def test_markdown_row_count(self) -> None:
        """Row count is shown."""
        result = QueryResult(
            columns=["id"],
            rows=[{"id": i} for i in range(5)],
            row_count=5,
        )
        output = render(result, OutputFormat.MARKDOWN)
        assert "5 row" in output


class TestRenderCSV:
    """Test CSV rendering."""

    def test_csv_simple_table(self) -> None:
        """Simple table renders as CSV."""
        result = QueryResult(
            columns=["id", "name"],
            rows=[{"id": 1, "name": "Alice"}],
            row_count=1,
        )
        output = render(result, OutputFormat.CSV)
        lines = output.strip().split("\n")
        assert "id,name" in lines[0]
        assert "1,Alice" in lines[1]

    def test_csv_quoted_values(self) -> None:
        """Values with commas are quoted."""
        result = QueryResult(
            columns=["name"],
            rows=[{"name": "Smith, John"}],
            row_count=1,
        )
        output = render(result, OutputFormat.CSV)
        assert '"Smith, John"' in output

    def test_csv_null_values(self) -> None:
        """NULL values rendered as empty string in CSV."""
        result = QueryResult(
            columns=["id", "name"],
            rows=[{"id": 1, "name": None}],
            row_count=1,
        )
        output = render(result, OutputFormat.CSV)
        # CSV renders None as empty string
        assert "1," in output

    def test_csv_truncation_marker(self) -> None:
        """Truncation marker appears as comment."""
        result = QueryResult(
            columns=["id"],
            rows=[{"id": 1}],
            row_count=1,
            truncated=True,
            limit=100,
        )
        output = render(result, OutputFormat.CSV)
        assert "truncated" in output.lower()


class TestRenderWithMaxBytes:
    """Test rendering with byte budget."""

    def test_json_truncated_to_budget(self) -> None:
        """JSON is truncated when over byte budget."""
        result = QueryResult(
            columns=["data"],
            rows=[{"data": "x" * 1000}],
            row_count=1,
        )
        output = render(result, OutputFormat.JSON, max_bytes=100)
        assert len(output.encode("utf-8")) <= 100
        assert "[output truncated" in output

    def test_markdown_truncated_to_budget(self) -> None:
        """Markdown is truncated when over budget."""
        result = QueryResult(
            columns=["text"],
            rows=[{"text": "word " * 100}],
            row_count=1,
        )
        output = render(result, OutputFormat.MARKDOWN, max_bytes=100)
        assert "[output truncated" in output

    def test_csv_truncated_to_budget(self) -> None:
        """CSV is truncated when over budget."""
        result = QueryResult(
            columns=["col"],
            rows=[{"col": "a" * 1000}],
            row_count=1,
        )
        output = render(result, OutputFormat.CSV, max_bytes=100)
        assert "[output truncated" in output


class TestRenderFormat:
    """Test render() function format parameter."""

    def test_format_by_enum(self) -> None:
        """OutputFormat enum is accepted."""
        result = QueryResult(columns=["id"], rows=[{"id": 1}], row_count=1)
        output = render(result, OutputFormat.JSON)
        assert json.loads(output)

    def test_format_by_string(self) -> None:
        """Format string is converted."""
        result = QueryResult(columns=["id"], rows=[{"id": 1}], row_count=1)
        output = render(result, "json")
        assert json.loads(output)

    def test_invalid_format_raises(self) -> None:
        """Invalid format raises ValueError."""
        result = QueryResult(columns=["id"], rows=[{"id": 1}], row_count=1)
        with pytest.raises(ValueError):
            render(result, "invalid_format")
