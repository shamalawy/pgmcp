from mcp.server.fastmcp import FastMCP
import psycopg2
import psycopg2.extras
import os
import json
from typing import List, Dict, Any, Optional

# Create an MCP server
mcp = FastMCP("PostgreSQL Database Inspector")

# Database connection parameters
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5433"),
    "database": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres")
}

def get_connection():
    """Get a database connection."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        raise Exception(f"Failed to connect to database: {str(e)}")

def execute_query(query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Execute a query and return results as a list of dictionaries."""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params)
            if cur.description:
                return [dict(row) for row in cur.fetchall()]
            return []

@mcp.tool()
def list_schemas() -> str:
    """
    List all schemas in the current database.
    
    Returns:
        str: JSON string containing list of schema names and their details.
    """
    query = """
    SELECT 
        schema_name,
        schema_owner
    FROM information_schema.schemata 
    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
    ORDER BY schema_name;
    """
    
    try:
        results = execute_query(query)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error listing schemas: {str(e)}"

@mcp.tool()
def list_tables(schema_name: str = "public") -> str:
    """
    List all tables in a specific schema.
    
    Args:
        schema_name (str): The schema name to list tables from (default: "public").
    
    Returns:
        str: JSON string containing list of tables with their details.
    """
    query = """
    SELECT 
        table_name,
        table_type,
        table_schema
    FROM information_schema.tables 
    WHERE table_schema = %s
    ORDER BY table_name;
    """
    
    try:
        results = execute_query(query, (schema_name,))
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error listing tables in schema '{schema_name}': {str(e)}"

@mcp.tool()
def describe_table(table_name: str, schema_name: str = "public") -> str:
    """
    Get detailed information about a table including columns, types, and constraints.
    
    Args:
        table_name (str): The name of the table to describe.
        schema_name (str): The schema name (default: "public").
    
    Returns:
        str: JSON string containing table structure details.
    """
    # Get column information
    columns_query = """
    SELECT 
        column_name,
        data_type,
        is_nullable,
        column_default,
        character_maximum_length,
        numeric_precision,
        numeric_scale,
        ordinal_position
    FROM information_schema.columns 
    WHERE table_name = %s AND table_schema = %s
    ORDER BY ordinal_position;
    """
    
    # Get constraints information
    constraints_query = """
    SELECT 
        tc.constraint_name,
        tc.constraint_type,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM information_schema.table_constraints tc
    LEFT JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
    LEFT JOIN information_schema.constraint_column_usage ccu 
        ON tc.constraint_name = ccu.constraint_name
    WHERE tc.table_name = %s AND tc.table_schema = %s;
    """
    
    try:
        columns = execute_query(columns_query, (table_name, schema_name))
        constraints = execute_query(constraints_query, (table_name, schema_name))
        
        result = {
            "table_name": table_name,
            "schema_name": schema_name,
            "columns": columns,
            "constraints": constraints
        }
        
        return json.dumps(result, indent=2)
    except Exception as e:
        return f"Error describing table '{schema_name}.{table_name}': {str(e)}"

@mcp.tool()
def get_sample_data(table_name: str, schema_name: str = "public", limit: int = 10) -> str:
    """
    Get sample data from a table.
    
    Args:
        table_name (str): The name of the table to sample from.
        schema_name (str): The schema name (default: "public").
        limit (int): Maximum number of rows to return (default: 10).
    
    Returns:
        str: JSON string containing sample data rows.
    """
    query = f"""
    SELECT * FROM {schema_name}.{table_name} 
    LIMIT %s;
    """
    
    try:
        results = execute_query(query, (limit,))
        return json.dumps(results, indent=2, default=str)  # default=str to handle datetime objects
    except Exception as e:
        return f"Error getting sample data from '{schema_name}.{table_name}': {str(e)}"

@mcp.tool()
def execute_sql_query(query: str) -> str:
    """
    Execute a custom SQL query (SELECT statements only for safety).
    
    Args:
        query (str): The SQL query to execute (must be a SELECT statement).
    
    Returns:
        str: JSON string containing query results.
    """
    # Basic safety check - only allow SELECT statements
    query_trimmed = query.strip().upper()
    if not query_trimmed.startswith('SELECT'):
        return "Error: Only SELECT statements are allowed for security reasons."
    
    try:
        results = execute_query(query)
        return json.dumps(results, indent=2, default=str)
    except Exception as e:
        return f"Error executing query: {str(e)}"

@mcp.tool()
def get_database_overview() -> str:
    """
    Get a comprehensive overview of the entire database structure.
    
    Returns:
        str: JSON string containing database overview with schemas, tables, and basic stats.
    """
    overview_query = """
    SELECT 
        s.schema_name,
        COUNT(t.table_name) as table_count,
        string_agg(t.table_name, ', ' ORDER BY t.table_name) as tables
    FROM information_schema.schemata s
    LEFT JOIN information_schema.tables t 
        ON s.schema_name = t.table_schema 
        AND t.table_type = 'BASE TABLE'
    WHERE s.schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
    GROUP BY s.schema_name
    ORDER BY s.schema_name;
    """
    
    try:
        results = execute_query(overview_query)
        return json.dumps(results, indent=2)
    except Exception as e:
        return f"Error getting database overview: {str(e)}"

@mcp.resource("postgres://database/overview")
def get_database_structure() -> str:
    """
    Resource providing complete database structure information.
    
    Returns:
        str: Comprehensive database structure as JSON.
    """
    try:
        # Get all schemas
        schemas = execute_query("""
            SELECT schema_name FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
            ORDER BY schema_name;
        """)
        
        database_structure = {}
        
        for schema in schemas:
            schema_name = schema['schema_name']
            
            # Get tables for this schema
            tables = execute_query("""
                SELECT table_name, table_type 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                ORDER BY table_name;
            """, (schema_name,))
            
            database_structure[schema_name] = {}
            
            for table in tables:
                table_name = table['table_name']
                
                # Get columns for this table
                columns = execute_query("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns 
                    WHERE table_name = %s AND table_schema = %s
                    ORDER BY ordinal_position;
                """, (table_name, schema_name))
                
                database_structure[schema_name][table_name] = {
                    "type": table['table_type'],
                    "columns": columns
                }
        
        return json.dumps(database_structure, indent=2)
    except Exception as e:
        return f"Error getting database structure: {str(e)}"

@mcp.prompt()
def sql_generation_prompt() -> str:
    """
    Generate a prompt for SQL query generation based on current database structure.
    
    Returns:
        str: A prompt that includes database schema information for AI assistance.
    """
    try:
        overview = get_database_overview()
        return f"""
You are helping to write SQL queries for a PostgreSQL database. Here is the current database structure:

{overview}

Please use this information to write accurate SQL queries. When suggesting queries:
1. Use the correct schema and table names
2. Reference actual column names (use describe_table tool if needed)
3. Follow PostgreSQL syntax
4. Consider data types and constraints
5. Suggest appropriate JOINs based on foreign key relationships

What SQL query would you like help with?
"""
    except Exception as e:
        return f"Error generating SQL prompt: {str(e)}"

if __name__ == "__main__":
    # Run the MCP server
    mcp.run()
