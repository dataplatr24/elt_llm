# Query service with table discovery functionality

def get_tables(connection, catalog="dev_uc", schema="default"):
    """Get all tables in the specified catalog and schema"""
    query = f"""
    SHOW TABLES IN {catalog}.{schema}
    """
    
    cursor = connection.cursor()
    try:
        print(f"DEBUG: Getting tables from {catalog}.{schema}...")
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        print(f"DEBUG: Columns = {columns}")
        
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        print(f"DEBUG: Found {len(rows)} tables")
        
        # Extract table names (usually in 'tableName' column)
        tables = []
        for row in rows:
            table_name = row.get('tableName') or row.get('table_name') or row.get('name')
            if table_name:
                tables.append({
                    'name': table_name,
                    'catalog': catalog,
                    'schema': schema,
                    'full_name': f"{catalog}.{schema}.{table_name}"
                })
        
        return tables
    except Exception as e:
        print(f"ERROR: Failed to get tables - {e}")
        raise
    finally:
        cursor.close()


def get_table_preview(connection, catalog, schema, table_name, limit=100):
    """Get preview data from a specific table"""
    full_table_name = f"{catalog}.{schema}.{table_name}"
    query = f"""
    SELECT * FROM {full_table_name}
    LIMIT {limit}
    """
    
    cursor = connection.cursor()
    try:
        print(f"DEBUG: Getting preview for {full_table_name}...")
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        print(f"DEBUG: Columns = {columns}")
        
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        print(f"DEBUG: Fetched {len(rows)} preview rows")
        
        return {
            'columns': columns,
            'rows': rows,
            'row_count': len(rows)
        }
    except Exception as e:
        print(f"ERROR: Failed to get table preview - {e}")
        raise
    finally:
        cursor.close()


def get_catalogs(connection):
    """Get all available catalogs"""
    query = "SHOW CATALOGS"
    
    cursor = connection.cursor()
    try:
        print("DEBUG: Getting catalogs...")
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Extract catalog names
        catalogs = []
        for row in rows:
            catalog_name = row.get('catalog') or row.get('catalogName') or row.get('name')
            if catalog_name:
                catalogs.append(catalog_name)
        
        print(f"DEBUG: Found {len(catalogs)} catalogs")
        return catalogs
    except Exception as e:
        print(f"ERROR: Failed to get catalogs - {e}")
        raise
    finally:
        cursor.close()


def get_schemas(connection, catalog):
    """Get all schemas in a catalog"""
    query = f"SHOW SCHEMAS IN {catalog}"
    
    cursor = connection.cursor()
    try:
        print(f"DEBUG: Getting schemas from {catalog}...")
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        # Extract schema names
        schemas = []
        for row in rows:
            schema_name = row.get('databaseName') or row.get('schema') or row.get('name')
            if schema_name:
                schemas.append(schema_name)
        
        print(f"DEBUG: Found {len(schemas)} schemas")
        return schemas
    except Exception as e:
        print(f"ERROR: Failed to get schemas - {e}")
        raise
    finally:
        cursor.close()


# Original finance query (keep for reference)
FINANCE_QUERY = """
SELECT
  department,
  quarter,
  total_budget,
  total_actual,
  (total_actual - total_budget) AS variance,
  CASE 
    WHEN total_budget = 0 THEN NULL
    ELSE ((total_actual - total_budget) / total_budget) * 100 
  END AS variance_pct
FROM dev_uc.default.budget_actuals
ORDER BY quarter, department
"""

def load_finance_data(connection):
    """Execute query and return results as list of dicts"""
    cursor = connection.cursor()
    try:
        print("DEBUG: Executing query...")
        cursor.execute(FINANCE_QUERY)
        
        columns = [desc[0] for desc in cursor.description]
        print(f"DEBUG: Columns = {columns}")
        
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        print(f"DEBUG: Fetched {len(rows)} rows")
        print(f"DEBUG: First row = {rows[0] if rows else 'No data'}")
        
        return rows
    except Exception as e:
        print(f"ERROR: Query failed - {e}")
        raise
    finally:
        cursor.close()