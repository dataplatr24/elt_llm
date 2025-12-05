"""
Table description enrichment service using Databricks Foundation Models
"""
import os
import json
import requests
from typing import Dict, List, Optional

# LLM Model Configuration - Using Databricks Foundation Models
LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"
LLM_ENDPOINT = f"/serving-endpoints/{LLM_MODEL}/invocations"


def _is_missing_description(desc) -> bool:
    """Check if a description is missing or placeholder"""
    if desc is None:
        return True
    
    if not isinstance(desc, str):
        print(f"DEBUG: Non-string description detected: {desc} ({type(desc)})")
        return True
    
    s = desc.strip()
    if s == "":
        return True
    if s.lower() in ("null", "none", "nan", "n/a", "-"):
        return True
    
    # anything longer than 3 chars is valid
    if len(s) > 3:
        return False
    
    return True


def get_table_metadata(connection, catalog: str, schema: str, table: str) -> Dict:
    """Get table metadata including columns and sample data"""
    full_table_name = f"{catalog}.{schema}.{table}"
    
    # Get column information
    describe_query = f"DESCRIBE TABLE {full_table_name}"
    cursor = connection.cursor()
    
    try:
        cursor.execute(describe_query)
        columns = []
        for row in cursor.fetchall():
            col_name = row[0] if hasattr(row, '__getitem__') else row._values[0]
            col_type = row[1] if hasattr(row, '__getitem__') else row._values[1]
            col_comment = row[2] if len(row) > 2 else None
            
            columns.append({
                "name": col_name,
                "type": col_type,
                "description": col_comment
            })
        
        # Get sample data (first 20 rows)
        sample_query = f"SELECT * FROM {full_table_name} LIMIT 20"
        cursor.execute(sample_query)
        sample_rows = []
        col_names = [desc[0] for desc in cursor.description]
        
        for row in cursor.fetchall():
            row_dict = {}
            for i, col in enumerate(col_names):
                value = row[i] if hasattr(row, '__getitem__') else row._values[i]
                row_dict[col] = value
            sample_rows.append(row_dict)
        
        return {
            "table_name": table,
            "full_name": full_table_name,
            "columns": columns,
            "sample_data": sample_rows[:5]  # First 5 rows for context
        }
    
    finally:
        cursor.close()


def get_other_tables_context(connection, catalog: str, schema: str, current_table: str) -> List[Dict]:
    """Get descriptions of other tables in the schema for context"""
    query = f"""
    SELECT table_name, comment 
    FROM {catalog}.information_schema.tables 
    WHERE table_schema = '{schema}' 
    AND table_name != '{current_table}'
    LIMIT 10
    """
    
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        tables = []
        for row in cursor.fetchall():
            table_name = row[0] if hasattr(row, '__getitem__') else row._values[0]
            comment = row[1] if hasattr(row, '__getitem__') else row._values[1]
            tables.append({
                "table": table_name,
                "description": comment
            })
        return tables
    except Exception as e:
        print(f"Warning: Could not fetch other tables context: {e}")
        return []
    finally:
        cursor.close()


def call_databricks_llm(prompt: str, access_token: str, server_hostname: str) -> str:
    """Call Databricks Foundation Model API"""
    url = f"https://{server_hostname}{LLM_ENDPOINT}"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.3,
        "max_tokens": 2048
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        result = response.json()
        
        # Extract response from the API
        if "choices" in result and len(result["choices"]) > 0:
            return result["choices"][0]["message"]["content"]
        return result.get("text", "")
    
    except Exception as e:
        print(f"ERROR calling LLM: {e}")
        raise


def generate_table_description(
    connection,
    catalog: str,
    schema: str,
    table: str,
    access_token: str,
    server_hostname: str
) -> str:
    """Generate a table description using LLM"""
    
    # Get table metadata
    print(f"DEBUG: Fetching metadata for {catalog}.{schema}.{table}")
    table_meta = get_table_metadata(connection, catalog, schema, table)
    
    # Get context from other tables
    other_tables = get_other_tables_context(connection, catalog, schema, table)
    
    # Prepare column context
    column_context = []
    for col in table_meta["columns"]:
        # Get sample values from sample data
        sample_vals = []
        if table_meta["sample_data"]:
            for row in table_meta["sample_data"]:
                val = row.get(col["name"])
                if val is not None and str(val) not in sample_vals:
                    sample_vals.append(str(val))
                if len(sample_vals) >= 5:
                    break
        
        column_context.append({
            "name": col["name"],
            "type": col["type"],
            "description": col.get("description"),
            "sample_values": sample_vals
        })
    
    # Build prompt
    prompt = f"""You are enriching metadata for a Databricks table.

Current Table: {table}
Schema: {schema}
Catalog: {catalog}

Other tables in the schema with descriptions:
{json.dumps(other_tables, indent=2)}

Columns (with existing descriptions and sample values):
{json.dumps(column_context, indent=2)}

Task:
Suggest a clear and elaborate description for this table summarizing its purpose and content.
Make sure it is consistent with the style followed by other tables in the dataset.

Return ONLY a JSON object in this exact format:
{{"table_description": "your description here"}}

Do not include any other text, markdown, or explanation. Only the JSON object.
"""
    
    print(f"DEBUG: Calling LLM for table description...")
    response = call_databricks_llm(prompt, access_token, server_hostname)
    
    # Parse JSON response
    try:
        # Try to extract JSON if wrapped in markdown
        if "```json" in response:
            response = response.split("```json")[1].split("```")[0].strip()
        elif "```" in response:
            response = response.split("```")[1].split("```")[0].strip()
        
        result = json.loads(response)
        description = result.get("table_description", "")
        print(f"DEBUG: Generated description: {description[:100]}...")
        return description
    
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse LLM response as JSON: {e}")
        print(f"Response was: {response}")
        # Return raw response as fallback
        return response


def update_table_comment(
    connection,
    catalog: str,
    schema: str,
    table: str,
    description: str
):
    """Update table comment/description in Databricks"""
    full_table_name = f"{catalog}.{schema}.{table}"
    
    # Escape single quotes in description
    escaped_desc = description.replace("'", "''")
    
    query = f"COMMENT ON TABLE {full_table_name} IS '{escaped_desc}'"
    
    cursor = connection.cursor()
    try:
        print(f"DEBUG: Updating table comment for {full_table_name}")
        cursor.execute(query)
        print(f"DEBUG: Table description updated successfully")
    except Exception as e:
        print(f"ERROR: Failed to update table comment: {e}")
        raise
    finally:
        cursor.close()


def get_current_table_description(connection, catalog: str, schema: str, table: str) -> Optional[str]:
    """Get the current description of a table"""
    try:
        query = f"""
        SELECT comment 
        FROM {catalog}.information_schema.tables 
        WHERE table_schema = '{schema}' 
        AND table_name = '{table}'
        """
        
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        
        if rows and len(rows) > 0:
            comment = rows[0][0] if hasattr(rows[0], '__getitem__') else rows[0]._values[0]
            return comment
        return None
    except Exception as e:
        print(f"Warning: Could not fetch table description: {e}")
        return None