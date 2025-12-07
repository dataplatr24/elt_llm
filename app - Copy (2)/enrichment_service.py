"""
Table and column description enrichment service using Databricks Foundation Models
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
        describe_rows = cursor.fetchall()
        
        for row in describe_rows:
            # Handle Row object properly
            if hasattr(row, '_mapping'):
                col_name = row._mapping.get('col_name') or row._values[0]
                col_type = row._mapping.get('data_type') or row._values[1]
                col_comment = row._mapping.get('comment') or (row._values[2] if len(row._values) > 2 else None)
            else:
                col_name = row[0]
                col_type = row[1]
                col_comment = row[2] if len(row) > 2 else None
            
            # Skip partition columns and metadata rows
            if col_name and not col_name.startswith('#'):
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
            # Use _mapping if available, otherwise iterate
            if hasattr(row, '_mapping'):
                row_dict = dict(row._mapping)
            else:
                for i, col in enumerate(col_names):
                    if i < len(row._values if hasattr(row, '_values') else row):
                        value = row._values[i] if hasattr(row, '_values') else row[i]
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
            if hasattr(row, '_mapping'):
                table_name = row._mapping.get('table_name') or row._values[0]
                comment = row._mapping.get('comment') or row._values[1]
            else:
                table_name = row[0]
                comment = row[1]
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
    if not server_hostname or server_hostname == "None":
        raise ValueError("server_hostname is required but was None or empty")
    
    url = f"https://{server_hostname}{LLM_ENDPOINT}"
    
    print(f"DEBUG: Calling LLM at {url}")
    
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
        print(f"DEBUG: URL was: {url}")
        if 'response' in locals():
            print(f"DEBUG: Response: {response.text}")
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
    
    print(f"DEBUG: generate_table_description called with hostname={server_hostname}")
    
    if not server_hostname or server_hostname == "None":
        raise ValueError(f"server_hostname is required but got: {server_hostname}")
    
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


def generate_column_descriptions(
    connection,
    catalog: str,
    schema: str,
    table: str,
    access_token: str,
    server_hostname: str
) -> List[Dict]:
    """Generate descriptions for columns that are missing them"""
    
    print(f"DEBUG: Generating column descriptions for {catalog}.{schema}.{table}")
    
    # Get table metadata
    table_meta = get_table_metadata(connection, catalog, schema, table)
    
    # Get current table description
    table_description = get_current_table_description(connection, catalog, schema, table)
    
    # Find columns missing descriptions
    missing_columns = [col for col in table_meta["columns"] if _is_missing_description(col.get("description"))]
    
    if not missing_columns:
        print("DEBUG: No columns need descriptions")
        return []
    
    # Columns with existing descriptions for context
    other_cols_context = [
        {"name": col["name"], "type": col["type"], "description": col.get("description")}
        for col in table_meta["columns"]
        if not _is_missing_description(col.get("description"))
    ]
    
    # Prepare sample values for missing columns
    sample_values = {}
    if table_meta["sample_data"]:
        for col in missing_columns:
            vals = []
            for row in table_meta["sample_data"]:
                val = row.get(col["name"])
                if val is not None and str(val) not in vals:
                    vals.append(str(val))
                if len(vals) >= 5:
                    break
            sample_values[col["name"]] = vals
    
    # Build prompt
    cols_for_prompt = [{"name": col["name"], "type": col["type"]} for col in missing_columns]
    
    prompt = f"""You are enriching metadata for columns of a Databricks table.

Table: {table}
Schema: {schema}
Catalog: {catalog}

Table description:
{table_description or "No description"}

Other columns with descriptions:
{json.dumps(other_cols_context, indent=2)}

Columns needing descriptions:
{json.dumps(cols_for_prompt, indent=2)}

Sample values for missing columns:
{json.dumps(sample_values, indent=2)}

Task:
Suggest clear, concise descriptions for each missing column.
Keep descriptions consistent with the style of existing ones.

Return ONLY a JSON object in this exact format:
{{"columns": [{{"name": "col1", "description": "..."}}, {{"name": "col2", "description": "..."}}]}}

Do not include any other text, markdown, or explanation. Only the JSON object.
"""
    
    print(f"DEBUG: Calling LLM for column descriptions...")
    response = call_databricks_llm(prompt, access_token, server_hostname)
    
    # Parse JSON response
    try:
        # Try to extract JSON if wrapped in markdown
        if "```json" in response:
            response = response.split("```json")[1].split("```")[0].strip()
        elif "```" in response:
            response = response.split("```")[1].split("```")[0].strip()
        
        result = json.loads(response)
        columns = result.get("columns", [])
        print(f"DEBUG: Generated descriptions for {len(columns)} columns")
        return columns
    
    except json.JSONDecodeError as e:
        print(f"ERROR: Failed to parse LLM response as JSON: {e}")
        print(f"Response was: {response}")
        return []


def update_column_comments(
    connection,
    catalog: str,
    schema: str,
    table: str,
    column_descriptions: Dict[str, str]
):
    """Update column comments in Databricks"""
    full_table_name = f"{catalog}.{schema}.{table}"
    
    cursor = connection.cursor()
    try:
        for col_name, description in column_descriptions.items():
            # Escape single quotes in description
            escaped_desc = description.replace("'", "''")
            
            query = f"ALTER TABLE {full_table_name} CHANGE COLUMN `{col_name}` COMMENT '{escaped_desc}'"
            
            print(f"DEBUG: Updating comment for column {col_name}")
            cursor.execute(query)
        
        print(f"DEBUG: All column comments updated successfully")
    except Exception as e:
        print(f"ERROR: Failed to update column comments: {e}")
        raise
    finally:
        cursor.close()


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
            row = rows[0]
            if hasattr(row, '_mapping'):
                comment = row._mapping.get('comment') or row._values[0]
            else:
                comment = row[0]
            return comment
        return None
    except Exception as e:
        print(f"Warning: Could not fetch table description: {e}")
        return None


def get_column_metadata(connection, catalog: str, schema: str, table: str) -> List[Dict]:
    """Get metadata for all columns in a table"""
    full_table_name = f"{catalog}.{schema}.{table}"
    
    describe_query = f"DESCRIBE TABLE {full_table_name}"
    cursor = connection.cursor()
    
    try:
        cursor.execute(describe_query)
        columns = []
        
        for row in cursor.fetchall():
            if hasattr(row, '_mapping'):
                col_name = row._mapping.get('col_name') or row._values[0]
                col_type = row._mapping.get('data_type') or row._values[1]
                col_comment = row._mapping.get('comment') or (row._values[2] if len(row._values) > 2 else None)
            else:
                col_name = row[0]
                col_type = row[1]
                col_comment = row[2] if len(row) > 2 else None
            
            # Skip partition columns and metadata rows
            if col_name and not col_name.startswith('#'):
                columns.append({
                    "name": col_name,
                    "type": col_type,
                    "description": col_comment,
                    "is_missing": _is_missing_description(col_comment)
                })
        
        return columns
    finally:
        cursor.close()