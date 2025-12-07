import os
import requests
import time
from contextlib import contextmanager
from dotenv import load_dotenv
load_dotenv() 

class DatabricksConnection:
    def __init__(self, server_hostname, http_path, access_token):
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.base_url = f"https://{server_hostname}/api/2.0/sql/statements"
        
    def cursor(self):
        return DatabricksCursor(self)
    
    def close(self):
        pass

class DatabricksCursor:
    def __init__(self, connection):
        self.connection = connection
        self.description = None
        self._results = []
        
    def execute(self, query):
        print(f"DEBUG: Executing query via REST API...")
        
        headers = {
            "Authorization": f"Bearer {self.connection.access_token}",
            "Content-Type": "application/json"
        }
        
        warehouse_id = self.connection.http_path.split('/')[-1]
        
        payload = {
            "statement": query,
            "warehouse_id": warehouse_id,
            "wait_timeout": "50s",  # Changed from 120s to 50s (max allowed)
            "on_wait_timeout": "CONTINUE"
        }
        
        response = requests.post(
            self.connection.base_url,
            headers=headers,
            json=payload,
            timeout=120
        )
        
        if response.status_code != 200:
            raise Exception(f"Query failed: {response.text}")
        
        result = response.json()
        statement_id = result.get("statement_id")
        
        # Poll for results if still executing
        max_polls = 60  # Maximum number of polls (60 * 2 seconds = 2 minutes)
        poll_count = 0
        
        while result.get("status", {}).get("state") in ["PENDING", "RUNNING"]:
            if poll_count >= max_polls:
                raise Exception(f"Query exceeded maximum execution time")
            
            print(f"DEBUG: Query still running, polling... ({poll_count + 1}/{max_polls})")
            time.sleep(2)
            poll_count += 1
            
            response = requests.get(
                f"{self.connection.base_url}/{statement_id}",
                headers=headers,
                timeout=30
            )
            result = response.json()
        
        if result.get("status", {}).get("state") != "SUCCEEDED":
            raise Exception(f"Query failed: {result.get('status', {}).get('error')}")
        
        # Parse results
        manifest = result.get("manifest", {})
        chunks = manifest.get("chunks", [])
        
        if chunks:
            # Get column names
            schema = manifest.get("schema", {}).get("columns", [])
            self.description = [(col["name"], None, None, None, None, None, None) for col in schema]
            
            # Fetch data from chunks
            for chunk in chunks:
                chunk_response = requests.get(
                    f"{self.connection.base_url}/{statement_id}/result/chunks/{chunk['chunk_index']}",
                    headers=headers,
                    timeout=30
                )
                chunk_data = chunk_response.json()
                self._results.extend(chunk_data.get("data_array", []))
        else:
            self.description = []
            self._results = []
        
        print(f"DEBUG: Query completed, {len(self._results)} rows")
        
    def fetchall(self):
        # Convert arrays to Row objects with _mapping
        class Row:
            def __init__(self, values, columns):
                self._values = values
                self._columns = columns
                self._data = dict(zip(columns, values))
                self._mapping = self._data
            
            def __iter__(self):
                # Make Row iterable so it works with dict(zip(columns, row))
                return iter(self._values)
            
            def __getitem__(self, key):
                # Support both numeric indexing and key access
                if isinstance(key, int):
                    return self._values[key]
                return self._data[key]
                
        columns = [col[0] for col in self.description] if self.description else []
        return [Row(row, columns) for row in self._results]
    
    def close(self):
        pass

def get_oauth_token(client_id, client_secret, server_hostname):
    """Get M2M OAuth token"""
    print("DEBUG: Getting M2M OAuth token...")
    
    token_url = f"https://{server_hostname}/oidc/v1/token"
    
    response = requests.post(
        token_url,
        data={
            "grant_type": "client_credentials",
            "scope": "all-apis"
        },
        auth=(client_id, client_secret),
        timeout=30
    )
    
    if response.status_code != 200:
        raise Exception(f"Failed to get OAuth token: {response.text}")
    
    token = response.json().get("access_token")
    print("DEBUG: OAuth token obtained successfully")
    return token

@contextmanager
def get_db():
    """Get a Databricks SQL connection using REST API"""
    
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_SQL_HTTP_PATH")
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

    server_hostname = "dbc-4a33b48b-e7a2.cloud.databricks.com"
    http_path =  "/sql/1.0/warehouses/9069ece157d70975"
    client_id = "bfc5d5b4-0d3c-45cd-b92d-83c334ae27f8"
    client_secret = "dosef7815f63ffaa0ae6aaa9ade2101a6ac3"
    
    print(f"DEBUG: Connecting to {server_hostname} with warehouse {http_path}")
    
    if not all([server_hostname, http_path, client_id, client_secret]):
        raise ValueError("Missing required Databricks credentials")
    
    try:
        # Get OAuth token
        access_token = get_oauth_token(client_id, client_secret, server_hostname)
        
        # Create connection
        connection = DatabricksConnection(server_hostname, http_path, access_token)
        print("DEBUG: Connection created")
        
        yield connection
        
    except Exception as e:
        print(f"ERROR: Connection failed: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        try:
            connection.close()
            print("DEBUG: Connection closed")
        except:
            pass