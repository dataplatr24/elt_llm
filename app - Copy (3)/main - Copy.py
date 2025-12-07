from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from .db import get_db, get_oauth_token
from .query_service import (
    load_finance_data, 
    get_tables, 
    get_table_preview,
    get_catalogs,
    get_schemas
)
from .enrichment_service import (
    generate_table_description,
    update_table_comment,
    get_current_table_description,
    _is_missing_description
)
import os
import asyncio

app = FastAPI(title="Finance Lakehouse App")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve React frontend build
FRONTEND_BUILD_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "frontend", "build"))

# Mount static files directory
static_dir = os.path.join(FRONTEND_BUILD_DIR, "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


class TableDescriptionUpdate(BaseModel):
    description: str


@app.get("/api/catalogs")
async def get_all_catalogs():
    """Get all available catalogs"""
    try:
        print("API: Getting catalogs...")
        
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(lambda: _get_catalogs_sync()),
                timeout=60.0
            )
            catalogs = result
        except asyncio.TimeoutError:
            error_msg = "Database connection timed out."
            print(f"API TIMEOUT: {error_msg}")
            raise HTTPException(status_code=504, detail=error_msg)
        
        print(f"API: Successfully returned {len(catalogs)} catalogs")
        return {"catalogs": catalogs}
    except HTTPException:
        raise
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def _get_catalogs_sync():
    with get_db() as connection:
        return get_catalogs(connection)


@app.get("/api/schemas")
async def get_all_schemas(catalog: str = Query(...)):
    """Get all schemas in a catalog"""
    try:
        print(f"API: Getting schemas for catalog {catalog}...")
        
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(lambda: _get_schemas_sync(catalog)),
                timeout=60.0
            )
            schemas = result
        except asyncio.TimeoutError:
            raise HTTPException(status_code=504, detail="Request timed out")
        
        print(f"API: Successfully returned {len(schemas)} schemas")
        return {"schemas": schemas}
    except HTTPException:
        raise
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def _get_schemas_sync(catalog):
    with get_db() as connection:
        return get_schemas(connection, catalog)


@app.get("/api/tables")
async def get_all_tables(
    catalog: str = Query("dev_uc"),
    schema: str = Query("default")
):
    """Get all tables in the specified catalog and schema"""
    try:
        print(f"API: Getting tables from {catalog}.{schema}...")
        
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(lambda: _get_tables_sync(catalog, schema)),
                timeout=60.0
            )
            tables = result
        except asyncio.TimeoutError:
            raise HTTPException(status_code=504, detail="Request timed out")
        
        print(f"API: Successfully returned {len(tables)} tables")
        return {"tables": tables, "catalog": catalog, "schema": schema}
    except HTTPException:
        raise
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def _get_tables_sync(catalog, schema):
    with get_db() as connection:
        return get_tables(connection, catalog, schema)


@app.get("/api/table-description")
async def get_table_description(
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...)
):
    """Get current description of a table"""
    try:
        print(f"API: Getting description for {catalog}.{schema}.{table}...")
        
        result = await asyncio.wait_for(
            asyncio.to_thread(lambda: _get_table_description_sync(catalog, schema, table)),
            timeout=30.0
        )
        
        return {
            "current_description": result,
            "is_missing": _is_missing_description(result)
        }
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Request timed out")
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


def _get_table_description_sync(catalog, schema, table):
    with get_db() as connection:
        return get_current_table_description(connection, catalog, schema, table)


@app.post("/api/generate-description")
async def generate_description(
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...)
):
    """Generate a new description for the table using LLM"""
    try:
        print(f"API: Generating description for {catalog}.{schema}.{table}...")
        
        # Get environment variables here in the async context
        server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")

        server_hostname = "dbc-4a33b48b-e7a2.cloud.databricks.com"
        http_path =  "/sql/1.0/warehouses/9069ece157d70975"
        client_id = "bfc5d5b4-0d3c-45cd-b92d-83c334ae27f8"
        client_secret = "dosef7815f63ffaa0ae6aaa9ade2101a6ac3"
        
        print(f"DEBUG: Environment check - hostname={server_hostname}")
        
        if not all([server_hostname, client_id, client_secret]):
            missing = []
            if not server_hostname: missing.append("DATABRICKS_SERVER_HOSTNAME")
            if not client_id: missing.append("DATABRICKS_CLIENT_ID")
            if not client_secret: missing.append("DATABRICKS_CLIENT_SECRET")
            raise HTTPException(
                status_code=500, 
                detail=f"Missing environment variables: {', '.join(missing)}"
            )
        
        result = await asyncio.wait_for(
            asyncio.to_thread(
                _generate_description_sync, 
                catalog, schema, table,
                server_hostname, client_id, client_secret
            ),
            timeout=120.0
        )
        
        return {"generated_description": result}
    
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="LLM request timed out. Please try again.")
    except HTTPException:
        raise
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error generating description: {str(e)}")


def _generate_description_sync(catalog, schema, table, server_hostname, client_id, client_secret):
    """Synchronous helper for generating description"""
    print(f"DEBUG: _generate_description_sync called with hostname={server_hostname}")
    
    # Get OAuth token
    access_token = get_oauth_token(client_id, client_secret, server_hostname)
    
    with get_db() as connection:
        return generate_table_description(
            connection, catalog, schema, table,
            access_token, server_hostname
        )


@app.post("/api/update-description")
async def update_description(
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    body: TableDescriptionUpdate = None
):
    """Update the table description"""
    try:
        print(f"API: Updating description for {catalog}.{schema}.{table}...")
        
        result = await asyncio.wait_for(
            asyncio.to_thread(
                lambda: _update_description_sync(catalog, schema, table, body.description)
            ),
            timeout=30.0
        )
        
        return {"success": True, "message": "Table description updated successfully"}
    
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Update request timed out")
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error updating description: {str(e)}")


def _update_description_sync(catalog, schema, table, description):
    with get_db() as connection:
        update_table_comment(connection, catalog, schema, table, description)


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "message": "App is running"}


@app.get("/{full_path:path}")
def serve_react_app(full_path: str):
    if full_path.startswith("api/") or full_path.startswith("static/"):
        return JSONResponse({"error": "Not found"}, status_code=404)
    
    index_path = os.path.join(FRONTEND_BUILD_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return JSONResponse({"message": "Frontend build not found."}, status_code=404)