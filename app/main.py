from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from .db import get_db
from .query_service import (
    load_finance_data, 
    get_tables, 
    get_table_preview,
    get_catalogs,
    get_schemas
)
import os
import asyncio
from functools import wraps

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


class TimeoutException(Exception):
    pass


def timeout_decorator(seconds):
    """Cross-platform timeout decorator"""
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await asyncio.wait_for(
                    asyncio.to_thread(func, *args, **kwargs),
                    timeout=seconds
                )
            except asyncio.TimeoutError:
                raise TimeoutException(f"Operation timed out after {seconds} seconds")
        return wrapper
    return decorator


@app.get("/api/catalogs")
async def get_all_catalogs():
    """Get all available catalogs"""
    try:
        print("API: Getting catalogs...")
        
        # Run with timeout
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(lambda: _get_catalogs_sync()),
                timeout=60.0
            )
            catalogs = result
        except asyncio.TimeoutError:
            error_msg = "Database connection timed out. The SQL warehouse may be starting up. Please wait 30 seconds and try again."
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
    """Synchronous helper for getting catalogs"""
    with get_db() as connection:
        return get_catalogs(connection)


@app.get("/api/schemas")
async def get_all_schemas(catalog: str = Query(..., description="Catalog name")):
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
            error_msg = "Database connection timed out. The SQL warehouse may be starting up. Please wait 30 seconds and try again."
            print(f"API TIMEOUT: {error_msg}")
            raise HTTPException(status_code=504, detail=error_msg)
        
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
    """Synchronous helper for getting schemas"""
    with get_db() as connection:
        return get_schemas(connection, catalog)


@app.get("/api/tables")
async def get_all_tables(
    catalog: str = Query("dev_uc", description="Catalog name"),
    schema: str = Query("default", description="Schema name")
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
            error_msg = "Database connection timed out. The SQL warehouse may be starting up. Please wait 30 seconds and try again."
            print(f"API TIMEOUT: {error_msg}")
            raise HTTPException(status_code=504, detail=error_msg)
        
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
    """Synchronous helper for getting tables"""
    with get_db() as connection:
        return get_tables(connection, catalog, schema)


@app.get("/api/table-preview")
async def get_table_data(
    catalog: str = Query(..., description="Catalog name"),
    schema: str = Query(..., description="Schema name"),
    table: str = Query(..., description="Table name"),
    limit: int = Query(100, ge=1, le=1000, description="Number of rows to fetch")
):
    """Get preview data from a specific table"""
    try:
        print(f"API: Getting preview for {catalog}.{schema}.{table} (limit={limit})...")
        
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(lambda: _get_table_preview_sync(catalog, schema, table, limit)),
                timeout=60.0
            )
            preview_data = result
        except asyncio.TimeoutError:
            error_msg = "Query timed out. The table may be too large or the warehouse may be starting up."
            print(f"API TIMEOUT: {error_msg}")
            raise HTTPException(status_code=504, detail=error_msg)
        
        print(f"API: Successfully returned preview with {preview_data['row_count']} rows")
        return preview_data
    except HTTPException:
        raise
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def _get_table_preview_sync(catalog, schema, table, limit):
    """Synchronous helper for getting table preview"""
    with get_db() as connection:
        return get_table_preview(connection, catalog, schema, table, limit)


@app.get("/api/finance-data")
async def get_finance_data():
    """Fetch finance data from Databricks (legacy endpoint)"""
    try:
        print("API: Starting finance data request...")
        
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(lambda: _get_finance_data_sync()),
                timeout=60.0
            )
            rows = result
        except asyncio.TimeoutError:
            error_msg = "Database connection timed out. The SQL warehouse may be starting up. Please wait 30 seconds and try again."
            print(f"API TIMEOUT: {error_msg}")
            raise HTTPException(status_code=504, detail=error_msg)
        
        print(f"API: Successfully returned {len(rows)} rows")
        return {"rows": rows}
    except HTTPException:
        raise
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def _get_finance_data_sync():
    """Synchronous helper for getting finance data"""
    with get_db() as connection:
        return load_finance_data(connection)


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "message": "App is running"}


# Serve index.html for all other routes (catch-all for React Router)
@app.get("/{full_path:path}")
def serve_react_app(full_path: str):
    if full_path.startswith("api/") or full_path.startswith("static/"):
        return JSONResponse({"error": "Not found"}, status_code=404)
    
    index_path = os.path.join(FRONTEND_BUILD_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return JSONResponse({"message": "Frontend build not found."}, status_code=404)