from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, HTTPException, Query, Body, Cookie, Response, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Dict, Optional
from .db import get_db
from .query_service import (
    get_tables, 
    get_catalogs,
    get_schemas
)
from .enrichment_service import (
    generate_table_description,
    generate_column_descriptions,
    update_table_comment,
    update_column_comments,
    get_current_table_description,
    get_column_metadata,
    _is_missing_description
)
from .auth_service import (
    authenticate_user,
    create_session,
    get_session,
    delete_session,
    get_user_from_session,
    get_credentials_from_session
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


class LoginRequest(BaseModel):
    username: str
    password: str


class TableDescriptionUpdate(BaseModel):
    description: str


class ColumnDescriptionsUpdate(BaseModel):
    column_descriptions: Dict[str, str]


# Dependency to check authentication
def get_current_user(session_id: Optional[str] = Cookie(None)):
    """Dependency to get current authenticated user"""
    if not session_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    user = get_user_from_session(session_id)
    if not user:
        raise HTTPException(status_code=401, detail="Session expired or invalid")
    
    return user


# ============== AUTH ENDPOINTS ==============

@app.post("/api/login")
async def login(credentials: LoginRequest, response: Response):
    """Login with Databricks username and password"""
    try:
        # Authenticate user
        auth_data = authenticate_user(credentials.username, credentials.password)
        
        if not auth_data:
            raise HTTPException(status_code=401, detail="Invalid credentials")
        
        # Create session
        session_id = create_session(auth_data)
        
        # Set session cookie
        response.set_cookie(
            key="session_id",
            value=session_id,
            httponly=True,
            max_age=28800,  # 8 hours
            samesite="lax"
        )
        
        return {"success": True, "user": auth_data["user_info"]}
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"LOGIN ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Login failed: {str(e)}")


@app.post("/api/logout")
async def logout(response: Response, session_id: Optional[str] = Cookie(None)):
    """Logout user"""
    if session_id:
        delete_session(session_id)
    
    response.delete_cookie("session_id")
    return {"success": True}


@app.get("/api/me")
async def get_current_user_info(user: dict = Depends(get_current_user)):
    """Get current user information"""
    return {"user": user}


# ============== PROTECTED ENDPOINTS ==============

@app.get("/api/catalogs")
async def get_all_catalogs(user: dict = Depends(get_current_user)):
    """Get all available catalogs"""
    try:
        print("API: Getting catalogs...")
        
        result = await asyncio.wait_for(
            asyncio.to_thread(lambda: _get_catalogs_sync()),
            timeout=60.0
        )
        catalogs = result
        
        print(f"API: Successfully returned {len(catalogs)} catalogs")
        return {"catalogs": catalogs}
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Request timed out")
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


def _get_catalogs_sync():
    with get_db() as connection:
        return get_catalogs(connection)


@app.get("/api/schemas")
async def get_all_schemas(catalog: str = Query(...), user: dict = Depends(get_current_user)):
    """Get all schemas in a catalog"""
    try:
        print(f"API: Getting schemas for catalog {catalog}...")
        
        result = await asyncio.wait_for(
            asyncio.to_thread(lambda: _get_schemas_sync(catalog)),
            timeout=60.0
        )
        schemas = result
        
        print(f"API: Successfully returned {len(schemas)} schemas")
        return {"schemas": schemas}
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Request timed out")
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
    schema: str = Query("default"),
    user: dict = Depends(get_current_user)
):
    """Get all tables in the specified catalog and schema"""
    try:
        print(f"API: Getting tables from {catalog}.{schema}...")
        
        result = await asyncio.wait_for(
            asyncio.to_thread(lambda: _get_tables_sync(catalog, schema)),
            timeout=60.0
        )
        tables = result
        
        print(f"API: Successfully returned {len(tables)} tables")
        return {"tables": tables, "catalog": catalog, "schema": schema}
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Request timed out")
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
    table: str = Query(...),
    user: dict = Depends(get_current_user)
):
    """Get current description of a table"""
    try:
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


@app.get("/api/column-metadata")
async def get_columns(
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    user: dict = Depends(get_current_user)
):
    """Get metadata for all columns in a table"""
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(lambda: _get_column_metadata_sync(catalog, schema, table)),
            timeout=30.0
        )
        
        return {"columns": result}
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Request timed out")
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")


def _get_column_metadata_sync(catalog, schema, table):
    with get_db() as connection:
        return get_column_metadata(connection, catalog, schema, table)


@app.post("/api/generate-description")
async def generate_description(
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    user: dict = Depends(get_current_user),
    session_id: Optional[str] = Cookie(None)
):
    """Generate a new description for the table using LLM"""
    try:
        print(f"API: Generating description for {catalog}.{schema}.{table}...")
        
        # Get user credentials from session
        credentials = get_credentials_from_session(session_id)
        if not credentials:
            raise HTTPException(status_code=401, detail="No valid credentials")
        
        username, password = credentials
        server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        
        # For LLM calls, we still need the M2M token
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
        
        from .db import get_oauth_token
        access_token = get_oauth_token(client_id, client_secret, server_hostname)
        
        result = await asyncio.wait_for(
            asyncio.to_thread(
                _generate_description_sync, 
                catalog, schema, table,
                access_token, server_hostname
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


def _generate_description_sync(catalog, schema, table, access_token, server_hostname):
    """Generate description using access token"""
    with get_db() as connection:
        return generate_table_description(
            connection, catalog, schema, table,
            access_token, server_hostname
        )


@app.post("/api/generate-column-descriptions")
async def generate_col_descriptions(
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    user: dict = Depends(get_current_user),
    session_id: Optional[str] = Cookie(None)
):
    """Generate descriptions for columns missing them"""
    try:
        print(f"API: Generating column descriptions for {catalog}.{schema}.{table}...")
        
        server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
        
        from .db import get_oauth_token
        access_token = get_oauth_token(client_id, client_secret, server_hostname)
        
        result = await asyncio.wait_for(
            asyncio.to_thread(
                _generate_column_descriptions_sync,
                catalog, schema, table,
                access_token, server_hostname
            ),
            timeout=120.0
        )
        
        return {"columns": result}
    
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="LLM request timed out. Please try again.")
    except HTTPException:
        raise
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error generating column descriptions: {str(e)}")


def _generate_column_descriptions_sync(catalog, schema, table, access_token, server_hostname):
    """Generate column descriptions using access token"""
    with get_db() as connection:
        return generate_column_descriptions(
            connection, catalog, schema, table,
            access_token, server_hostname
        )


@app.post("/api/update-description")
async def update_description(
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    body: TableDescriptionUpdate = None,
    user: dict = Depends(get_current_user)
):
    """Update the table description"""
    try:
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


@app.post("/api/update-column-descriptions")
async def update_col_descriptions(
    catalog: str = Query(...),
    schema: str = Query(...),
    table: str = Query(...),
    body: ColumnDescriptionsUpdate = None,
    user: dict = Depends(get_current_user)
):
    """Update column descriptions"""
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(
                lambda: _update_column_descriptions_sync(
                    catalog, schema, table, body.column_descriptions
                )
            ),
            timeout=30.0
        )
        
        return {"success": True, "message": "Column descriptions updated successfully"}
    
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Update request timed out")
    except Exception as e:
        print(f"API ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Error updating column descriptions: {str(e)}")


def _update_column_descriptions_sync(catalog, schema, table, column_descriptions):
    with get_db() as connection:
        update_column_comments(connection, catalog, schema, table, column_descriptions)


@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "message": "App is running"}


# Serve index.html for all other routes (catch-all for React Router)
@app.get("/{full_path:path}")
def serve_react_app(full_path: str):
    if full_path.startswith("api/"):
        return JSONResponse({"error": "Not found"}, status_code=404)
    
    index_path = os.path.join(FRONTEND_BUILD_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return JSONResponse({"message": "Frontend build not found."}, status_code=404)