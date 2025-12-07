"""
Simple authentication service using Databricks username/password
"""
import os
import secrets
import requests
from datetime import datetime, timedelta
from typing import Optional, Dict

# In-memory session storage
sessions: Dict[str, Dict] = {}

DATABRICKS_HOST = os.getenv("DATABRICKS_SERVER_HOSTNAME")
SESSION_TIMEOUT_MINUTES = 480  # 8 hours


def authenticate_user(username: str, password: str) -> Optional[Dict]:
    """
    Authenticate user against Databricks using username/password
    Returns user info and access token if successful
    """
    try:
        # Try to get a token using basic auth
        token_url = f"https://{DATABRICKS_HOST}/api/2.0/token/create"
        
        # First, verify credentials by making a simple API call
        response = requests.get(
            f"https://{DATABRICKS_HOST}/api/2.0/clusters/list",
            auth=(username, password),
            timeout=30
        )
        
        if response.status_code == 401:
            print("ERROR: Invalid credentials")
            return None
        
        if response.status_code != 200:
            print(f"ERROR: Authentication failed with status {response.status_code}")
            return None
        
        # Credentials are valid, create a personal access token
        token_response = requests.post(
            token_url,
            auth=(username, password),
            json={
                "comment": "Table Enrichment App Session",
                "lifetime_seconds": SESSION_TIMEOUT_MINUTES * 60
            },
            timeout=30
        )
        
        if token_response.status_code == 200:
            token_data = token_response.json()
            access_token = token_data.get("token_value")
        else:
            # If token creation fails, use basic auth
            # Create a simple token representation
            import base64
            access_token = base64.b64encode(f"{username}:{password}".encode()).decode()
        
        # Get user info
        user_response = requests.get(
            f"https://{DATABRICKS_HOST}/api/2.0/preview/scim/v2/Me",
            auth=(username, password),
            timeout=30
        )
        
        user_info = {
            "email": username,
            "name": username.split("@")[0],
            "username": username
        }
        
        if user_response.status_code == 200:
            user_data = user_response.json()
            user_info["name"] = user_data.get("displayName", user_info["name"])
            user_info["email"] = user_data.get("emails", [{}])[0].get("value", username)
        
        return {
            "user_info": user_info,
            "access_token": access_token,
            "username": username,
            "password": password  # Store for future API calls
        }
    
    except Exception as e:
        print(f"ERROR: Authentication failed: {e}")
        return None


def create_session(auth_data: Dict) -> str:
    """Create a new session for the authenticated user"""
    session_id = secrets.token_urlsafe(32)
    
    sessions[session_id] = {
        "user_info": auth_data["user_info"],
        "access_token": auth_data["access_token"],
        "username": auth_data["username"],
        "password": auth_data["password"],
        "created_at": datetime.now().isoformat(),
        "expires_at": (datetime.now() + timedelta(minutes=SESSION_TIMEOUT_MINUTES)).isoformat()
    }
    
    return session_id


def get_session(session_id: str) -> Optional[Dict]:
    """Get session data"""
    if not session_id or session_id not in sessions:
        return None
    
    session = sessions[session_id]
    
    # Check if session expired
    expires_at = datetime.fromisoformat(session["expires_at"])
    if datetime.now() > expires_at:
        del sessions[session_id]
        return None
    
    return session


def delete_session(session_id: str):
    """Delete a session (logout)"""
    if session_id in sessions:
        # Try to revoke token if it exists
        session = sessions[session_id]
        try:
            # Attempt to revoke the token
            if session.get("access_token") and not session["access_token"].startswith("Basic"):
                requests.post(
                    f"https://{DATABRICKS_HOST}/api/2.0/token/delete",
                    auth=(session["username"], session["password"]),
                    json={"token_id": session.get("token_id")},
                    timeout=10
                )
        except:
            pass
        
        del sessions[session_id]


def get_user_from_session(session_id: str) -> Optional[Dict]:
    """Get user info from session"""
    session = get_session(session_id)
    if session:
        return session.get("user_info")
    return None


def get_credentials_from_session(session_id: str) -> Optional[tuple]:
    """Get username/password from session for API calls"""
    session = get_session(session_id)
    if session:
        return (session.get("username"), session.get("password"))
    return None


def verify_databricks_access(username: str, password: str) -> bool:
    """Verify user has access to Databricks"""
    try:
        response = requests.get(
            f"https://{DATABRICKS_HOST}/api/2.0/clusters/list",
            auth=(username, password),
            timeout=30
        )
        return response.status_code == 200
    except:
        return False