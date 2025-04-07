# /pages/business/auth/middleware.py
import streamlit as st
from functools import wraps
from datetime import datetime, timedelta
from typing import Callable
from utils.business.business_auth import verify_business_session
from database.connection import snowflake_conn

def init_business_session() -> None:
    """Initialize business session state and check timeout"""
    if 'last_activity' not in st.session_state:
        st.session_state.last_activity = datetime.now().timestamp()
    
    check_session_timeout()

def is_business_authenticated() -> bool:
    """Check if business user is authenticated with valid session"""
    session_id = st.session_state.get('business_session_id')
    if not session_id:
        return False
    
    # Validate session
    session_data = verify_business_session(session_id)
    if not session_data:
        clear_business_session("Invalid session")
        return False
    
    # Update session state
    st.session_state.employee_id = session_data['EMPLOYEE_ID']
    st.session_state.is_admin = session_data['IS_ADMIN']
    return True

def require_business_auth(func: Callable) -> Callable:
    """Decorator to require business authentication"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not is_business_authenticated():
            st.session_state.return_to = st.session_state.get('page')
            st.session_state.page = 'business_login'
            st.rerun()
        return func(*args, **kwargs)
    return wrapper

def check_session_timeout() -> None:
    """Check for session timeout and clear if inactive"""
    last_activity = st.session_state.get('last_activity')
    if last_activity:
        timeout = datetime.fromtimestamp(last_activity) + timedelta(hours=12)
        if datetime.now() > timeout:
            clear_business_session("Session timeout")
            st.rerun()
    
    # Update last activity
    st.session_state.last_activity = datetime.now().timestamp()

def clear_business_session(reason: str = None) -> None:
    """Clear business session state"""
    session_id = st.session_state.get('business_session_id')
    if session_id:
        # Update session in database
        query = """
        UPDATE OPERATIONAL.CARPET.BUSINESS_SESSIONS
        SET IS_ACTIVE = FALSE,
            LAST_ACTIVITY = CURRENT_TIMESTAMP()
        WHERE SESSION_ID = ?
        """
        try:
            snowflake_conn.execute_query(query, [session_id])
        except Exception as e:
            print(f"Error clearing session: {str(e)}")
    
    # Clear session state
    keys_to_clear = [
        'business_session_id',
        'employee_id', 
        'is_admin',
        'last_activity'
    ]
    for key in keys_to_clear:
        st.session_state.pop(key, None)