import streamlit as st
from functools import wraps
from datetime import datetime, timedelta
from typing import Callable, Optional
from database.connection import snowflake_conn
from utils.auth.auth_utils import validate_session, log_security_event

def init_customer_session() -> None:
    """Initialize customer session state and check timeout"""
    if 'last_activity' not in st.session_state:
        st.session_state.last_activity = datetime.now().timestamp()
    
    check_session_timeout()

def is_customer_authenticated() -> bool:
    """Check if customer is authenticated with valid session"""
    session_id = st.session_state.get('customer_session_id')
    if not session_id:
        return False
    
    # Validate session
    session_data = validate_session(session_id)
    if not session_data:
        # Clear invalid session
        clear_customer_session("Invalid session")
        return False
    
    # Update session state with latest data
    st.session_state.customer_id = session_data['CUSTOMER_ID']
    st.session_state.portal_user_id = session_data['PORTAL_USER_ID']
    return True

def require_customer_auth(func: Callable) -> Callable:
    """Decorator to require customer authentication"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not is_customer_authenticated():
            st.session_state.return_to = st.session_state.get('page')
            st.session_state.page = 'login'
            st.rerun()
        return func(*args, **kwargs)
    return wrapper

def check_session_timeout() -> None:
    """Check for session timeout and clear if inactive"""
    last_activity = st.session_state.get('last_activity')
    if last_activity:
        timeout = datetime.fromtimestamp(last_activity) + timedelta(minutes=30)
        if datetime.now() > timeout:
            clear_customer_session("Session timeout due to inactivity")
            st.rerun()
    
    # Update last activity
    st.session_state.last_activity = datetime.now().timestamp()

def clear_customer_session(reason: Optional[str] = None) -> None:
    """Clear customer session and log event"""
    session_id = st.session_state.get('customer_session_id')
    portal_user_id = st.session_state.get('portal_user_id')
    
    if session_id:
        # Update session in database
        query = """
        UPDATE OPERATIONAL.CARPET.CUSTOMER_SESSIONS
        SET IS_ACTIVE = FALSE,
            MODIFIED_AT = CURRENT_TIMESTAMP()  
        WHERE SESSION_ID = ?
        """
        try:
            snowflake_conn.execute_query(query, [session_id])
            
            # Log session end if reason provided
            if reason and portal_user_id:
                log_security_event(
                    portal_user_id,
                    'SESSION_ENDED',
                    st.request.headers.get('X-Forwarded-For', 'unknown'),
                    st.request.headers.get('User-Agent', 'unknown'),
                    f"Session ended: {reason}"
                )
        except Exception as e:
            print(f"Error clearing session: {str(e)}")
    
    # Clear session state
    keys_to_clear = [
        'customer_session_id',
        'customer_id',
        'portal_user_id',
        'last_activity'
    ]
    for key in keys_to_clear:
        st.session_state.pop(key, None)