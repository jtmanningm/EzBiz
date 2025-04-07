# pages/auth/unified_login.py
import streamlit as st
from database.connection import snowflake_conn
from utils.auth.auth_utils import verify_password, create_session
from utils.business.business_auth import create_business_session

def unified_login_page():
    """Simplified login page for testing"""
    st.title("Ez Biz Login")
    
    # Add debug mode toggle in development
    if st.secrets.get("environment") == "development":
        st.sidebar.checkbox("Debug Mode", key="debug_mode")
    
    with st.form("login_form"):
        email = st.text_input("Email Address")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Log In")
        
        if submit:
            if not email or not password:
                st.error("Please enter both email and password")
                return

            try:
                # Try business login first
                query = """
                SELECT 
                    PORTAL_USER_ID,
                    PASSWORD_HASH,
                    IS_ACTIVE
                FROM OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS
                WHERE EMAIL = ?
                """
                
                result = snowflake_conn.execute_query(query, [email.lower()])
                
                if result and len(result) > 0 and verify_password(password, result[0]['PASSWORD_HASH']):
                    if not result[0]['IS_ACTIVE']:
                        st.error("Business account is inactive. Please contact support.")
                        return
                        
                    # Create business session
                    session_id = create_business_session(
                        result[0]['PORTAL_USER_ID'],
                        st.session_state.get('client_ip', 'test-ip'),
                        st.session_state.get('user_agent', 'test-agent')
                    )
                    
                    if session_id:
                        st.session_state.business_session_id = session_id
                        st.session_state.show_settings = False
                        st.session_state.page = None
                        st.rerun()
                    else:
                        st.error("Failed to create business session")
                        return

                # Try customer login if business login failed
                query = """
                SELECT 
                    PORTAL_USER_ID,
                    CUSTOMER_ID,
                    PASSWORD_HASH,
                    IS_ACTIVE
                FROM OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS
                WHERE EMAIL = ?
                """
                
                result = snowflake_conn.execute_query(query, [email.lower()])
                
                if result and len(result) > 0 and verify_password(password, result[0]['PASSWORD_HASH']):
                    if not result[0]['IS_ACTIVE']:
                        st.error("Customer account is inactive. Please contact support.")
                        return
                    
                    # Create customer session
                    session_id = create_session(
                        result[0]['PORTAL_USER_ID'],
                        st.session_state.get('client_ip', 'test-ip'),
                        st.session_state.get('user_agent', 'test-agent')
                    )
                    
                    if not session_id:
                        st.error("Failed to create customer session")
                        return
                    
                    # Clear business-related session state
                    for key in ['business_session_id', 'show_settings']:
                        if key in st.session_state:
                            del st.session_state[key]
                    
                    # Set minimum required session state (matching registration flow)
                    st.session_state.update({
                        'customer_session_id': session_id,
                        'customer_id': result[0]['CUSTOMER_ID'],
                        'portal_user_id': result[0]['PORTAL_USER_ID'],
                        'page': 'portal_home'
                    })
                    
                    if st.session_state.get('debug_mode'):
                        st.write("Debug - Session state:", st.session_state)
                        
                    st.success("Login successful! Redirecting to customer portal...")
                    st.rerun()
                else:
                    st.error("Invalid email or password")
                    return

            except Exception as e:
                st.error("An error occurred during login")
                if st.session_state.get('debug_mode'):
                    st.exception(e)
                return

    # Registration and reset buttons
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Register Account", use_container_width=True):
            st.session_state.page = 'register'
            st.rerun()
            
    with col2:
        if st.button("Reset Password", use_container_width=True):
            st.session_state.page = 'reset'
            st.rerun()