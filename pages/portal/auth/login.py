#pages.auth.login.py
import streamlit as st
from database.connection import snowflake_conn
from utils.auth.auth_utils import (
    verify_password,
    create_session,
    check_rate_limit,
    log_security_event
)
from datetime import datetime, timedelta

def get_client_info():
    """
    Get client information from session state or set defaults.
    In a production environment, you would want to implement proper
    client information gathering, possibly through a proxy or middleware.
    """
    if 'client_ip' not in st.session_state:
        st.session_state.client_ip = 'unknown'
    if 'user_agent' not in st.session_state:
        st.session_state.user_agent = 'unknown'
    
    return st.session_state.client_ip, st.session_state.user_agent

def customer_login_page():
    st.title("Customer Portal Login")
    
    # Get client info using the new helper function
    client_ip, user_agent = get_client_info()
    
    with st.form("login_form"):
        email = st.text_input("Email Address")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Log In")
        
        if submit:
            if not email or not password:
                st.error("Please enter both email and password")
                return
            
            # Check rate limits
            rate_check, message = check_rate_limit(client_ip, 'LOGIN_ATTEMPT')
            if not rate_check:
                st.error(message)
                return
            
            # Query user
            query = """
            SELECT 
                u.PORTAL_USER_ID,
                u.CUSTOMER_ID,
                u.EMAIL,
                u.PASSWORD_HASH,
                u.IS_ACTIVE,
                u.FAILED_LOGIN_ATTEMPTS,
                u.ACCOUNT_LOCKED,
                u.ACCOUNT_LOCKED_UNTIL
            FROM CUSTOMER_PORTAL_USERS u
            WHERE u.EMAIL = :1
            """
            
            try:
                result = snowflake_conn.execute_query(query, [email])
                if not result or len(result) == 0:
                    st.error("Invalid email or password")
                    log_security_event(
                        None, 'LOGIN_FAILED',
                        client_ip, user_agent,
                        f"Invalid email attempt: {email}"
                    )
                    return
                
                user = result[0].as_dict()
                
                # Check account status
                if not user['IS_ACTIVE']:
                    st.error("Account is inactive")
                    return
                    
                if user['ACCOUNT_LOCKED']:
                    if user['ACCOUNT_LOCKED_UNTIL'] > datetime.now():
                        st.error("Account is temporarily locked. Please try again later.")
                        return
                    else:
                        # Reset lock if time has expired
                        snowflake_conn.execute_query(
                            """
                            UPDATE CUSTOMER_PORTAL_USERS
                            SET ACCOUNT_LOCKED = FALSE,
                                FAILED_LOGIN_ATTEMPTS = 0
                            WHERE PORTAL_USER_ID = :1
                            """, 
                            [user['PORTAL_USER_ID']]
                        )
                
                # Verify password
                if verify_password(password, user['PASSWORD_HASH']):
                    # Create session
                    session_id = create_session(
                        user['PORTAL_USER_ID'],
                        client_ip,
                        user_agent
                    )
                    
                    if session_id:
                        # Reset failed attempts
                        snowflake_conn.execute_query(
                            """
                            UPDATE CUSTOMER_PORTAL_USERS
                            SET FAILED_LOGIN_ATTEMPTS = 0,
                                LAST_LOGIN_DATE = CURRENT_TIMESTAMP()
                            WHERE PORTAL_USER_ID = :1
                            """, 
                            [user['PORTAL_USER_ID']]
                        )
                        
                        # Set session in streamlit
                        st.session_state.customer_session_id = session_id
                        st.session_state.customer_id = user['CUSTOMER_ID']
                        st.session_state.portal_user_id = user['PORTAL_USER_ID']
                        
                        log_security_event(
                            user['PORTAL_USER_ID'], 'LOGIN_SUCCESS',
                            client_ip, user_agent,
                            "Successful login"
                        )
                        
                        st.session_state.page = 'portal_home'
                        st.rerun()
                    else:
                        st.error("Error creating session")
                else:
                    # Increment failed attempts
                    failed_attempts = user['FAILED_LOGIN_ATTEMPTS'] + 1
                    lock_account = failed_attempts >= 5
                    locked_until = None
                    
                    if lock_account:
                        locked_until = datetime.now() + timedelta(minutes=30)
                    
                    snowflake_conn.execute_query(
                        """
                        UPDATE CUSTOMER_PORTAL_USERS
                        SET FAILED_LOGIN_ATTEMPTS = :1,
                            ACCOUNT_LOCKED = :2,
                            ACCOUNT_LOCKED_UNTIL = :3
                        WHERE PORTAL_USER_ID = :4
                        """, 
                        [failed_attempts, lock_account, locked_until, user['PORTAL_USER_ID']]
                    )
                    
                    log_security_event(
                        user['PORTAL_USER_ID'], 'LOGIN_FAILED',
                        client_ip, user_agent,
                        f"Failed password attempt ({failed_attempts})"
                    )
                    
                    if lock_account:
                        st.error("Too many failed attempts. Account has been temporarily locked.")
                    else:
                        st.error("Invalid email or password")
                        
            except Exception as e:
                st.error("An error occurred during login")
                print(f"Login error: {str(e)}")
    
    # Login link
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("Already have an account?")
    with col2:
        if st.button("Login"):
            st.session_state.portal_mode = 'customer'  # Ensure we're in customer mode
            st.session_state.page = 'login'  # Set page to login
            st.rerun()