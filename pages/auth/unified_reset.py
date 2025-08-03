# pages/auth/unified_reset.py
import streamlit as st
from datetime import datetime, timedelta
from typing import Tuple, Optional
from database.connection import snowflake_conn
from utils.auth.auth_utils import (
    validate_password, 
    hash_password,
    check_rate_limit,
    log_security_event
)
import uuid

def get_client_info() -> Tuple[str, str]:
    """Get client information from session state or set defaults."""
    if 'client_ip' not in st.session_state:
        st.session_state.client_ip = 'unknown'
    if 'user_agent' not in st.session_state:
        st.session_state.user_agent = 'unknown'
    
    return st.session_state.client_ip, st.session_state.user_agent

def generate_reset_token() -> str:
    """Generate a secure reset token"""
    return str(uuid.uuid4())

def unified_reset_page():
    """Unified password reset page for both business and customer users"""
    st.title("Password Reset")
    
    # Get client info for rate limiting
    client_ip, user_agent = get_client_info()
    
    # Check if we're in reset mode (with token) or request mode
    if 'reset_token' in st.query_params:
        # Password reset form with token
        token = st.query_params.get('reset_token')
        _show_reset_form(token, client_ip, user_agent)
    else:
        # Password reset request form
        _show_request_form(client_ip, user_agent)

def _show_request_form(client_ip: str, user_agent: str):
    """Show password reset request form"""
    st.markdown("Enter your email address to receive password reset instructions.")
    
    with st.form("reset_request_form"):
        email = st.text_input("Email Address")
        submit = st.form_submit_button("Request Password Reset", type="primary")
        
        if submit:
            if not email:
                st.error("Please enter your email address")
                return
                
            # Check rate limits
            rate_check, message = check_rate_limit(client_ip, 'RESET_REQUEST')
            if not rate_check:
                st.error(message)
                return
                
            try:
                user_found = False
                user_info = None
                user_type = None
                
                # Check business users first
                business_query = """
                SELECT 
                    bpu.PORTAL_USER_ID,
                    bpu.EMAIL,
                    e.FIRST_NAME,
                    e.LAST_NAME,
                    'BUSINESS' as USER_TYPE
                FROM OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS bpu
                LEFT JOIN OPERATIONAL.CARPET.EMPLOYEE e ON bpu.EMPLOYEE_ID = e.EMPLOYEE_ID
                WHERE bpu.EMAIL = ? AND bpu.IS_ACTIVE = TRUE
                """
                
                result = snowflake_conn.execute_query(business_query, [email.lower()])
                if result:
                    user_found = True
                    user_info = result[0]
                    user_type = 'BUSINESS'
                else:
                    # Check customer users
                    customer_query = """
                    SELECT 
                        cpu.PORTAL_USER_ID,
                        cpu.EMAIL,
                        c.FIRST_NAME,
                        c.LAST_NAME,
                        'CUSTOMER' as USER_TYPE
                    FROM OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS cpu
                    LEFT JOIN OPERATIONAL.CARPET.CUSTOMER c ON cpu.CUSTOMER_ID = c.CUSTOMER_ID
                    WHERE cpu.EMAIL = ? AND cpu.IS_ACTIVE = TRUE
                    """
                    
                    result = snowflake_conn.execute_query(customer_query, [email.lower()])
                    if result:
                        user_found = True
                        user_info = result[0]
                        user_type = 'CUSTOMER'
                
                # Always show success message for security (don't reveal if email exists)
                st.success(
                    "If an account exists with this email address, "
                    "you will receive password reset instructions shortly."
                )
                
                if user_found and user_info:
                    # Generate reset token
                    token = generate_reset_token()
                    expiry = datetime.now() + timedelta(hours=1)
                    
                    # Store token in appropriate table
                    if user_type == 'BUSINESS':
                        update_query = """
                        UPDATE OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS
                        SET PASSWORD_RESET_TOKEN = ?,
                            PASSWORD_RESET_EXPIRY = ?,
                            MODIFIED_AT = CURRENT_TIMESTAMP()
                        WHERE PORTAL_USER_ID = ?
                        """
                    else:  # CUSTOMER
                        update_query = """
                        UPDATE OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS
                        SET PASSWORD_RESET_TOKEN = ?,
                            PASSWORD_RESET_EXPIRY = ?,
                            MODIFIED_AT = CURRENT_TIMESTAMP()
                        WHERE PORTAL_USER_ID = ?
                        """
                    
                    snowflake_conn.execute_query(update_query, [
                        token,
                        expiry,
                        user_info['PORTAL_USER_ID']
                    ])
                    
                    # For development/testing, show the reset link
                    if st.session_state.get('debug_mode', True):
                        st.info(f"🔧 Debug Mode - Reset link available")
                        if st.button("🔗 Use Reset Link (Debug)", key="debug_reset"):
                            st.query_params['reset_token'] = token
                            st.rerun()
                    
                    # Log security event
                    log_security_event(
                        user_info['PORTAL_USER_ID'],
                        'RESET_REQUESTED',
                        client_ip,
                        user_agent,
                        f"Password reset requested for {email} ({user_type})"
                    )
                    
            except Exception as e:
                st.error("Error processing reset request")
                print(f"Reset request error: {str(e)}")

    # Back to login
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("← Back to Login", use_container_width=True):
            st.session_state.page = None  # Return to unified login
            st.rerun()
    with col2:
        if st.button("Register Business", use_container_width=True):
            st.session_state.page = 'business_register'
            st.rerun()
    with col3:
        if st.button("Customer Register", use_container_width=True):
            st.session_state.page = 'register'
            st.rerun()

def _show_reset_form(token: str, client_ip: str, user_agent: str):
    """Show password reset form with token validation"""
    st.markdown("Enter your new password below.")
    
    # Verify token in both business and customer tables
    try:
        user_info = None
        user_type = None
        
        # Check business users first
        business_query = """
        SELECT 
            bpu.PORTAL_USER_ID,
            bpu.EMAIL,
            bpu.PASSWORD_RESET_EXPIRY,
            e.FIRST_NAME,
            e.LAST_NAME,
            'BUSINESS' as USER_TYPE
        FROM OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS bpu
        LEFT JOIN OPERATIONAL.CARPET.EMPLOYEE e ON bpu.EMPLOYEE_ID = e.EMPLOYEE_ID
        WHERE bpu.PASSWORD_RESET_TOKEN = ? 
        AND bpu.IS_ACTIVE = TRUE
        AND bpu.PASSWORD_RESET_EXPIRY > CURRENT_TIMESTAMP()
        """
        
        result = snowflake_conn.execute_query(business_query, [token])
        if result:
            user_info = result[0]
            user_type = 'BUSINESS'
        else:
            # Check customer users
            customer_query = """
            SELECT 
                cpu.PORTAL_USER_ID,
                cpu.EMAIL,
                cpu.PASSWORD_RESET_EXPIRY,
                c.FIRST_NAME,
                c.LAST_NAME,
                'CUSTOMER' as USER_TYPE
            FROM OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS cpu
            LEFT JOIN OPERATIONAL.CARPET.CUSTOMER c ON cpu.CUSTOMER_ID = c.CUSTOMER_ID
            WHERE cpu.PASSWORD_RESET_TOKEN = ? 
            AND cpu.IS_ACTIVE = TRUE
            AND cpu.PASSWORD_RESET_EXPIRY > CURRENT_TIMESTAMP()
            """
            
            result = snowflake_conn.execute_query(customer_query, [token])
            if result:
                user_info = result[0]
                user_type = 'CUSTOMER'
        
        if not user_info:
            st.error("Invalid or expired reset token")
            if st.button("Request New Reset"):
                # Clear query params and return to request form
                st.query_params.clear()
                st.rerun()
            return
        
        with st.form("reset_password_form"):
            st.write(f"Resetting password for: **{user_info['EMAIL']}** ({user_type.title()} Account)")
            
            new_password = st.text_input(
                "New Password",
                type="password",
                help="Must be at least 8 characters with uppercase, lowercase, number, and special character"
            )
            confirm_password = st.text_input(
                "Confirm New Password",
                type="password"
            )
            
            submitted = st.form_submit_button("Reset Password", type="primary")
            
            if submitted:
                if not new_password or not confirm_password:
                    st.error("Please fill in both password fields")
                    return
                    
                if new_password != confirm_password:
                    st.error("Passwords do not match")
                    return
                    
                # Validate password strength
                password_errors = validate_password(new_password)
                if password_errors:
                    for error in password_errors:
                        st.error(error)
                    return
                    
                try:
                    # Update password and clear reset token in appropriate table
                    if user_type == 'BUSINESS':
                        update_query = """
                        UPDATE OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS
                        SET 
                            PASSWORD_HASH = ?,
                            PASSWORD_RESET_TOKEN = NULL,
                            PASSWORD_RESET_EXPIRY = NULL,
                            MODIFIED_AT = CURRENT_TIMESTAMP()
                        WHERE PORTAL_USER_ID = ?
                        """
                    else:  # CUSTOMER
                        update_query = """
                        UPDATE OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS
                        SET 
                            PASSWORD_HASH = ?,
                            PASSWORD_RESET_TOKEN = NULL,
                            PASSWORD_RESET_EXPIRY = NULL,
                            MODIFIED_AT = CURRENT_TIMESTAMP()
                        WHERE PORTAL_USER_ID = ?
                        """
                    
                    snowflake_conn.execute_query(update_query, [
                        hash_password(new_password),
                        user_info['PORTAL_USER_ID']
                    ])
                    
                    # Log security event
                    log_security_event(
                        user_info['PORTAL_USER_ID'],
                        'PASSWORD_RESET_SUCCESS',
                        client_ip,
                        user_agent,
                        f"Password reset completed for {user_info['EMAIL']} ({user_type})"
                    )
                    
                    st.success("✅ Password reset successfully!")
                    st.info("You can now log in with your new password.")
                    
                    # Show login button
                    if st.button("Go to Login", type="primary"):
                        st.query_params.clear()  # Clear reset token
                        st.session_state.page = None  # Return to unified login
                        st.rerun()
                        
                except Exception as e:
                    st.error("Error resetting password")
                    print(f"Password reset error: {str(e)}")
                    
    except Exception as e:
        st.error("Error validating reset token")
        print(f"Token validation error: {str(e)}")

if __name__ == "__main__":
    unified_reset_page()