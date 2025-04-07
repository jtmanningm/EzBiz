# pages/portal/auth/reset.py
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
from utils.portal.verification import generate_verification_token, verify_token

def get_client_info() -> Tuple[str, str]:
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

def request_reset_page():
    """Password reset request page"""
    st.title("Reset Password")
    
    # Get client info for rate limiting
    client_ip, user_agent = get_client_info()
    
    with st.form("reset_request_form"):
        email = st.text_input("Email Address")
        submit = st.form_submit_button("Request Reset")
        
        if submit:
            if not email:
                st.error("Please enter your email address")
                return
                
            # Check rate limits
            rate_check, message = check_rate_limit(
                client_ip, 
                'RESET_REQUEST'
            )
            if not rate_check:
                st.error(message)
                return
                
            try:
                # Find user
                query = """
                SELECT 
                    u.PORTAL_USER_ID,
                    u.CUSTOMER_ID,
                    c.FIRST_NAME
                FROM CUSTOMER_PORTAL_USERS u
                JOIN CUSTOMER c 
                    ON u.CUSTOMER_ID = c.CUSTOMER_ID
                WHERE u.EMAIL = :1
                AND u.IS_ACTIVE = TRUE
                """
                
                result = snowflake_conn.execute_query(query, [email])
                if not result:
                    # Don't reveal if email exists
                    st.success(
                        "If an account exists with this email, "
                        "you will receive reset instructions."
                    )
                    return
                    
                user = result[0]
                
                # Generate reset token
                token = generate_verification_token(
                    user['PORTAL_USER_ID'],
                    'PASSWORD_RESET'
                )
                
                if not token:
                    st.error("Error generating reset token")
                    return
                    
                # Store token in user record
                update_query = """
                UPDATE CUSTOMER_PORTAL_USERS
                SET 
                    PASSWORD_RESET_TOKEN = :1,
                    PASSWORD_RESET_EXPIRY = DATEADD(hour, 1, CURRENT_TIMESTAMP()),
                    MODIFIED_AT = CURRENT_TIMESTAMP()
                WHERE PORTAL_USER_ID = :2
                """
                
                snowflake_conn.execute_query(update_query, [
                    token,
                    user['PORTAL_USER_ID']
                ])
                
                # Send reset email
                reset_url = f"{st.secrets.BASE_URL}/reset?token={token}"
                
                template_query = """
                SELECT TEMPLATE_CONTENT
                FROM MESSAGE_TEMPLATES
                WHERE TEMPLATE_TYPE = 'PASSWORD_RESET'
                AND IS_ACTIVE = TRUE
                LIMIT 1
                """
                
                template = snowflake_conn.execute_query(template_query)
                if not template:
                    st.error("Error sending reset email")
                    return
                    
                email_content = template[0]['TEMPLATE_CONTENT'].replace(
                    '{RESET_URL}', reset_url
                ).replace(
                    '{FIRST_NAME}', user['FIRST_NAME']
                )
                
                # Get business info for email
                from pages.settings.business import fetch_business_info
                business_info = fetch_business_info()
                
                # Send email using your email utility
                from utils.email import send_email
                if send_email(
                    to_email=email,
                    subject="Password Reset Request",
                    content=email_content,
                    business_info=business_info
                ):
                    st.success(
                        "Password reset instructions have been sent to your email."
                    )
                    
                    # Log security event
                    log_security_event(
                        user['PORTAL_USER_ID'],
                        'RESET_REQUESTED',
                        client_ip,
                        user_agent,
                        f"Password reset requested for {email}"
                    )
                else:
                    st.error("Error sending reset email")
                    
            except Exception as e:
                st.error("Error processing reset request")
                print(f"Reset request error: {str(e)}")

    # Add back to login option
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("Remember your password?")
    with col2:
        if st.button("Back to Login"):
            st.session_state.portal_mode = 'customer'
            st.session_state.page = 'login'
            st.rerun()

def reset_password_page():
    """Password reset page with token"""
    st.title("Reset Your Password")
    
    # Get client info
    client_ip, user_agent = get_client_info()
    
    # Get token from URL parameters
    token = st.query_params.get("token")
    
    if not token:
        st.error("Invalid or missing reset token")
        # Add button to request new reset
        if st.button("Request Password Reset"):
            st.session_state.portal_mode = 'customer'
            st.session_state.page = 'reset'
            st.rerun()
        return
        
    # Verify token
    is_valid, portal_user_id, message = verify_token(token, 'PASSWORD_RESET')
    
    if not is_valid:
        st.error(message)
        if "expired" in message.lower():
            if st.button("Request New Reset Link"):
                st.session_state.portal_mode = 'customer'
                st.session_state.page = 'reset'
                st.rerun()
        return
        
    with st.form("reset_password_form"):
        new_password = st.text_input(
            "New Password",
            type="password",
            help="Must be at least 8 characters with uppercase, lowercase, and special characters"
        )
        confirm_password = st.text_input(
            "Confirm New Password",
            type="password"
        )
        
        submitted = st.form_submit_button("Reset Password")
        
        if submitted:
            if new_password != confirm_password:
                st.error("Passwords do not match")
                return
                
            # Validate password strength
            valid, message = validate_password(new_password)
            if not valid:
                st.error(message)
                return
                
            try:
                # Update password
                update_query = """
                UPDATE CUSTOMER_PORTAL_USERS
                SET 
                    PASSWORD_HASH = :1,
                    PASSWORD_RESET_TOKEN = NULL,
                    PASSWORD_RESET_EXPIRY = NULL,
                    MODIFIED_AT = CURRENT_TIMESTAMP()
                WHERE PORTAL_USER_ID = :2
                """
                
                snowflake_conn.execute_query(update_query, [
                    hash_password(new_password),
                    portal_user_id
                ])
                
                # Log security event
                log_security_event(
                    portal_user_id,
                    'PASSWORD_RESET',
                    client_ip,
                    user_agent,
                    "Password reset successful"
                )
                
                st.success("Password reset successfully!")
                
                # Clear any existing sessions for this user
                clear_query = """
                UPDATE CUSTOMER_SESSIONS
                SET 
                    IS_ACTIVE = FALSE,
                    MODIFIED_AT = CURRENT_TIMESTAMP()
                WHERE PORTAL_USER_ID = :1
                """
                snowflake_conn.execute_query(clear_query, [portal_user_id])
                
                # Show login button after successful reset
                _, right_col = st.columns([2, 1])
                with right_col:
                    if st.button("Go to Login", type="primary"):
                        st.session_state.portal_mode = 'customer'
                        st.session_state.page = 'login'
                        st.rerun()
                    
            except Exception as e:
                st.error("Error resetting password")
                print(f"Password reset error: {str(e)}")

def reset_password_handler():
    """Main handler for password reset pages"""
    # Check if we have a reset token
    token = st.query_params.get("token")
    
    if token:
        reset_password_page()
    else:
        request_reset_page()

if __name__ == "__main__":
    reset_password_handler()