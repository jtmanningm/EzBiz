# pages/portal/auth/verify.py
import streamlit as st
from utils.portal.security import verify_action_token
from utils.auth.auth_utils import log_security_event
from database.connection import snowflake_conn

def verify_email_page():
    """Handle email verification process"""
    st.title("Email Verification")
    
    # Get token from URL parameters
    token = st.query_params.get("token")
    
    if not token:
        st.error("No verification token provided")
        return
        
    # Get client info for logging
    client_ip = st.request.headers.get('X-Forwarded-For', 'unknown')
    user_agent = st.request.headers.get('User-Agent', 'unknown')
    
    # Verify token
    is_valid, user_id, message = verify_action_token(
        token, 
        'EMAIL_VERIFICATION'
    )
    
    if not is_valid:
        st.error(message)
        log_security_event(
            None,
            'VERIFY_FAILED',
            client_ip,
            user_agent,
            f"Email verification failed: {message}"
        )
        
        if "expired" in message.lower():
            if st.button("Request New Verification Email"):
                st.session_state.page = 'resend_verification'
                st.rerun()
        return
        
    try:
        # Update user verification status
        update_query = """
        UPDATE OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS
        SET 
            EMAIL_VERIFIED = TRUE,
            MODIFIED_AT = CURRENT_TIMESTAMP()
        WHERE PORTAL_USER_ID = ?
        """
        
        snowflake_conn.execute_query(update_query, [user_id])
        
        # Log successful verification
        log_security_event(
            user_id,
            'VERIFY_SUCCESS',
            client_ip,
            user_agent,
            "Email verified successfully"
        )
        
        st.success("Email verified successfully!")
        st.info("You can now log in to your account")
        
        if st.button("Go to Login"):
            st.session_state.page = 'login'
            st.rerun()
            
    except Exception as e:
        st.error("Error verifying email")
        print(f"Verification error: {str(e)}")
        
        log_security_event(
            user_id,
            'VERIFY_ERROR',
            client_ip,
            user_agent,
            f"Error during verification: {str(e)}"
        )

if __name__ == "__main__":
    verify_email_page()