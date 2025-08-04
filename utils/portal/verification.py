# utils/portal/verification.py
import secrets
from datetime import datetime, timedelta
from typing import Optional, Tuple
import streamlit as st
from database.connection import snowflake_conn

def generate_verification_token(portal_user_id: int, token_type: str) -> Optional[str]:
    """Generate a secure verification token"""
    try:
        # Generate token
        token = secrets.token_urlsafe(32)
        expires_at = datetime.now() + timedelta(hours=24)
        
        # Save token
        query = """
        INSERT INTO OPERATIONAL.CARPET.VERIFICATION_TOKENS (
            TOKEN_ID, PORTAL_USER_ID, TOKEN_TYPE,
            EXPIRES_AT, CREATED_AT
        ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP())
        """
        
        snowflake_conn.execute_query(query, [
            token,
            portal_user_id,
            token_type,
            expires_at
        ])
        
        return token
    except Exception as e:
        print(f"Error generating token: {str(e)}")
        return None

def verify_token(token: str, token_type: str) -> Tuple[bool, Optional[int], str]:
    """Verify a token and return (is_valid, portal_user_id, message)"""
    try:
        # Check token
        query = """
        SELECT 
            t.PORTAL_USER_ID,
            t.EXPIRES_AT,
            t.IS_USED,
            u.EMAIL_VERIFIED
        FROM OPERATIONAL.CARPET.VERIFICATION_TOKENS t
        JOIN OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS u 
            ON t.PORTAL_USER_ID = u.PORTAL_USER_ID
        WHERE t.TOKEN_ID = ?
        AND t.TOKEN_TYPE = ?
        """
        
        result = snowflake_conn.execute_query(query, [token, token_type])
        if not result:
            return False, None, "Invalid verification token"
            
        token_data = result[0]
        
        # Check if token is expired
        if token_data['EXPIRES_AT'] < datetime.now():
            return False, None, "Verification token has expired"
            
        # Check if token was already used
        if token_data['IS_USED']:
            if token_type == 'EMAIL_VERIFICATION' and token_data['EMAIL_VERIFIED']:
                return False, None, "Email is already verified"
            return False, None, "Token has already been used"
            
        return True, int(token_data['PORTAL_USER_ID']), "Token valid"
        
    except Exception as e:
        print(f"Error verifying token: {str(e)}")
        return False, None, f"Error verifying token: {str(e)}"

def mark_token_used(token: str) -> bool:
    """Mark a token as used"""
    try:
        query = """
        UPDATE OPERATIONAL.CARPET.VERIFICATION_TOKENS
        SET 
            IS_USED = TRUE,
            USED_AT = CURRENT_TIMESTAMP()
        WHERE TOKEN_ID = ?
        """
        
        snowflake_conn.execute_query(query, [token])
        return True
    except Exception as e:
        print(f"Error marking token used: {str(e)}")
        return False

def mark_email_verified(portal_user_id: int) -> bool:
    """Mark user's email as verified"""
    try:
        query = """
        UPDATE OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS
        SET 
            EMAIL_VERIFIED = TRUE,
            MODIFIED_AT = CURRENT_TIMESTAMP()
        WHERE PORTAL_USER_ID = ?
        """
        
        snowflake_conn.execute_query(query, [portal_user_id])
        return True
    except Exception as e:
        print(f"Error marking email verified: {str(e)}")
        return False

def send_verification_email(email: str, portal_user_id: int) -> bool:
    """Send verification email to user"""
    try:
        # Generate verification token
        token = generate_verification_token(portal_user_id, 'EMAIL_VERIFICATION')
        if not token:
            return False
            
        # Get verification URL
        verify_url = f"{st.secrets.BASE_URL}/verify?token={token}"
        
        # Get email template
        template_query = """
        SELECT TEMPLATE_CONTENT
        FROM MESSAGE_TEMPLATES
        WHERE TEMPLATE_TYPE = 'EMAIL_VERIFICATION'
        AND IS_ACTIVE = TRUE
        LIMIT 1
        """
        
        result = snowflake_conn.execute_query(template_query)
        if not result:
            print("No email template found")
            return False
            
        template = result[0]['TEMPLATE_CONTENT']
        
        # Replace placeholders
        email_content = template.replace('{VERIFY_URL}', verify_url)
        
        # Send email using email utility
        from utils.email import send_email
        from pages.settings.business import fetch_business_info
        
        business_info = fetch_business_info()
        return send_email(
            to_email=email,
            subject="Verify Your Email",
            content=email_content,
            business_info=business_info
        )
        
    except Exception as e:
        print(f"Error sending verification email: {str(e)}")
        return False