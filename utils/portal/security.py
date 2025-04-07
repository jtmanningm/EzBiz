# utils/portal/security.py
import secrets
from datetime import datetime, timedelta
from typing import Optional, Tuple, Dict
import streamlit as st
from database.connection import snowflake_conn

def verify_action_token(token: str, token_type: str) -> Tuple[bool, Optional[int], str]:
    """
    Verify a token for actions like email verification or password reset.
    Returns (is_valid, user_id, message)
    """
    try:
        query = """
        SELECT 
            t.TOKEN_ID,
            t.PORTAL_USER_ID,
            t.EXPIRES_AT,
            t.IS_USED,
            u.EMAIL_VERIFIED
        FROM VERIFICATION_TOKENS t
        JOIN CUSTOMER_PORTAL_USERS u 
            ON t.PORTAL_USER_ID = u.PORTAL_USER_ID
        WHERE t.TOKEN_ID = ?
        AND t.TOKEN_TYPE = ?
        """
        
        result = snowflake_conn.execute_query(query, [token, token_type])
        if not result:
            return False, None, "Invalid token"
            
        token_data = result[0]
        
        # Check if token is expired
        if token_data['EXPIRES_AT'] < datetime.now():
            return False, None, "Token has expired"
            
        # Check if token was already used
        if token_data['IS_USED']:
            if token_type == 'EMAIL_VERIFICATION' and token_data['EMAIL_VERIFIED']:
                return False, None, "Email already verified"
            return False, None, "Token has already been used"
            
        # Mark token as used
        update_query = """
        UPDATE VERIFICATION_TOKENS
        SET 
            IS_USED = TRUE,
            USED_AT = CURRENT_TIMESTAMP()
        WHERE TOKEN_ID = ?
        """
        
        snowflake_conn.execute_query(update_query, [token])
        
        return True, int(token_data['PORTAL_USER_ID']), "Token valid"
        
    except Exception as e:
        print(f"Error verifying token: {str(e)}")
        return False, None, f"Error verifying token: {str(e)}"

def check_rate_limit(
    ip_address: str,
    action_type: str,
    user_id: Optional[int] = None
) -> Tuple[bool, str]:
    """
    Check rate limits for various actions
    Returns (allowed, message)
    """
    limits = {
        'LOGIN_ATTEMPT': {'count': 5, 'window': 60},  # 5 per hour
        'RESET_REQUEST': {'count': 3, 'window': 60},  # 3 per hour
        'REGISTRATION': {'count': 3, 'window': 60},   # 3 per hour
        'BOOKING_ATTEMPT': {'count': 10, 'window': 60} # 10 per hour
    }
    
    if action_type not in limits:
        return False, "Invalid action type"
        
    window_minutes = limits[action_type]['window']
    max_attempts = limits[action_type]['count']
    
    try:
        # Get attempts in time window
        query = """
        SELECT COUNT(*) as attempt_count
        FROM RATE_LIMIT_LOG
        WHERE IP_ADDRESS = ?
        AND ACTION_TYPE = ?
        AND LAST_ATTEMPT > DATEADD(minute, -?, CURRENT_TIMESTAMP())
        """
        params = [ip_address, action_type, window_minutes]
        
        if user_id:
            query += " AND PORTAL_USER_ID = ?"
            params.append(user_id)
        
        result = snowflake_conn.execute_query(query, params)
        if not result:
            return True, "First attempt"
            
        current_attempts = result[0]['ATTEMPT_COUNT']
        
        if current_attempts >= max_attempts:
            return False, f"Rate limit exceeded for {action_type}"
            
        # Log attempt
        log_query = """
        INSERT INTO RATE_LIMIT_LOG (
            IP_ADDRESS, ACTION_TYPE, PORTAL_USER_ID,
            ATTEMPT_COUNT
        ) VALUES (?, ?, ?, ?)
        """
        
        snowflake_conn.execute_query(log_query, [
            ip_address,
            action_type,
            user_id,
            current_attempts + 1
        ])
        
        return True, "Rate limit check passed"
        
    except Exception as e:
        print(f"Error checking rate limit: {str(e)}")
        return False, "Error checking rate limit"

def check_suspicious_activity(
    ip_address: str,
    user_agent: str,
    user_id: Optional[int] = None
) -> Tuple[bool, str]:
    """
    Check for suspicious activity patterns
    Returns (is_suspicious, reason)
    """
    try:
        checks = []
        
        # Check for rapid requests from same IP
        rapid_query = """
        SELECT COUNT(*) as request_count
        FROM SESSION_LOG
        WHERE IP_ADDRESS = ?
        AND EVENT_TIME > DATEADD(second, -5, CURRENT_TIMESTAMP())
        """
        
        result = snowflake_conn.execute_query(rapid_query, [ip_address])
        if result and result[0]['REQUEST_COUNT'] > 10:
            checks.append("Rapid requests detected")
            
        # Check for multiple failed attempts
        if user_id:
            fails_query = """
            SELECT COUNT(*) as fail_count
            FROM SESSION_LOG
            WHERE PORTAL_USER_ID = ?
            AND EVENT_TYPE IN ('LOGIN_FAILED', 'VERIFY_FAILED')
            AND EVENT_TIME > DATEADD(minute, -30, CURRENT_TIMESTAMP())
            """
            
            result = snowflake_conn.execute_query(fails_query, [user_id])
            if result and result[0]['FAIL_COUNT'] > 5:
                checks.append("Multiple failed attempts")
                
        # Return results
        is_suspicious = len(checks) > 0
        reason = " | ".join(checks) if checks else "No suspicious activity"
        
        return is_suspicious, reason
        
    except Exception as e:
        print(f"Error checking suspicious activity: {str(e)}")
        return True, "Error performing security check"

# Create required tables
"""
CREATE TABLE IF NOT EXISTS VERIFICATION_TOKENS (
    TOKEN_ID VARCHAR(64) PRIMARY KEY,
    PORTAL_USER_ID NUMBER NOT NULL,
    TOKEN_TYPE VARCHAR(20) NOT NULL,
    EXPIRES_AT TIMESTAMP_NTZ NOT NULL,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    USED_AT TIMESTAMP_NTZ,
    IS_USED BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (PORTAL_USER_ID) REFERENCES CUSTOMER_PORTAL_USERS(PORTAL_USER_ID)
);

CREATE TABLE IF NOT EXISTS RATE_LIMIT_LOG (
    LOG_ID NUMBER IDENTITY(1,1),
    IP_ADDRESS VARCHAR(45) NOT NULL,
    ACTION_TYPE VARCHAR(50) NOT NULL,
    PORTAL_USER_ID NUMBER,
    ATTEMPT_COUNT NUMBER DEFAULT 1,
    FIRST_ATTEMPT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LAST_ATTEMPT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (PORTAL_USER_ID) REFERENCES CUSTOMER_PORTAL_USERS(PORTAL_USER_ID)
);
"""