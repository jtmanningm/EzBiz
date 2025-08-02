from passlib.hash import pbkdf2_sha256
import re
from datetime import datetime, timedelta
import uuid
from typing import Optional, Tuple, Dict, List
from database.connection import snowflake_conn

def validate_password(password: str) -> List[str]:
    """
    Validate password meets requirements:
    - At least 8 characters
    - Contains uppercase and lowercase
    - Contains at least one special character
    """
    errors = []
    
    if len(password) < 8:
        errors.append("Password must be at least 8 characters long")
    
    if not re.search(r'[A-Z]', password):
        errors.append("Password must contain at least one uppercase letter")
    
    if not re.search(r'[a-z]', password):
        errors.append("Password must contain at least one lowercase letter")
    
    if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
        errors.append("Password must contain at least one special character")
    
    return errors

def validate_email(email: str) -> bool:
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

def hash_password(password: str) -> str:
    """Hash password using PBKDF2-SHA256"""
    return pbkdf2_sha256.hash(password)

def verify_password(password: str, password_hash: str) -> bool:
    """Verify password against hash"""
    return pbkdf2_sha256.verify(password, password_hash)

def create_session(portal_user_id: int, ip_address: str, user_agent: str) -> Optional[str]:
    """Create new session for user"""
    session_id = str(uuid.uuid4())
    expires_at = datetime.now() + timedelta(hours=2)  # 2 hour session

    query = """
    INSERT INTO OPERATIONAL.CARPET.CUSTOMER_SESSIONS (
        SESSION_ID, PORTAL_USER_ID, IP_ADDRESS, USER_AGENT, 
        LOGIN_TIME, LAST_ACTIVITY, EXPIRES_AT, IS_ACTIVE
    ) 
    SELECT
        :1, :2, :3, :4,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 
        :5, TRUE
    """
    
    try:
        snowflake_conn.execute_query(
            query, 
            [session_id, portal_user_id, ip_address, user_agent, expires_at]
        )
        return session_id
    except Exception as e:
        print(f"Error creating session: {str(e)}")
        return None

def validate_session(session_id: str) -> Optional[Dict]:
    """Validate session and update last activity"""
    query = """
    SELECT 
        s.PORTAL_USER_ID,
        s.EXPIRES_AT,
        u.CUSTOMER_ID,
        u.EMAIL
    FROM OPERATIONAL.CARPET.CUSTOMER_SESSIONS s
    JOIN OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS u 
        ON s.PORTAL_USER_ID = u.PORTAL_USER_ID
    WHERE s.SESSION_ID = :1
    AND s.IS_ACTIVE = TRUE
    AND s.EXPIRES_AT > CURRENT_TIMESTAMP()
    """
    
    try:
        result = snowflake_conn.execute_query(query, [session_id])
        if result and len(result) > 0:
            # Convert Row to dict
            session_data = result[0].as_dict()
            
            # Update last activity
            update_query = """
            UPDATE CUSTOMER_SESSIONS
            SET LAST_ACTIVITY = CURRENT_TIMESTAMP()
            WHERE SESSION_ID = :1
            """
            snowflake_conn.execute_query(update_query, [session_id])
            
            return session_data
        return None
            
    except Exception as e:
        print(f"Error validating session: {str(e)}")
        return None

def log_security_event(
    portal_user_id: Optional[int],
    event_type: str,
    ip_address: str,
    user_agent: str,
    details: str
) -> bool:
    """Log security events for monitoring"""
    query = """
    INSERT INTO OPERATIONAL.CARPET.SESSION_LOG (
        PORTAL_USER_ID, EVENT_TYPE, IP_ADDRESS, 
        USER_AGENT, EVENT_DETAILS
    )
    SELECT :1, :2, :3, :4, :5
    """
    
    try:
        snowflake_conn.execute_query(
            query, 
            [portal_user_id, event_type, ip_address, user_agent, details]
        )
        return True
    except Exception as e:
        print(f"Error logging security event: {str(e)}")
        return False

def check_rate_limit(ip_address: str, action_type: str, portal_user_id: Optional[int] = None) -> Tuple[bool, str]:
    """
    Check rate limits:
    - Login attempts: 5 per hour per IP
    - Registration attempts: 3 per hour per IP
    - Booking creation: 3 per hour per customer
    - Service availability checks: 60 per hour per IP
    - Reset attempts: 3 per hour per IP
    """
    limits = {
        'LOGIN_ATTEMPT': 5,
        'REGISTRATION_ATTEMPT': 3,
        'BOOKING_ATTEMPT': 3,
        'AVAILABILITY_CHECK': 60,
        'RESET_REQUEST': 3
    }
    
    if action_type not in limits:
        print(f"Invalid action type: {action_type}")
        return False, "Invalid action type"
    
    # Get attempts in last hour
    query = """
    SELECT COUNT(*) as ATTEMPT_COUNT
    FROM OPERATIONAL.CARPET.RATE_LIMIT_LOG
    WHERE IP_ADDRESS = :1
    AND ACTION_TYPE = :2
    AND LAST_ATTEMPT > DATEADD(hour, -1, CURRENT_TIMESTAMP())
    """
    params = [ip_address, action_type]
    
    if portal_user_id:
        query += " AND PORTAL_USER_ID = :3"
        params.append(portal_user_id)
    
    try:
        result = snowflake_conn.execute_query(query, params)
        if result and len(result) > 0:
            current_attempts = result[0]['ATTEMPT_COUNT']
            
            if current_attempts >= limits[action_type]:
                return False, f"Too many attempts. Please try again in 1 hour."
            
            # Log attempt
            log_query = """
            INSERT INTO RATE_LIMIT_LOG (
                IP_ADDRESS, ACTION_TYPE, PORTAL_USER_ID
            )
            SELECT :1, :2, :3
            """
            snowflake_conn.execute_query(log_query, [
                ip_address, action_type, portal_user_id
            ])
            
            return True, "Rate limit check passed"
            
        return True, "First attempt"
    except Exception as e:
        print(f"Error checking rate limit: {str(e)}")
        return False, "Error checking rate limit"