import secrets
import re
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple
from passlib.hash import pbkdf2_sha256
from database.connection import snowflake_conn

def validate_password(password: str) -> Tuple[bool, str]:
    """
    Validate password meets requirements:
    - At least 8 characters
    - Contains uppercase and lowercase
    - Contains at least one special character
    """
    if len(password) < 8:
        return False, "Password must be at least 8 characters long"
    
    if not re.search(r'[A-Z]', password):
        return False, "Password must contain at least one uppercase letter"
    
    if not re.search(r'[a-z]', password):
        return False, "Password must contain at least one lowercase letter"
    
    if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
        return False, "Password must contain at least one special character"
    
    return True, "Password is valid"

def hash_password(password: str) -> str:
    """Hash password using PBKDF2-SHA256"""
    return pbkdf2_sha256.hash(password)

def verify_password(password: str, password_hash: str) -> bool:
    """Verify password against hash"""
    return pbkdf2_sha256.verify(password, password_hash)

def check_business_rate_limit(ip_address: str, action_type: str) -> Tuple[bool, str]:
    """Check rate limits for business actions"""
    query = """
    SELECT COUNT(*) as attempt_count
    FROM OPERATIONAL.CARPET.RATE_LIMIT_LOG
    WHERE IP_ADDRESS = :1
    AND ACTION_TYPE = :2
    AND LAST_ATTEMPT > DATEADD(hour, -1, CURRENT_TIMESTAMP())
    """
    try:
        result = snowflake_conn.execute_query(query, [ip_address, action_type])
        if result[0]['ATTEMPT_COUNT'] >= 5:
            return False, "Too many attempts. Please try again later."
        
        # Log attempt
        log_query = """
        INSERT INTO OPERATIONAL.CARPET.RATE_LIMIT_LOG (
            IP_ADDRESS, ACTION_TYPE, LAST_ATTEMPT
        ) VALUES (:1, :2, CURRENT_TIMESTAMP())
        """
        snowflake_conn.execute_query(log_query, [ip_address, action_type])
        return True, "OK"
    except Exception as e:
        print(f"Rate limit error: {str(e)}")
        return False, "Rate limit check failed"

def log_business_event(portal_user_id: Optional[int], event_type: str, details: str, 
                      ip_address: str = 'unknown', user_agent: str = 'unknown') -> None:
    """Log business security events"""
    query = """
    INSERT INTO OPERATIONAL.CARPET.SESSION_LOG (
        PORTAL_USER_ID, EVENT_TYPE, IP_ADDRESS, USER_AGENT, EVENT_DETAILS
    ) VALUES (:1, :2, :3, :4, :5)
    """
    try:
        snowflake_conn.execute_query(query, [
            portal_user_id, event_type, ip_address, user_agent, details
        ])
    except Exception as e:
        print(f"Log error: {str(e)}")

def create_business_session(portal_user_id: int, ip_address: str, user_agent: str) -> Optional[str]:
    """Create new business session"""
    session_id = secrets.token_urlsafe(32)
    expires_at = datetime.now() + timedelta(hours=12)  # Longer session for business

    query = """
    INSERT INTO OPERATIONAL.CARPET.BUSINESS_SESSIONS (
        SESSION_ID, PORTAL_USER_ID, IP_ADDRESS, USER_AGENT,
        LAST_ACTIVITY, EXPIRES_AT
    ) VALUES (:1, :2, :3, :4, CURRENT_TIMESTAMP(), :5)
    """
    
    try:
        snowflake_conn.execute_query(query, [
            session_id, portal_user_id, ip_address, user_agent, expires_at
        ])
        return session_id
    except Exception as e:
        print(f"Error creating business session: {str(e)}")
        return None

def verify_business_session(session_id: str) -> Optional[Dict]:
    """Validate business session and update last activity"""
    query = """
    SELECT 
        s.PORTAL_USER_ID,
        s.EXPIRES_AT,
        u.EMPLOYEE_ID,
        u.IS_ADMIN,
        u.EMAIL
    FROM OPERATIONAL.CARPET.BUSINESS_SESSIONS s
    JOIN OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS u ON s.PORTAL_USER_ID = u.PORTAL_USER_ID
    WHERE s.SESSION_ID = :1
    AND s.IS_ACTIVE = TRUE
    AND s.EXPIRES_AT > CURRENT_TIMESTAMP()
    """
    
    try:
        result = snowflake_conn.execute_query(query, [session_id])
        if result and len(result) > 0:
            session_data = result[0]
            
            # Update last activity
            update_query = """
            UPDATE OPERATIONAL.CARPET.BUSINESS_SESSIONS 
            SET LAST_ACTIVITY = CURRENT_TIMESTAMP()
            WHERE SESSION_ID = :1
            """
            snowflake_conn.execute_query(update_query, [session_id])
            
            return session_data
        return None
            
    except Exception as e:
        print(f"Error validating business session: {str(e)}")
        return None

def create_business_user(employee_id: int, email: str, password: str, is_admin: bool = False) -> Optional[int]:
    """Create new business portal user"""
    try:
        # Validate password
        valid, message = validate_password(password)
        if not valid:
            raise ValueError(message)

        # Create user
        query = """
        INSERT INTO OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS (
            EMPLOYEE_ID, EMAIL, PASSWORD_HASH, IS_ADMIN,
            IS_ACTIVE, EMAIL_VERIFIED
        ) VALUES (:1, :2, :3, :4, TRUE, TRUE)
        RETURNING PORTAL_USER_ID
        """
        
        result = snowflake_conn.execute_query(query, [
            employee_id,
            email.lower(),
            hash_password(password),
            is_admin
        ])
        
        return result[0]['PORTAL_USER_ID'] if result else None

    except Exception as e:
        print(f"Error creating business user: {str(e)}")
        return None

def business_login(email: str, password: str, ip_address: str, user_agent: str) -> Tuple[bool, str, Optional[str]]:
    """
    Business portal login
    Returns: (success, message, session_id)
    """
    try:
        # Rate limit check
        rate_check, message = check_business_rate_limit(ip_address, 'BUSINESS_LOGIN')
        if not rate_check:
            return False, message, None

        # Get user
        query = """
        SELECT 
            PORTAL_USER_ID,
            EMPLOYEE_ID,
            PASSWORD_HASH,
            IS_ACTIVE,
            FAILED_LOGIN_ATTEMPTS,
            ACCOUNT_LOCKED,
            ACCOUNT_LOCKED_UNTIL
        FROM OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS
        WHERE EMAIL = :1
        """
        
        result = snowflake_conn.execute_query(query, [email.lower()])
        if not result:
            return False, "Invalid email or password", None

        user = result[0]

        # Check account status
        if not user['IS_ACTIVE']:
            return False, "Account is inactive", None
            
        if user['ACCOUNT_LOCKED']:
            if user['ACCOUNT_LOCKED_UNTIL'] > datetime.now():
                return False, "Account is temporarily locked", None

        # Verify password
        if verify_password(password, user['PASSWORD_HASH']):
            
            # Create session
            session_id = create_business_session(
                user['PORTAL_USER_ID'],
                ip_address,
                user_agent
            )
            
            if session_id:
                # Reset failed attempts
                snowflake_conn.execute_query(
                    """
                    UPDATE OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS
                    SET FAILED_LOGIN_ATTEMPTS = 0,
                        LAST_LOGIN_DATE = CURRENT_TIMESTAMP()
                    WHERE PORTAL_USER_ID = :1
                    """,
                    [user['PORTAL_USER_ID']]
                )

                log_business_event(
                    user['PORTAL_USER_ID'],
                    'BUSINESS_LOGIN_SUCCESS',
                    "Business portal login successful",
                    ip_address,
                    user_agent
                )

                return True, "Login successful", session_id
            
            return False, "Error creating session", None
        else:
            # Increment failed attempts
            failed_attempts = user['FAILED_LOGIN_ATTEMPTS'] + 1
            lock_account = failed_attempts >= 5
            locked_until = datetime.now() + timedelta(minutes=30) if lock_account else None

            snowflake_conn.execute_query(
                """
                UPDATE OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS
                SET FAILED_LOGIN_ATTEMPTS = :1,
                    ACCOUNT_LOCKED = :2,
                    ACCOUNT_LOCKED_UNTIL = :3
                WHERE PORTAL_USER_ID = :4
                """,
                [failed_attempts, lock_account, locked_until, user['PORTAL_USER_ID']]
            )

            log_business_event(
                user['PORTAL_USER_ID'],
                'BUSINESS_LOGIN_FAILED',
                f"Failed login attempt ({failed_attempts})",
                ip_address,
                user_agent
            )

            if lock_account:
                return False, "Too many failed attempts. Account locked.", None
            return False, "Invalid email or password", None

    except Exception as e:
        print(f"Login error: {str(e)}")
        return False, "Error during login", None