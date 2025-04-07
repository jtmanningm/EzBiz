# models/portal/user.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any
from database.connection import snowflake_conn

@dataclass
class PortalUser:
    portal_user_id: Optional[int] = None
    customer_id: int = 0
    email: str = ""
    is_active: bool = True
    last_login_date: Optional[datetime] = None
    failed_login_attempts: int = 0
    account_locked: bool = False
    account_locked_until: Optional[datetime] = None
    email_verified: bool = False
    created_at: Optional[datetime] = None
    modified_at: Optional[datetime] = None
    
    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'PortalUser':
        """Create PortalUser instance from database row"""
        return cls(
            portal_user_id=row.get('PORTAL_USER_ID'),
            customer_id=row.get('CUSTOMER_ID'),
            email=row.get('EMAIL', ''),
            is_active=row.get('IS_ACTIVE', True),
            last_login_date=row.get('LAST_LOGIN_DATE'),
            failed_login_attempts=row.get('FAILED_LOGIN_ATTEMPTS', 0),
            account_locked=row.get('ACCOUNT_LOCKED', False),
            account_locked_until=row.get('ACCOUNT_LOCKED_UNTIL'),
            email_verified=row.get('EMAIL_VERIFIED', False),
            created_at=row.get('CREATED_AT'),
            modified_at=row.get('MODIFIED_AT')
        )

def get_portal_user(user_id: int) -> Optional[PortalUser]:
    """Fetch portal user by ID"""
    query = """
    SELECT 
        PORTAL_USER_ID,
        CUSTOMER_ID,
        EMAIL,
        IS_ACTIVE,
        LAST_LOGIN_DATE,
        FAILED_LOGIN_ATTEMPTS,
        ACCOUNT_LOCKED,
        ACCOUNT_LOCKED_UNTIL,
        EMAIL_VERIFIED,
        CREATED_AT,
        MODIFIED_AT
    FROM CUSTOMER_PORTAL_USERS
    WHERE PORTAL_USER_ID = ?
    """
    
    try:
        result = snowflake_conn.execute_query(query, [user_id])
        if result and len(result) > 0:
            return PortalUser.from_db_row(result[0])
        return None
    except Exception as e:
        print(f"Error fetching portal user: {str(e)}")
        return None

def get_portal_user_by_email(email: str) -> Optional[PortalUser]:
    """Fetch portal user by email"""
    query = """
    SELECT 
        PORTAL_USER_ID,
        CUSTOMER_ID,
        EMAIL,
        IS_ACTIVE,
        LAST_LOGIN_DATE,
        FAILED_LOGIN_ATTEMPTS,
        ACCOUNT_LOCKED,
        ACCOUNT_LOCKED_UNTIL,
        EMAIL_VERIFIED,
        CREATED_AT,
        MODIFIED_AT
    FROM CUSTOMER_PORTAL_USERS
    WHERE EMAIL = ?
    """
    
    try:
        result = snowflake_conn.execute_query(query, [email])
        if result and len(result) > 0:
            return PortalUser.from_db_row(result[0])
        return None
    except Exception as e:
        print(f"Error fetching portal user by email: {str(e)}")
        return None

def update_login_attempt(user_id: int, success: bool) -> bool:
    """Update login attempt status"""
    try:
        if success:
            # Reset failed attempts on successful login
            query = """
            UPDATE CUSTOMER_PORTAL_USERS
            SET 
                FAILED_LOGIN_ATTEMPTS = 0,
                ACCOUNT_LOCKED = FALSE,
                ACCOUNT_LOCKED_UNTIL = NULL,
                LAST_LOGIN_DATE = CURRENT_TIMESTAMP(),
                MODIFIED_AT = CURRENT_TIMESTAMP()
            WHERE PORTAL_USER_ID = ?
            """
        else:
            # Increment failed attempts and possibly lock account
            query = """
            UPDATE CUSTOMER_PORTAL_USERS
            SET 
                FAILED_LOGIN_ATTEMPTS = FAILED_LOGIN_ATTEMPTS + 1,
                ACCOUNT_LOCKED = CASE 
                    WHEN FAILED_LOGIN_ATTEMPTS >= 4 THEN TRUE 
                    ELSE FALSE 
                END,
                ACCOUNT_LOCKED_UNTIL = CASE 
                    WHEN FAILED_LOGIN_ATTEMPTS >= 4 
                    THEN DATEADD(hour, 1, CURRENT_TIMESTAMP())
                    ELSE NULL 
                END,
                MODIFIED_AT = CURRENT_TIMESTAMP()
            WHERE PORTAL_USER_ID = ?
            """
            
        snowflake_conn.execute_query(query, [user_id])
        return True
    except Exception as e:
        print(f"Error updating login attempt: {str(e)}")
        return False

def update_portal_user(user: PortalUser) -> bool:
    """Update portal user details"""
    try:
        query = """
        UPDATE CUSTOMER_PORTAL_USERS
        SET 
            EMAIL = ?,
            IS_ACTIVE = ?,
            EMAIL_VERIFIED = ?,
            MODIFIED_AT = CURRENT_TIMESTAMP()
        WHERE PORTAL_USER_ID = ?
        """
        
        snowflake_conn.execute_query(query, [
            user.email,
            user.is_active,
            user.email_verified,
            user.portal_user_id
        ])
        return True
    except Exception as e:
        print(f"Error updating portal user: {str(e)}")
        return False

def create_portal_user(
    customer_id: int,
    email: str,
    password_hash: str
) -> Optional[int]:
    """Create new portal user"""
    try:
        query = """
        INSERT INTO CUSTOMER_PORTAL_USERS (
            CUSTOMER_ID,
            EMAIL,
            PASSWORD_HASH,
            IS_ACTIVE,
            EMAIL_VERIFIED,
            CREATED_AT,
            MODIFIED_AT
        ) VALUES (?, ?, ?, TRUE, FALSE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        RETURNING PORTAL_USER_ID
        """
        
        result = snowflake_conn.execute_query(query, [
            customer_id,
            email,
            password_hash
        ])
        
        if result and len(result) > 0:
            return result[0]['PORTAL_USER_ID']
        return None
    except Exception as e:
        print(f"Error creating portal user: {str(e)}")
        return None