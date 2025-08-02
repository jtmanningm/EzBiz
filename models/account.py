# models/account.py
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import pandas as pd
import streamlit as st
from datetime import datetime
from database.connection import SnowflakeConnection

@dataclass
class AccountModel:
    account_id: Optional[int] = None
    account_name: str = ""
    account_type: str = ""
    account_description: Optional[str] = None
    contact_person: Optional[str] = None
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None
    billing_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[int] = None
    billing_date: Optional[datetime] = None
    active_flag: bool = True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "account_name": self.account_name,
            "account_type": self.account_type,
            "account_description": self.account_description,
            "contact_person": self.contact_person,
            "contact_email": self.contact_email,
            "contact_phone": self.contact_phone,
            "billing_address": self.billing_address,
            "city": self.city,
            "state": self.state,
            "zip_code": self.zip_code,
            "billing_date": self.billing_date,
            "active_flag": self.active_flag
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AccountModel':
        """Convert dictionary or Snowflake Row to AccountModel."""
        return cls(
            account_id=data.get('ACCOUNT_ID'),
            account_name=data.get('ACCOUNT_NAME', ''),
            account_type=data.get('ACCOUNT_TYPE', ''),
            account_description=data.get('ACCOUNT_DESCRIPTION'),
            contact_person=data.get('CONTACT_PERSON'),
            contact_email=data.get('CONTACT_EMAIL'),
            contact_phone=data.get('CONTACT_PHONE'),
            billing_address=data.get('BILLING_ADDRESS'),
            city=data.get('CITY'),
            state=data.get('STATE'),
            zip_code=data.get('ZIP_CODE'),
            billing_date=data.get('BILLING_DATE'),
            active_flag=data.get('ACTIVE_FLAG', True)
        )

def validate_account_data(data: Dict[str, Any]) -> List[str]:
    """Validate account data before saving."""
    errors = []
    
    if not data.get('account_name'):
        errors.append("Business name is required")
    if not data.get('contact_person'):
        errors.append("Contact person is required")
    if not data.get('contact_phone'):
        errors.append("Contact phone is required")
    if data.get('contact_email') and '@' not in data['contact_email']:
        errors.append("Invalid email format")
    if data.get('zip_code'):
        try:
            zip_code = int(str(data['zip_code']))
            if len(str(zip_code)) != 5:
                errors.append("ZIP code must be exactly 5 digits")
        except ValueError:
            errors.append("ZIP code must be a valid 5-digit number")
            
    return errors

# Add this function near the top of account.py, after the imports
def sanitize_zip_code(zip_code: Any) -> Optional[int]:
    """Convert zip code to integer format for Snowflake compatibility"""
    if not zip_code:
        return None
        
    try:
        # Convert to string first to handle various input types
        zip_str = str(zip_code).strip()
        # Remove any non-numeric characters
        zip_str = ''.join(filter(str.isdigit, zip_str))
        
        if len(zip_str) == 5:
            return int(zip_str)
        return None
    except (ValueError, TypeError):
        return None

# Update the validate_account_data function
def validate_account_data(data: Dict[str, Any]) -> List[str]:
    """Validate account data before saving."""
    errors = []
    
    if not data.get('account_name'):
        errors.append("Business name is required")
    if not data.get('contact_person'):
        errors.append("Contact person is required")
    if not data.get('contact_phone'):
        errors.append("Contact phone is required")
    if data.get('contact_email') and '@' not in data['contact_email']:
        errors.append("Invalid email format")
    
    # Validate zip code using sanitize_zip_code function
    if data.get('zip_code'):
        zip_code = sanitize_zip_code(data.get('zip_code'))
        if zip_code is None:
            errors.append("ZIP code must be a valid 5-digit number")
            
    return errors

# Update the save_account function
def save_account(data: Dict[str, Any], account_id: Optional[int] = None) -> Optional[int]:
    """Save or update account information."""
    try:
        # Enable debug mode for troubleshooting
        st.session_state.debug_mode = True
        print(f"DEBUG: save_account called with data: {data}")
        
        snowflake_conn = SnowflakeConnection.get_instance()
        if not snowflake_conn:
            print("DEBUG: Failed to get Snowflake connection")
            st.error("Database connection failed. Please try again.")
            return None
        
        # Sanitize zip code first
        zip_code = sanitize_zip_code(data.get('zip_code'))
        print(f"DEBUG: Original zip_code: {data.get('zip_code')}, sanitized: {zip_code}")
        if data.get('zip_code') and zip_code is None:
            st.error("Invalid ZIP code format. Please enter a 5-digit number.")
            return None
        
        # Clean and validate data
        clean_data = {
            'account_name': str(data.get('account_name', '')).strip(),
            'account_type': str(data.get('account_type', 'Commercial')).strip(),
            'account_description': str(data.get('account_description', '')),
            'contact_person': str(data.get('contact_person', '')).strip(),
            'contact_email': str(data.get('contact_email', '')),
            'contact_phone': str(data.get('contact_phone', '')).strip(),
            'billing_address': str(data.get('billing_address', '')),
            'city': str(data.get('city', '')),
            'state': str(data.get('state', '')),
            'zip_code': zip_code,  # Use sanitized zip code
            'billing_date': data.get('billing_date'),
            'active_flag': bool(data.get('active_flag', True))
        }
        print(f"DEBUG: Clean data: {clean_data}")
        
        if account_id:
            # Update existing account
            query = """
            UPDATE OPERATIONAL.CARPET.ACCOUNTS
            SET ACCOUNT_NAME = ?,
                ACCOUNT_TYPE = ?,
                ACCOUNT_DESCRIPTION = ?,
                CONTACT_PERSON = ?,
                CONTACT_EMAIL = ?,
                CONTACT_PHONE = ?,
                BILLING_ADDRESS = ?,
                CITY = ?,
                STATE = ?,
                ZIP_CODE = ?,
                BILLING_DATE = ?,
                ACTIVE_FLAG = ?,
                LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
            WHERE ACCOUNT_ID = ?
            """
            params = [
                clean_data['account_name'],
                clean_data['account_type'],
                clean_data['account_description'],
                clean_data['contact_person'],
                clean_data['contact_email'],
                clean_data['contact_phone'],
                clean_data['billing_address'],
                clean_data['city'],
                clean_data['state'],
                clean_data['zip_code'],
                clean_data['billing_date'],
                clean_data['active_flag'],
                account_id
            ]
            print(f"DEBUG: Updating existing account with ID: {account_id}")
            result = snowflake_conn.execute_query(query, params)
            print(f"DEBUG: Update result: {result}")
            return account_id
        else:
            # Insert new account with RETURNING clause to capture the new ACCOUNT_ID
            print("DEBUG: Inserting new account")
            query = """
            INSERT INTO OPERATIONAL.CARPET.ACCOUNTS (
                ACCOUNT_NAME,
                ACCOUNT_TYPE,
                ACCOUNT_DESCRIPTION,
                CONTACT_PERSON,
                CONTACT_EMAIL,
                CONTACT_PHONE,
                BILLING_ADDRESS,
                CITY,
                STATE,
                ZIP_CODE,
                BILLING_DATE,
                ACTIVE_FLAG,
                ACCOUNT_CREATION_DATE,
                LAST_MODIFIED_DATE
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
            RETURNING ACCOUNT_ID;
            """
            params = [
                clean_data['account_name'],
                clean_data['account_type'],
                clean_data['account_description'],
                clean_data['contact_person'],
                clean_data['contact_email'],
                clean_data['contact_phone'],
                clean_data['billing_address'],
                clean_data['city'],
                clean_data['state'],
                clean_data['zip_code'],
                clean_data['billing_date'],
                clean_data['active_flag']
            ]
            print(f"DEBUG: Insert parameters: {params}")
            try:
                result = snowflake_conn.execute_query(query, params)
                print(f"DEBUG: Insert result: {result}")
                if result:
                    print(f"DEBUG: New account ID: {result[0]['ACCOUNT_ID']}")
                    return result[0]['ACCOUNT_ID']
                else:
                    print("DEBUG: Insert returned no result")
                    return None
            except Exception as e:
                print(f"DEBUG: Error during INSERT operation: {str(e)}")
                st.error(f"Database error during account creation: {str(e)}")
                return None

    except Exception as e:
        print(f"DEBUG: Exception in save_account: {str(e)}")
        import traceback
        print(f"DEBUG: Traceback: {traceback.format_exc()}")
        st.error(f"Error saving account: {str(e)}")
        if st.session_state.get('debug_mode'):
            st.error(f"Debug - Error details: {str(e)}")
        return None

def fetch_all_accounts() -> pd.DataFrame:
    """Fetch all active accounts from the database."""
    query = """
    SELECT 
        ACCOUNT_ID, ACCOUNT_NAME, ACCOUNT_TYPE, ACCOUNT_DESCRIPTION,
        CONTACT_PERSON, CONTACT_EMAIL, CONTACT_PHONE,
        BILLING_ADDRESS, CITY, STATE, ZIP_CODE, BILLING_DATE,
        ACTIVE_FLAG
    FROM OPERATIONAL.CARPET.ACCOUNTS
    WHERE ACTIVE_FLAG = TRUE
    ORDER BY ACCOUNT_NAME
    """
    snowflake_conn = SnowflakeConnection.get_instance()
    result = snowflake_conn.execute_query(query)
    
    if result:
        df = pd.DataFrame(result)
        df['ACCOUNT_DETAILS'] = df['ACCOUNT_NAME'] + " (" + df['CONTACT_PERSON'] + ")"
        return df
    return pd.DataFrame()

def search_accounts(search_term: str) -> pd.DataFrame:
    """Search accounts by name, email, phone, or contact person."""
    query = """
    SELECT 
        ACCOUNT_ID, ACCOUNT_NAME, ACCOUNT_TYPE, ACCOUNT_DESCRIPTION,
        CONTACT_PERSON, CONTACT_EMAIL, CONTACT_PHONE,
        BILLING_ADDRESS, CITY, STATE, ZIP_CODE, BILLING_DATE,
        ACTIVE_FLAG
    FROM OPERATIONAL.CARPET.ACCOUNTS
    WHERE ACTIVE_FLAG = TRUE
    AND (
        LOWER(ACCOUNT_NAME) LIKE LOWER(:1)
        OR LOWER(CONTACT_EMAIL) LIKE LOWER(:2)
        OR CONTACT_PHONE LIKE :3
        OR LOWER(CONTACT_PERSON) LIKE LOWER(:4)
    )
    ORDER BY ACCOUNT_NAME
    """
    snowflake_conn = SnowflakeConnection.get_instance()
    search_pattern = f"%{search_term}%"
    result = snowflake_conn.execute_query(query, [
        search_pattern, search_pattern, search_pattern, search_pattern
    ])
    
    if result:
        df = pd.DataFrame(result)
        df['ACCOUNT_DETAILS'] = df['ACCOUNT_NAME'] + " (" + df['CONTACT_PERSON'] + ")"
        return df
    return pd.DataFrame()

def save_account_service_address(account_id: int, data: Dict[str, Any], is_primary: bool = True) -> Optional[int]:
    """Save account service address to the SERVICE_ADDRESSES table
    
    Args:
        account_id: ID of the account to associate with the address
        data: Dictionary containing service address details
        is_primary: Whether this is the primary service address
        
    Returns:
        Optional[int]: The address ID if successful, None if failed
    """
    try:
        print(f"DEBUG: save_account_service_address called with account_id: {account_id}")
        print(f"DEBUG: data: {data}")
        print(f"DEBUG: is_primary: {is_primary}")
        
        snowflake_conn = SnowflakeConnection.get_instance()
        if not snowflake_conn:
            print("DEBUG: Failed to get database connection")
            st.error("Database connection failed")
            return None
            
        # Clean and validate data
        service_zip = sanitize_zip_code(data.get('service_zip'))
        print(f"DEBUG: Original zip_code: {data.get('service_zip')}, sanitized: {service_zip}")
        
        if data.get('service_zip') and not service_zip:
            st.error("Invalid service ZIP code format. Please enter a 5-digit number.")
            return None
            
        # Check if address already exists for this account
        check_query = """
        SELECT ADDRESS_ID FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES
        WHERE ACCOUNT_ID = ? AND IS_PRIMARY_SERVICE = TRUE
        """
        try:
            existing = snowflake_conn.execute_query(check_query, [account_id])
            print(f"DEBUG: Check for existing address result: {existing}")
        except Exception as e:
            print(f"DEBUG: Error checking for existing address: {str(e)}")
            # Continue with insert if we can't check for existing
            existing = None
        
        if existing:
            # Update existing address
            address_id = existing[0]['ADDRESS_ID']
            print(f"DEBUG: Updating existing service address ID: {address_id}")
            
            query = """
            UPDATE OPERATIONAL.CARPET.SERVICE_ADDRESSES SET
                STREET_ADDRESS = ?,
                CITY = ?,
                STATE = ?,
                ZIP_CODE = ?,
                SQUARE_FOOTAGE = ?,
                IS_PRIMARY_SERVICE = ?,
                LAST_UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE ADDRESS_ID = ?
            """
            params = [
                str(data.get('service_address', '')),
                str(data.get('service_city', '')),
                str(data.get('service_state', '')),
                service_zip,
                int(data.get('service_addr_sq_ft', 0) or 0),
                is_primary,
                address_id
            ]
            
            print(f"DEBUG: Update query: {query}")
            print(f"DEBUG: Update params: {params}")
            
            try:
                snowflake_conn.execute_query(query, params)
                print(f"DEBUG: Address update successful")
                return address_id
            except Exception as e:
                print(f"DEBUG: Error updating service address: {str(e)}")
                st.error(f"Error updating service address: {str(e)}")
                return None
            
        else:
            # Insert new service address
            print(f"DEBUG: Inserting new service address for account: {account_id}")
            
            query = """
            INSERT INTO OPERATIONAL.CARPET.SERVICE_ADDRESSES (
                CUSTOMER_ID,
                ACCOUNT_ID,
                STREET_ADDRESS,
                CITY,
                STATE,
                ZIP_CODE,
                SQUARE_FOOTAGE,
                IS_PRIMARY_SERVICE
            ) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?)
            RETURNING ADDRESS_ID
            """
            params = [
                account_id,
                str(data.get('service_address', '')),
                str(data.get('service_city', '')),
                str(data.get('service_state', '')),
                service_zip,
                int(data.get('service_addr_sq_ft', 0) or 0),
                is_primary
            ]
            
            print(f"DEBUG: Insert query: {query}")
            print(f"DEBUG: Insert params: {params}")
            
            try:
                result = snowflake_conn.execute_query(query, params)
                print(f"DEBUG: Insert result: {result}")
                if result and len(result) > 0:
                    address_id = result[0]['ADDRESS_ID']
                    print(f"DEBUG: New service address created with ID: {address_id}")
                    return address_id
                else:
                    print("DEBUG: Insert did not return an ADDRESS_ID")
                    return None
            except Exception as e:
                print(f"DEBUG: Error inserting service address: {str(e)}")
                st.error(f"Error creating service address: {str(e)}")
                return None
            
    except Exception as e:
        st.error(f"Error saving service address: {str(e)}")
        print(f"DEBUG: Error saving service address: {str(e)}")
        import traceback
        print(f"DEBUG: Traceback: {traceback.format_exc()}")
        return None

def fetch_account(account_id: int) -> Optional[Dict[str, Any]]:
    """Fetch account details by ID."""
    query = """
    SELECT 
        ACCOUNT_ID, ACCOUNT_NAME, ACCOUNT_TYPE, ACCOUNT_DESCRIPTION,
        CONTACT_PERSON, CONTACT_EMAIL, CONTACT_PHONE,
        BILLING_ADDRESS, CITY, STATE, ZIP_CODE, BILLING_DATE,
        ACTIVE_FLAG
    FROM OPERATIONAL.CARPET.ACCOUNTS
    WHERE ACCOUNT_ID = ?
    """
    try:
        snowflake_conn = SnowflakeConnection.get_instance()
        result = snowflake_conn.execute_query(query, [account_id])
        return result[0] if result else None
    except Exception as e:
        print(f"Error fetching account: {str(e)}")
        return None