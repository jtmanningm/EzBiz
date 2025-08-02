from typing import Optional, Dict, Any
from dataclasses import dataclass
import pandas as pd
import streamlit as st
from database.connection import SnowflakeConnection
from utils.validation import sanitize_zip_code

@dataclass
class CustomerModel:
    customer_id: Optional[int] = None
    first_name: str = ""
    last_name: str = ""
    phone_number: str = ""
    email_address: Optional[str] = None
    billing_address: Optional[str] = None
    billing_city: Optional[str] = None
    billing_state: Optional[str] = None
    billing_zip: Optional[int] = None
    text_flag: bool = False
    primary_contact_method: str = "SMS"
    comments: Optional[str] = None
    member_flag: bool = False

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "first_name": self.first_name,
            "last_name": self.last_name,
            "phone_number": self.phone_number,
            "email_address": self.email_address,
            "billing_address": self.billing_address,
            "billing_city": self.billing_city,
            "billing_state": self.billing_state,
            "billing_zip": self.billing_zip,
            "text_flag": self.text_flag,
            "primary_contact_method": self.primary_contact_method,
            "comments": self.comments,
            "member_flag": self.member_flag
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CustomerModel':
        """Convert dictionary or Snowflake Row to CustomerModel"""
        try:
            # Handle Snowflake Row object
            if hasattr(data, 'asDict'):
                data = data.asDict()
            elif hasattr(data, 'as_dict'):
                data = data.as_dict()
            elif hasattr(data, '_asdict'):
                data = data._asdict()
            
            return cls(
                customer_id=data.get('CUSTOMER_ID'),
                first_name=data.get('FIRST_NAME', ''),
                last_name=data.get('LAST_NAME', ''),
                phone_number=data.get('PHONE_NUMBER', ''),
                email_address=data.get('EMAIL_ADDRESS'),
                billing_address=data.get('BILLING_ADDRESS'),
                billing_city=data.get('BILLING_CITY'),
                billing_state=data.get('BILLING_STATE'),
                billing_zip=data.get('BILLING_ZIP'),
                text_flag=data.get('TEXT_FLAG', False),
                primary_contact_method=data.get('PRIMARY_CONTACT_METHOD', 'Phone'),
                comments=data.get('COMMENTS'),
                member_flag=data.get('MEMBER_FLAG', False)
            )
        except Exception as e:
            print(f"Error converting data to CustomerModel: {str(e)}")
            print("Input data:", data)
            print("Data type:", type(data))
            raise


# Keep your existing functions
def fetch_customer(customer_id: int) -> Optional[CustomerModel]:
    """Fetch customer details by ID"""
    query = """
    SELECT 
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME,
        PHONE_NUMBER,
        EMAIL_ADDRESS,
        BILLING_ADDRESS,
        BILLING_CITY,
        BILLING_STATE,
        BILLING_ZIP,
        PRIMARY_CONTACT_METHOD,
        TEXT_FLAG,
        COMMENTS,
        MEMBER_FLAG
    FROM OPERATIONAL.CARPET.CUSTOMER
    WHERE CUSTOMER_ID = ?
    """
    try:
        snowflake_conn = SnowflakeConnection.get_instance()
        result = snowflake_conn.execute_query(query, [customer_id])
        
        if result and len(result) > 0:
            return CustomerModel.from_dict(result[0])
        return None
        
    except Exception as e:
        print(f"Error fetching customer: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return None

def test_fetch_customer(customer_id: int):
    """Test function to verify customer fetching"""
    print("Testing customer fetch...")
    customer = fetch_customer(customer_id)
    if customer:
        print("Found customer:")
        print(f"  Name: {customer.full_name}")
        print(f"  Email: {customer.email_address}")
        print(f"  Phone: {customer.phone_number}")
        return True
    else:
        print("Customer not found")
        return False

if __name__ == "__main__":
    # Add this at the bottom of customer.py for testing
    import sys
    if len(sys.argv) > 1:
        test_fetch_customer(int(sys.argv[1]))

def fetch_all_customers():
    """Fetch all customers with their service addresses"""
    query = """
    SELECT 
        C.CUSTOMER_ID, 
        C.FIRST_NAME, 
        C.LAST_NAME,
        C.PHONE_NUMBER, 
        C.EMAIL_ADDRESS, 
        C.PRIMARY_CONTACT_METHOD,
        C.TEXT_FLAG,
        -- Billing address fields
        C.BILLING_ADDRESS,
        C.BILLING_CITY,
        C.BILLING_STATE, 
        C.BILLING_ZIP,
        -- Service address fields
        SA.STREET_ADDRESS as SERVICE_ADDRESS,
        SA.CITY as SERVICE_CITY,
        SA.STATE as SERVICE_STATE,
        SA.ZIP_CODE as SERVICE_ZIP,
        SA.SQUARE_FOOTAGE as SERVICE_ADDR_SQ_FT,
        SA.IS_PRIMARY_SERVICE
    FROM OPERATIONAL.CARPET.CUSTOMER C
    LEFT JOIN OPERATIONAL.CARPET.SERVICE_ADDRESSES SA 
        ON C.CUSTOMER_ID = SA.CUSTOMER_ID
        AND (SA.IS_PRIMARY_SERVICE = TRUE OR SA.IS_PRIMARY_SERVICE IS NULL)
    ORDER BY C.CUSTOMER_ID DESC
    """
    try:
        snowflake_conn = SnowflakeConnection.get_instance()
        result = snowflake_conn.execute_query(query)
        if result:
            df = pd.DataFrame(result)
            df['FULL_NAME'] = df['FIRST_NAME'] + ' ' + df['LAST_NAME']
            
            # Fill any missing service address fields with billing address fields
            if 'SERVICE_ADDRESS' not in df.columns or df['SERVICE_ADDRESS'].isna().all():
                df['SERVICE_ADDRESS'] = df['BILLING_ADDRESS']
                df['SERVICE_CITY'] = df['BILLING_CITY']
                df['SERVICE_STATE'] = df['BILLING_STATE']
                df['SERVICE_ZIP'] = df['BILLING_ZIP']
                df['SERVICE_ADDR_SQ_FT'] = 0

            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching customers: {str(e)}")
        if st.session_state.get('debug_mode'):
            st.exception(e)
        return pd.DataFrame()

def save_service_address(snowflake_conn: Any, customer_id: int, data: Dict[str, Any], is_primary: bool = False) -> Optional[int]:
    """Save service address to the SERVICE_ADDRESSES table"""
    try:
        # Convert zip code to proper format
        service_zip = sanitize_zip_code(data.get('service_zip'))
        if not service_zip:
            st.error("Invalid service address ZIP code format. Please enter a 5-digit number.")
            return None

        query = """
        INSERT INTO OPERATIONAL.CARPET.SERVICE_ADDRESSES (
            CUSTOMER_ID,
            STREET_ADDRESS,
            CITY,
            STATE,
            ZIP_CODE,
            SQUARE_FOOTAGE,
            IS_PRIMARY_SERVICE
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        params = [
            customer_id,
            str(data.get('service_address', '')).strip(),
            str(data.get('service_city', '')).strip(),
            str(data.get('service_state', '')).strip(),
            service_zip,
            int(data.get('service_addr_sq_ft', 0)),
            is_primary
        ]

        snowflake_conn.execute_query(query, params)
        
        # Get the newly created address ID
        result = snowflake_conn.execute_query(
            """
            SELECT ADDRESS_ID 
            FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES 
            WHERE CUSTOMER_ID = ? 
            ORDER BY CREATED_AT DESC 
            LIMIT 1
            """,
            [customer_id]
        )
        
        return result[0]['ADDRESS_ID'] if result else None

    except Exception as e:
        st.error(f"Error saving service address: {str(e)}")
        if st.session_state.get('debug_mode'):
            st.exception(e)
        return None

def save_customer(data: Dict[str, Any], customer_id: Optional[int] = None) -> Optional[int]:
    """Save or update customer information and service addresses."""
    try:
        snowflake_conn = SnowflakeConnection.get_instance()
        
        # Convert zip codes to proper integer format
        billing_zip = sanitize_zip_code(data.get('billing_zip'))
        
        if not billing_zip:
            st.error("Invalid billing ZIP code format. Please enter a 5-digit number.")
            return None
            
        # Clean and validate data to match schema types
        clean_data = {
            'first_name': str(data.get('first_name', '')).strip(),
            'last_name': str(data.get('last_name', '')).strip(),
            'phone_number': str(data.get('phone_number', '')).strip(),
            'email_address': str(data.get('email_address', '')),
            'billing_address': str(data.get('billing_address', '')),
            'city': str(data.get('city', '')),
            'state': str(data.get('state', '')),
            'billing_zip': billing_zip,
            'text_flag': bool(data.get('text_flag', False)),
            'primary_contact_method': str(data.get('primary_contact_method', 'Phone'))[:50],
            'comments': str(data.get('comments', '')),
            'member_flag': bool(data.get('member_flag', False))
        }

        saved_customer_id = None
        if customer_id:
            # Update existing customer
            query = """
            UPDATE OPERATIONAL.CARPET.CUSTOMER
            SET FIRST_NAME = ?,
                LAST_NAME = ?,
                BILLING_ADDRESS = ?,
                CITY = ?,
                STATE = ?,
                BILLING_ZIP = ?,
                EMAIL_ADDRESS = ?,
                PHONE_NUMBER = ?,
                TEXT_FLAG = ?,
                COMMENTS = ?,
                PRIMARY_CONTACT_METHOD = ?,
                MEMBER_FLAG = ?,
                LAST_UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE CUSTOMER_ID = ?
            """
            params = [
                clean_data['first_name'],
                clean_data['last_name'],
                clean_data['billing_address'],
                clean_data['city'],
                clean_data['state'],
                clean_data['billing_zip'],
                clean_data['email_address'],
                clean_data['phone_number'],
                clean_data['text_flag'],
                clean_data['comments'],
                clean_data['primary_contact_method'],
                clean_data['member_flag'],
                customer_id
            ]
            snowflake_conn.execute_query(query, params)
            saved_customer_id = customer_id
        else:
            # Insert new customer
            query = """
            INSERT INTO OPERATIONAL.CARPET.CUSTOMER (
                FIRST_NAME,
                LAST_NAME,
                BILLING_ADDRESS,
                CITY,
                STATE,
                BILLING_ZIP,
                EMAIL_ADDRESS,
                PHONE_NUMBER,
                TEXT_FLAG,
                COMMENTS,
                PRIMARY_CONTACT_METHOD,
                MEMBER_FLAG
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = [
                clean_data['first_name'],
                clean_data['last_name'],
                clean_data['billing_address'],
                clean_data['city'],
                clean_data['state'],
                clean_data['billing_zip'],
                clean_data['email_address'],
                clean_data['phone_number'],
                clean_data['text_flag'],
                clean_data['comments'],
                clean_data['primary_contact_method'],
                clean_data['member_flag']
            ]

            # Execute insert
            snowflake_conn.execute_query(query, params)

            # Get the newly created customer ID
            result = snowflake_conn.execute_query(
                """
                SELECT CUSTOMER_ID 
                FROM OPERATIONAL.CARPET.CUSTOMER 
                WHERE FIRST_NAME = ? 
                AND LAST_NAME = ? 
                AND PHONE_NUMBER = ?
                ORDER BY CREATED_AT DESC 
                LIMIT 1
                """,
                [clean_data['first_name'], clean_data['last_name'], clean_data['phone_number']]
            )
            
            saved_customer_id = result[0]['CUSTOMER_ID'] if result else None

        # If customer was saved successfully, save the service address
        if saved_customer_id:
            # Check if this is a primary service address
            is_primary = not bool(data.get('different_billing', False))
            
            # Save service address
            address_id = save_service_address(
                snowflake_conn=snowflake_conn,
                customer_id=saved_customer_id,
                data=data,
                is_primary=is_primary
            )
            
            if not address_id:
                st.error("Failed to save service address")
                return None

        return saved_customer_id

    except Exception as e:
        st.error(f"Error saving customer: {str(e)}")
        if st.session_state.get('debug_mode'):
            st.error(f"Debug - Full error details: {str(e)}")
            import traceback
            st.error(f"Traceback: {traceback.format_exc()}")
        return None

def search_customers(search_term: str) -> pd.DataFrame:
    """Search customers by name, phone, or email"""
    query = """
    SELECT 
        CUSTOMER_ID, 
        FIRST_NAME, 
        LAST_NAME,
        PHONE_NUMBER, 
        EMAIL_ADDRESS,
        PRIMARY_CONTACT_METHOD,
        BILLING_ADDRESS, 
        BILLING_CITY, 
        BILLING_STATE, 
        BILLING_ZIP,
        PRIMARY_STREET,
        PRIMARY_CITY,
        PRIMARY_STATE,
        PRIMARY_ZIP,
        SERVICE_STREET,
        SERVICE_CITY,
        SERVICE_STATE,
        SERVICE_ZIP
    FROM OPERATIONAL.CARPET.CUSTOMER
    WHERE 
        LOWER(FIRST_NAME || ' ' || LAST_NAME) LIKE LOWER(:1)
        OR PHONE_NUMBER LIKE :2
        OR LOWER(EMAIL_ADDRESS) LIKE LOWER(:3)
    """
    
    snowflake_conn = SnowflakeConnection.get_instance()
    search_pattern = f"%{search_term}%"
    result = snowflake_conn.execute_query(query, [
        search_pattern, search_pattern, search_pattern
    ])
    
    if result:
        df = pd.DataFrame(result)
        df['FULL_NAME'] = df['FIRST_NAME'] + " " + df['LAST_NAME']
        return df
    return pd.DataFrame()