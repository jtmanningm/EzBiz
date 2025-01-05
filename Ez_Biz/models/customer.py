from typing import Optional, Dict, Any
from dataclasses import dataclass
import pandas as pd
import streamlit as st
from database.connection import SnowflakeConnection

@dataclass
class CustomerModel:
    customer_id: Optional[int] = None
    first_name: str = ""
    last_name: str = ""
    phone_number: str = ""
    email_address: Optional[str] = None
    street_address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    text_flag: bool = False
    primary_contact_method: str = "Phone"

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "first_name": self.first_name,
            "last_name": self.last_name,
            "phone": self.phone_number,
            "email": self.email_address,
            "street_address": self.street_address,
            "city": self.city,
            "state": self.state,
            "zip_code": self.zip_code,
            "contact_method": self.primary_contact_method
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
                street_address=data.get('STREET_ADDRESS'),
                city=data.get('CITY'),
                state=data.get('STATE'),
                zip_code=data.get('ZIP_CODE'),
                primary_contact_method=data.get('PRIMARY_CONTACT_METHOD', 'Phone')
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
        STREET_ADDRESS,
        CITY,
        STATE,
        ZIP_CODE,
        PRIMARY_CONTACT_METHOD
    FROM OPERATIONAL.CARPET.CUSTOMER
    WHERE CUSTOMER_ID = ?
    """
    try:
        snowflake_conn = SnowflakeConnection.get_instance()
        result = snowflake_conn.execute_query(query, [customer_id])
        
        if result and len(result) > 0:
            # Convert Snowflake Row to dictionary using its native method
            row = result[0]
            if hasattr(row, 'asDict'):
                data = row.asDict()
            elif hasattr(row, 'as_dict'):
                data = row.as_dict()
            elif hasattr(row, '_asdict'):
                data = row._asdict()
            else:
                # If it's already a dictionary
                data = dict(row)
                
            return CustomerModel.from_dict(data)
            
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

def fetch_all_customers() -> pd.DataFrame:
    """Fetch all customers"""
    query = """
    SELECT 
        CUSTOMER_ID, FIRST_NAME, LAST_NAME, 
        PHONE_NUMBER, EMAIL_ADDRESS, PRIMARY_CONTACT_METHOD,
        STREET_ADDRESS, CITY, STATE, ZIP_CODE
    FROM OPERATIONAL.CARPET.CUSTOMER
    """
    snowflake_conn = SnowflakeConnection.get_instance()
    result = snowflake_conn.execute_query(query)
    if result:
        df = pd.DataFrame(result)
        df['FULL_NAME'] = df['FIRST_NAME'] + " " + df['LAST_NAME']
        return df
    return pd.DataFrame()

# In models/customer.py
def save_customer(data: Dict[str, Any], customer_id: Optional[int] = None) -> Optional[int]:
    """Save or update customer information."""
    try:
        snowflake_conn = SnowflakeConnection.get_instance()
        
        # Clean and validate data to match schema types
        clean_data = {
            'first_name': str(data.get('first_name', '')).strip(),
            'last_name': str(data.get('last_name', '')).strip(),
            'phone_number': str(data.get('phone_number', '')).strip(),
            'email_address': str(data.get('email_address', '')),
            'street_address': str(data.get('street_address', '')),
            'city': str(data.get('city', '')),
            'state': str(data.get('state', '')),
            'zip_code': int(str(data.get('zip_code', '0')).strip()) if data.get('zip_code') else None,
            'text_flag': bool(data.get('text_flag', False)),
            'primary_contact_method': str(data.get('primary_contact_method', 'Phone'))[:50],  # VARCHAR(50)
            'service_address': str(data.get('service_address', '')),
            'service_address_2': str(data.get('service_address_2', '')) if data.get('service_address_2') else None,
            'service_address_3': str(data.get('service_address_3', '')) if data.get('service_address_3') else None,
            'service_addr_sq_ft': int(data.get('service_addr_sq_ft')) if data.get('service_addr_sq_ft') else None,
            'comments': str(data.get('comments', '')),
            'member_flag': bool(data.get('member_flag', False))
        }

        if customer_id:
            # Update existing customer
            query = """
            UPDATE OPERATIONAL.CARPET.CUSTOMER
            SET FIRST_NAME = ?,
                LAST_NAME = ?,
                STREET_ADDRESS = ?,
                CITY = ?,
                STATE = ?,
                ZIP_CODE = ?,
                EMAIL_ADDRESS = ?,
                PHONE_NUMBER = ?,
                TEXT_FLAG = ?,
                SERVICE_ADDR_SQ_FT = ?,
                COMMENTS = ?,
                SERVICE_ADDRESS_3 = ?,
                SERVICE_ADDRESS_2 = ?,
                SERVICE_ADDRESS = ?,
                PRIMARY_CONTACT_METHOD = ?,
                MEMBER_FLAG = ?,
                LAST_UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE CUSTOMER_ID = ?
            """
            params = [
                clean_data['first_name'],
                clean_data['last_name'],
                clean_data['street_address'],
                clean_data['city'],
                clean_data['state'],
                clean_data['zip_code'],
                clean_data['email_address'],
                clean_data['phone_number'],
                clean_data['text_flag'],
                clean_data['service_addr_sq_ft'],
                clean_data['comments'],
                clean_data['service_address_3'],
                clean_data['service_address_2'],
                clean_data['service_address'],
                clean_data['primary_contact_method'],
                clean_data['member_flag'],
                customer_id
            ]
            snowflake_conn.execute_query(query, params)
            return customer_id
        else:
            # Insert new customer
            query = """
            INSERT INTO OPERATIONAL.CARPET.CUSTOMER (
                FIRST_NAME,
                LAST_NAME,
                STREET_ADDRESS,
                CITY,
                STATE,
                ZIP_CODE,
                EMAIL_ADDRESS,
                PHONE_NUMBER,
                TEXT_FLAG,
                SERVICE_ADDR_SQ_FT,
                COMMENTS,
                SERVICE_ADDRESS_3,
                SERVICE_ADDRESS_2,
                SERVICE_ADDRESS,
                PRIMARY_CONTACT_METHOD,
                MEMBER_FLAG
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            params = [
                clean_data['first_name'],
                clean_data['last_name'],
                clean_data['street_address'],
                clean_data['city'],
                clean_data['state'],
                clean_data['zip_code'],
                clean_data['email_address'],
                clean_data['phone_number'],
                clean_data['text_flag'],
                clean_data['service_addr_sq_ft'],
                clean_data['comments'],
                clean_data['service_address_3'],
                clean_data['service_address_2'],
                clean_data['service_address'],
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
            
            return result[0]['CUSTOMER_ID'] if result else None

    except Exception as e:
        st.error(f"Error saving customer: {str(e)}")
        # Add debug information
        st.write("Debug info:")
        st.write(f"Error type: {type(e).__name__}")
        st.write(f"Error message: {str(e)}")
        st.write("Cleaned data:")
        for key, value in clean_data.items():
            st.write(f"{key}: {value} ({type(value)})")
        return None

def search_customers(search_term: str) -> pd.DataFrame:
    """Search customers by name, phone, or email"""
    query = """
    SELECT 
        CUSTOMER_ID, FIRST_NAME, LAST_NAME,
        PHONE_NUMBER, EMAIL_ADDRESS,
        STREET_ADDRESS, CITY, STATE, ZIP_CODE,
        PRIMARY_CONTACT_METHOD
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