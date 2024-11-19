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


# Keep your existing functions
def fetch_customer(customer_id: int) -> Optional[CustomerModel]:
    """Fetch customer details by ID"""
    query = """
    SELECT *
    FROM OPERATIONAL.CARPET.CUSTOMER
    WHERE CUSTOMER_ID = :1
    """
    snowflake_conn = SnowflakeConnection.get_instance()
    result = snowflake_conn.execute_query(query, [customer_id])
    return CustomerModel.from_dict(result[0]) if result else None

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

def save_customer(data: Dict[str, Any]) -> Optional[int]:
    """Save new customer or update existing"""
    try:
        query = """
        INSERT INTO OPERATIONAL.CARPET.CUSTOMER (
            FIRST_NAME, LAST_NAME, PHONE_NUMBER,
            EMAIL_ADDRESS, STREET_ADDRESS, CITY,
            STATE, ZIP_CODE, TEXT_FLAG,
            PRIMARY_CONTACT_METHOD
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10
        )
        """
        params = [
            data['first_name'],
            data['last_name'],
            data['phone'],
            data.get('email'),
            data.get('street_address'),
            data.get('city'),
            data.get('state'),
            data.get('zip_code'),
            data.get('contact_method') == 'Text',
            data.get('contact_method', 'Phone')
        ]
        
        snowflake_conn = SnowflakeConnection.get_instance()
        snowflake_conn.execute_query(query, params)
        
        # Get the newly created customer ID
        result = snowflake_conn.execute_query(
            """
            SELECT CUSTOMER_ID 
            FROM CUSTOMER 
            WHERE FIRST_NAME = :1 
            AND LAST_NAME = :2 
            ORDER BY CREATED_AT DESC 
            LIMIT 1
            """,
            [data['first_name'], data['last_name']]
        )
        
        return result[0]['CUSTOMER_ID'] if result else None
        
    except Exception as e:
        st.error(f"Error saving customer: {str(e)}")
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