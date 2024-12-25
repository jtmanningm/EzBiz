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

# customer.py
# def save_customer(data: Dict[str, Any]) -> Optional[int]:
#     """Save new customer or update existing"""
#     try:
#         query = """
#         INSERT INTO OPERATIONAL.CARPET.CUSTOMER (
#             FIRST_NAME, LAST_NAME, PHONE_NUMBER,
#             EMAIL_ADDRESS, STREET_ADDRESS, CITY,
#             STATE, ZIP_CODE, TEXT_FLAG,
#             PRIMARY_CONTACT_METHOD, SERVICE_ADDRESS,
#             SERVICE_ADDRESS_2, SERVICE_ADDRESS_3,
#             SERVICE_ADDR_SQ_FT, COMMENTS,
#             MEMBER_FLAG
#         ) VALUES (
#             :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
#             :11, :12, :13, :14, :15, :16
#         )
#         """
#         params = [
#             data['first_name'],
#             data['last_name'],
#             data['phone_number'],
#             data['email_address'],
#             data['street_address'],
#             data['city'],
#             data['state'],
#             data['zip_code'],
#             data['text_flag'],
#             data['primary_contact_method'],
#             data['service_address'],
#             data['service_address_2'],
#             data['service_address_3'],
#             data['service_addr_sq_ft'],
#             data['comments'],
#             data['member_flag']
#         ]
        
#         snowflake_conn = SnowflakeConnection.get_instance()
#         snowflake_conn.execute_query(query, params)
        
#         # Get the newly created customer ID
#         result = snowflake_conn.execute_query(
#             """
#             SELECT CUSTOMER_ID 
#             FROM OPERATIONAL.CARPET.CUSTOMER 
#             WHERE FIRST_NAME = :1 
#             AND LAST_NAME = :2 
#             ORDER BY CREATED_AT DESC 
#             LIMIT 1
#             """,
#             [data['first_name'], data['last_name']]
#         )
        
#         return result[0]['CUSTOMER_ID'] if result else None
        
#     except Exception as e:
#         st.error(f"Error saving customer: {str(e)}")
#         return None

def save_customer(data: Dict[str, Any], customer_id: Optional[int] = None) -> Optional[int]:
    """Save or update customer information."""
    try:
        snowflake_conn = SnowflakeConnection.get_instance()
        
        if customer_id:
            # Update existing customer
            query = """
            UPDATE OPERATIONAL.CARPET.CUSTOMER
            SET FIRST_NAME = :1, LAST_NAME = :2, PHONE_NUMBER = :3, EMAIL_ADDRESS = :4,
                STREET_ADDRESS = :5, CITY = :6, STATE = :7, ZIP_CODE = :8,
                TEXT_FLAG = :9, PRIMARY_CONTACT_METHOD = :10, SERVICE_ADDRESS = :11,
                SERVICE_ADDRESS_2 = :12, SERVICE_ADDRESS_3 = :13, SERVICE_ADDR_SQ_FT = :14,
                COMMENTS = :15, MEMBER_FLAG = :16, LAST_UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE CUSTOMER_ID = :17
            """
            params = [
                data['first_name'],
                data['last_name'],
                data['phone_number'],
                data['email_address'],
                data['street_address'],
                data['city'],
                data['state'],
                data['zip_code'],
                data['text_flag'],
                data['primary_contact_method'],
                data['service_address'],
                data['service_address_2'],
                data['service_address_3'],
                data['service_addr_sq_ft'],
                data['comments'],
                data['member_flag'],
                customer_id
            ]
            snowflake_conn.execute_query(query, params)
            return customer_id
        else:
            # Insert new customer
            query = """
            INSERT INTO OPERATIONAL.CARPET.CUSTOMER (
                FIRST_NAME, LAST_NAME, PHONE_NUMBER, EMAIL_ADDRESS,
                STREET_ADDRESS, CITY, STATE, ZIP_CODE, TEXT_FLAG,
                PRIMARY_CONTACT_METHOD, SERVICE_ADDRESS, SERVICE_ADDRESS_2,
                SERVICE_ADDRESS_3, SERVICE_ADDR_SQ_FT, COMMENTS, MEMBER_FLAG
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
                :11, :12, :13, :14, :15, :16
            )
            """
            params = [
                data['first_name'],
                data['last_name'],
                data['phone_number'],
                data['email_address'],
                data['street_address'],
                data['city'],
                data['state'],
                data['zip_code'],
                data['text_flag'],
                data['primary_contact_method'],
                data['service_address'],
                data['service_address_2'],
                data['service_address_3'],
                data['service_addr_sq_ft'],
                data['comments'],
                data['member_flag']
            ]
            snowflake_conn.execute_query(query, params)
            
            # Retrieve and return the new customer ID
            result = snowflake_conn.execute_query(
                """
                SELECT CUSTOMER_ID 
                FROM OPERATIONAL.CARPET.CUSTOMER 
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