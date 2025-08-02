# utils/business/info.py
import streamlit as st
from typing import Dict, Any
from database.connection import snowflake_conn

def fetch_business_info() -> Dict[str, Any]:
    """Fetch current business information from settings"""
    query = """
    SELECT 
        BUSINESS_ID,
        BUSINESS_NAME,
        STREET_ADDRESS,
        CITY,
        STATE,
        ZIP_CODE,
        PHONE_NUMBER,
        EMAIL_ADDRESS,
        WEBSITE,
        OPERATING_HOURS_START,
        OPERATING_HOURS_END,
        WEEKEND_OPERATING_HOURS_START,
        WEEKEND_OPERATING_HOURS_END,
        ACTIVE_STATUS,
        MODIFIED_DATE
    FROM OPERATIONAL.CARPET.BUSINESS_INFO
    WHERE ACTIVE_STATUS = TRUE
    ORDER BY MODIFIED_DATE DESC
    LIMIT 1
    """
    try:
        result = snowflake_conn.execute_query(query)
        if not result:
            return {}

        # Convert Snowflake Row object to dictionary
        row = result[0]
        business_info = {}
        for column in [
            "BUSINESS_ID", "BUSINESS_NAME", "STREET_ADDRESS", "CITY", "STATE",
            "ZIP_CODE", "PHONE_NUMBER", "EMAIL_ADDRESS", "WEBSITE",
            "OPERATING_HOURS_START", "OPERATING_HOURS_END",
            "WEEKEND_OPERATING_HOURS_START", "WEEKEND_OPERATING_HOURS_END",
            "ACTIVE_STATUS", "MODIFIED_DATE"
        ]:
            try:
                business_info[column] = getattr(row, column, None)
            except AttributeError:
                business_info[column] = None

        # Clean each field
        for key in business_info:
            value = business_info[key]
            if value is None or value == 'None':
                business_info[key] = ''
            else:
                business_info[key] = str(value).strip()

        if not business_info.get('EMAIL_ADDRESS'):
            business_info['EMAIL_ADDRESS'] = 'no-reply@joinezbiz.com'

        # Format phone number if present
        if business_info.get('PHONE_NUMBER'):
            phone = ''.join(filter(str.isdigit, business_info['PHONE_NUMBER']))
            if len(phone) == 10:
                business_info['PHONE_NUMBER'] = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"

        return business_info
    except Exception as e:
        st.error(f"Error fetching business info: {str(e)}")
        return {}