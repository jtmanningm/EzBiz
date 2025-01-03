import streamlit as st
from datetime import time
from database.connection import snowflake_conn
import traceback
from typing import Dict, Any


def fetch_business_info() -> Dict:
    """Fetch current business information from settings with improved NULL handling"""
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
            print("No business info found in database")
            return {}

        # Inspect the structure of the first row
        print("Query result:", result)
        print("First row:", result[0])

        # Map result to dictionary
        column_names = [
            "BUSINESS_ID", "BUSINESS_NAME", "STREET_ADDRESS", "CITY", "STATE",
            "ZIP_CODE", "PHONE_NUMBER", "EMAIL_ADDRESS", "WEBSITE",
            "OPERATING_HOURS_START", "OPERATING_HOURS_END",
            "WEEKEND_OPERATING_HOURS_START", "WEEKEND_OPERATING_HOURS_END",
            "ACTIVE_STATUS", "MODIFIED_DATE"
        ]
        business_info = {column: result[0][i] for i, column in enumerate(column_names)}

        # Clean and validate each field
        for key in business_info:
            value = business_info[key]

            # Handle NULL and 'None' string values
            if value is None or value == 'None':
                business_info[key] = ''
            else:
                # Convert to string and strip whitespace
                business_info[key] = str(value).strip()

        # Validate required fields
        if not business_info.get('BUSINESS_NAME'):
            print("Business name is required")
            return {}

        if not business_info.get('EMAIL_ADDRESS'):
            # Set default sending email if not configured
            business_info['EMAIL_ADDRESS'] = 'no-reply@joinezbiz.com'

        # Format phone number if present
        if business_info.get('PHONE_NUMBER'):
            # Remove any non-numeric characters
            phone = ''.join(filter(str.isdigit, business_info['PHONE_NUMBER']))
            if len(phone) == 10:
                business_info['PHONE_NUMBER'] = f"({phone[:3]}) {phone[3:6]}-{phone[6:]}"

        return business_info

    except Exception as e:
        print(f"Error fetching business info: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return {}
        
def business_settings_page():
    st.title("Business Settings")

    # Fetch current settings
    query = """
    SELECT BUSINESS_ID, BUSINESS_NAME, PHONE_NUMBER, EMAIL_ADDRESS, WEBSITE,
           STREET_ADDRESS, CITY, STATE, ZIP_CODE,
           OPERATING_HOURS_START, OPERATING_HOURS_END,
           WEEKEND_OPERATING_HOURS_START, WEEKEND_OPERATING_HOURS_END,
           ACTIVE_STATUS, MODIFIED_DATE
    FROM OPERATIONAL.CARPET.BUSINESS_INFO
    WHERE ACTIVE_STATUS = TRUE
    ORDER BY MODIFIED_DATE DESC
    LIMIT 1
    """
    
    try:
        result = snowflake_conn.execute_query(query)
        if st.session_state.get('debug_mode'):
            st.write("Raw Query Result:", result)
            
        if result and len(result) > 0:
            columns = [
                'BUSINESS_ID', 'BUSINESS_NAME', 'PHONE_NUMBER', 'EMAIL_ADDRESS', 'WEBSITE',
                'STREET_ADDRESS', 'CITY', 'STATE', 'ZIP_CODE',
                'OPERATING_HOURS_START', 'OPERATING_HOURS_END',
                'WEEKEND_OPERATING_HOURS_START', 'WEEKEND_OPERATING_HOURS_END',
                'ACTIVE_STATUS', 'MODIFIED_DATE'
            ]
            settings = dict(zip(columns, result[0]))
            if st.session_state.get('debug_mode'):
                st.write("Settings After Conversion:", settings)
        else:
            st.warning("No active business settings found.")
            settings = {}
    except Exception as e:
        st.error(f"Failed to fetch settings: {str(e)}")
        if st.session_state.get('debug_mode'):
            import traceback
            st.error(f"Full error: {traceback.format_exc()}")
        settings = {}

    with st.form("business_settings_form"):
        # Company Information
        st.header("Company Details")
        col1, col2 = st.columns(2)
        
        with col1:
            business_name = st.text_input(
                "Business Name*",
                value=str(settings.get('BUSINESS_NAME', '')).replace('None', '').strip(),
                key="business_name_input"
            )
            phone = st.text_input(
                "Business Phone*",
                value=str(settings.get('PHONE_NUMBER', '')).replace('None', '').strip(),
                key="phone_input"
            )

        with col2:
            website = st.text_input(
                "Website",
                value=str(settings.get('WEBSITE', '')).replace('None', '').strip(),
                key="website_input"
            )
            email = st.text_input(
                "Business Email",
                value=str(settings.get('EMAIL_ADDRESS', '')).replace('None', '').strip(),
                key="email_input"
            )

        # Address Information
        st.header("Business Address")
        street_address = st.text_input(
            "Street Address*",
            value=str(settings.get('STREET_ADDRESS', '')).replace('None', '').strip(),
            key="street_input"
        )
        
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            city = st.text_input(
                "City*",
                value=str(settings.get('CITY', '')).replace('None', '').strip(),
                key="city_input"
            )
        with col2:
            state = st.text_input(
                "State*",
                value=str(settings.get('STATE', '')).replace('None', '').strip(),
                key="state_input"
            )
        with col3:
            zip_code = st.text_input(
                "ZIP Code*",
                value=str(settings.get('ZIP_CODE', '')).replace('None', '').strip(),
                key="zip_input"
            )

        # Operating Hours
        st.header("Operating Hours")
        default_start = time(8, 0)
        default_end = time(17, 0)
        
        col1, col2 = st.columns(2)
        with col1:
            try:
                weekday_start = st.time_input(
                    "Weekday Opening Time",
                    value=default_start if not settings.get('OPERATING_HOURS_START') else 
                          time.fromisoformat(str(settings.get('OPERATING_HOURS_START'))),
                    key="weekday_start_input"
                )
                weekend_start = st.time_input(
                    "Weekend Opening Time",
                    value=default_start if not settings.get('WEEKEND_OPERATING_HOURS_START') else
                          time.fromisoformat(str(settings.get('WEEKEND_OPERATING_HOURS_START'))),
                    key="weekend_start_input"
                )
            except (ValueError, TypeError):
                weekday_start = default_start
                weekend_start = default_start
        
        with col2:
            try:
                weekday_end = st.time_input(
                    "Weekday Closing Time",
                    value=default_end if not settings.get('OPERATING_HOURS_END') else
                          time.fromisoformat(str(settings.get('OPERATING_HOURS_END'))),
                    key="weekday_end_input"
                )
                weekend_end = st.time_input(
                    "Weekend Closing Time",
                    value=default_end if not settings.get('WEEKEND_OPERATING_HOURS_END') else
                          time.fromisoformat(str(settings.get('WEEKEND_OPERATING_HOURS_END'))),
                    key="weekend_end_input"
                )
            except (ValueError, TypeError):
                weekday_end = default_end
                weekend_end = default_end

        submitted = st.form_submit_button("Save Business Information")
        
        if submitted:
            try:
                if not all([business_name, phone, street_address, city, state, zip_code]):
                    st.error("Please fill in all required fields (*)")
                    return

                business_data = [
                    business_name,
                    phone,
                    email if email else None,
                    website,
                    street_address,
                    city,
                    state,
                    zip_code,
                    weekday_start.strftime('%H:%M'),
                    weekday_end.strftime('%H:%M'),
                    weekend_start.strftime('%H:%M'),
                    weekend_end.strftime('%H:%M')
                ]

                if settings.get('BUSINESS_ID'):
                    query = """
                    UPDATE OPERATIONAL.CARPET.BUSINESS_INFO
                    SET BUSINESS_NAME = ?,
                        PHONE_NUMBER = ?,
                        EMAIL_ADDRESS = ?,
                        WEBSITE = ?,
                        STREET_ADDRESS = ?,
                        CITY = ?,
                        STATE = ?,
                        ZIP_CODE = ?,
                        OPERATING_HOURS_START = ?,
                        OPERATING_HOURS_END = ?,
                        WEEKEND_OPERATING_HOURS_START = ?,
                        WEEKEND_OPERATING_HOURS_END = ?,
                        MODIFIED_DATE = CURRENT_TIMESTAMP()
                    WHERE BUSINESS_ID = ?
                    """
                    business_data.append(settings['BUSINESS_ID'])
                else:
                    query = """
                    INSERT INTO OPERATIONAL.CARPET.BUSINESS_INFO (
                        BUSINESS_NAME, PHONE_NUMBER, EMAIL_ADDRESS, WEBSITE,
                        STREET_ADDRESS, CITY, STATE, ZIP_CODE,
                        OPERATING_HOURS_START, OPERATING_HOURS_END,
                        WEEKEND_OPERATING_HOURS_START, WEEKEND_OPERATING_HOURS_END,
                        ACTIVE_STATUS
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                    """

                if st.session_state.get('debug_mode'):
                    st.write("Query:", query)
                    st.write("Data:", business_data)

                result = snowflake_conn.execute_query(query, business_data)
                
                if st.session_state.get('debug_mode'):
                    st.write("Save Result:", result)

                if result is not None:
                    st.success("Business information saved successfully!")
                    st.rerun()
                else:
                    st.error("Failed to save business information")

            except Exception as e:
                st.error(f"Error saving business information: {str(e)}")
                if st.session_state.get('debug_mode'):
                    import traceback
                    st.error(f"Full error: {traceback.format_exc()}")

if __name__ == "__main__":
    business_settings_page()