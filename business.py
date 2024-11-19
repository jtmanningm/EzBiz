import streamlit as st
from datetime import time
from database.connection import snowflake_conn

def business_settings_page():
    """Business information and settings management page"""
    st.title("Business Settings")

    # Fetch current settings
    query = """
    SELECT *
    FROM OPERATIONAL.CARPET.BUSINESS_INFO
    WHERE ACTIVE_STATUS = TRUE
    ORDER BY MODIFIED_DATE DESC
    LIMIT 1
    """
    current_settings = snowflake_conn.execute_query(query)
    
    # Convert Snowflake row to dictionary
    if current_settings:
        try:
            # Try accessing as dictionary
            settings = dict(current_settings[0])
        except:
            try:
                # Try accessing by index if dictionary access fails
                columns = ['BUSINESS_ID', 'BUSINESS_NAME', 'LEGAL_NAME', 'TAX_ID', 
                          'STREET_ADDRESS', 'CITY', 'STATE', 'ZIP_CODE', 
                          'PHONE_NUMBER', 'EMAIL_ADDRESS', 'WEBSITE',
                          'OPERATING_HOURS_START', 'OPERATING_HOURS_END',
                          'WEEKEND_OPERATING_HOURS_START', 'WEEKEND_OPERATING_HOURS_END',
                          'ACTIVE_STATUS', 'MODIFIED_DATE']
                settings = dict(zip(columns, current_settings[0]))
            except:
                settings = {}
    else:
        settings = {}

    with st.form("business_settings_form"):
        # Company Information
        st.header("Company Details")
        col1, col2 = st.columns(2)
        
        with col1:
            business_name = st.text_input(
                "Business Name",
                value=str(settings.get('BUSINESS_NAME', ''))
            )
            legal_name = st.text_input(
                "Legal Business Name",
                value=str(settings.get('LEGAL_NAME', ''))
            )
            tax_id = st.text_input(
                "Tax ID",
                value=str(settings.get('TAX_ID', ''))
            )

        with col2:
            phone = st.text_input(
                "Business Phone",
                value=str(settings.get('PHONE_NUMBER', ''))
            )
            email = st.text_input(
                "Business Email",
                value=str(settings.get('EMAIL_ADDRESS', ''))
            )
            website = st.text_input(
                "Website",
                value=str(settings.get('WEBSITE', ''))
            )

        # Address Information
        st.header("Business Address")
        col1, col2 = st.columns(2)
        with col1:
            street_address = st.text_input(
                "Street Address",
                value=str(settings.get('STREET_ADDRESS', ''))
            )
            city = st.text_input(
                "City",
                value=str(settings.get('CITY', ''))
            )

        with col2:
            state = st.text_input(
                "State",
                value=str(settings.get('STATE', ''))
            )
            zip_code = st.text_input(
                "ZIP Code",
                value=str(settings.get('ZIP_CODE', ''))
            )

        # Operating Hours
        st.header("Operating Hours")
        
        # Default times
        default_weekday_start = time(8, 0)
        default_weekday_end = time(17, 0)
        default_weekend_start = time(9, 0)
        default_weekend_end = time(15, 0)

        try:
            current_weekday_start = time.fromisoformat(str(settings.get('OPERATING_HOURS_START', default_weekday_start)))
        except:
            current_weekday_start = default_weekday_start

        try:
            current_weekday_end = time.fromisoformat(str(settings.get('OPERATING_HOURS_END', default_weekday_end)))
        except:
            current_weekday_end = default_weekday_end

        try:
            current_weekend_start = time.fromisoformat(str(settings.get('WEEKEND_OPERATING_HOURS_START', default_weekend_start)))
        except:
            current_weekend_start = default_weekend_start

        try:
            current_weekend_end = time.fromisoformat(str(settings.get('WEEKEND_OPERATING_HOURS_END', default_weekend_end)))
        except:
            current_weekend_end = default_weekend_end
        
        # Weekday Hours
        st.subheader("Weekday Hours")
        col1, col2 = st.columns(2)
        with col1:
            weekday_start = st.time_input(
                "Opening Time",
                value=current_weekday_start
            )
        with col2:
            weekday_end = st.time_input(
                "Closing Time",
                value=current_weekday_end
            )

        # Weekend Hours
        st.subheader("Weekend Hours")
        col1, col2 = st.columns(2)
        with col1:
            weekend_start = st.time_input(
                "Weekend Opening Time",
                value=current_weekend_start
            )
        with col2:
            weekend_end = st.time_input(
                "Weekend Closing Time",
                value=current_weekend_end
            )

        # Submit button
        submitted = st.form_submit_button("Save Business Information")
        
        if submitted:
            try:
                # Prepare update/insert query
                if settings.get('BUSINESS_ID'):
                    query = """
                    UPDATE OPERATIONAL.CARPET.BUSINESS_INFO
                    SET BUSINESS_NAME = :1,
                        LEGAL_NAME = :2,
                        TAX_ID = :3,
                        STREET_ADDRESS = :4,
                        CITY = :5,
                        STATE = :6,
                        ZIP_CODE = :7,
                        PHONE_NUMBER = :8,
                        EMAIL_ADDRESS = :9,
                        WEBSITE = :10,
                        OPERATING_HOURS_START = :11,
                        OPERATING_HOURS_END = :12,
                        WEEKEND_OPERATING_HOURS_START = :13,
                        WEEKEND_OPERATING_HOURS_END = :14,
                        MODIFIED_DATE = CURRENT_TIMESTAMP()
                    WHERE BUSINESS_ID = :15
                    """
                    params = [
                        business_name, legal_name, tax_id,
                        street_address, city, state,
                        zip_code if zip_code.strip() else None,
                        phone, email, website,
                        weekday_start.strftime('%H:%M'),
                        weekday_end.strftime('%H:%M'),
                        weekend_start.strftime('%H:%M'),
                        weekend_end.strftime('%H:%M'),
                        settings['BUSINESS_ID']
                    ]
                else:
                    query = """
                    INSERT INTO OPERATIONAL.CARPET.BUSINESS_INFO (
                        BUSINESS_NAME, LEGAL_NAME, TAX_ID,
                        STREET_ADDRESS, CITY, STATE, ZIP_CODE,
                        PHONE_NUMBER, EMAIL_ADDRESS, WEBSITE,
                        OPERATING_HOURS_START, OPERATING_HOURS_END,
                        WEEKEND_OPERATING_HOURS_START, WEEKEND_OPERATING_HOURS_END
                    ) VALUES (
                        :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14
                    )
                    """
                    params = [
                        business_name, legal_name, tax_id,
                        street_address, city, state,
                        zip_code if zip_code.strip() else None,
                        phone, email, website,
                        weekday_start.strftime('%H:%M'),
                        weekday_end.strftime('%H:%M'),
                        weekend_start.strftime('%H:%M'),
                        weekend_end.strftime('%H:%M')
                    ]

                snowflake_conn.execute_query(query, params)
                st.success("Business information updated successfully!")
                st.rerun()

            except Exception as e:
                st.error(f"Error saving business information: {str(e)}")