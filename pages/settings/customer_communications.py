import streamlit as st
from database.connection import snowflake_conn
from datetime import datetime

def customer_communications_page():
    """Customer communication and settings management page"""
    st.title("Customer Communications")

    tabs = st.tabs([
        "Message Templates", 
        "Automation Settings", 
        "Marketing Campaigns",
        "Customer Preferences"
    ])

    # Message Templates Tab
    with tabs[0]:
        st.header("Message Templates")
        
        # Show existing templates
        view_templates = st.expander("View Existing Templates", expanded=True)
        with view_templates:
            query = """
            SELECT *
            FROM OPERATIONAL.CARPET.MESSAGE_TEMPLATES
            ORDER BY IS_ACTIVE DESC, MODIFIED_AT DESC
            """
            templates = snowflake_conn.execute_query(query)
            
            if templates:
                for template in templates:
                    with st.expander(
                        f"{'✓ ' if template['IS_ACTIVE'] else '❌ '}{template['TEMPLATE_NAME']} ({template['TEMPLATE_TYPE']})"
                    ):
                        st.write(f"**Content:** {template['TEMPLATE_CONTENT']}")
                        st.write(f"**Delivery Channels:** {template['DELIVERY_CHANNELS']}")
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button(
                                "Toggle Active Status", 
                                key=f"toggle_{template['TEMPLATE_ID']}"
                            ):
                                try:
                                    update_query = """
                                    UPDATE OPERATIONAL.CARPET.MESSAGE_TEMPLATES
                                    SET IS_ACTIVE = NOT IS_ACTIVE,
                                        MODIFIED_AT = CURRENT_TIMESTAMP()
                                    WHERE TEMPLATE_ID = :1
                                    """
                                    snowflake_conn.execute_query(update_query, [template['TEMPLATE_ID']])
                                    st.success("Template status updated!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error updating template: {str(e)}")

        # Create new template
        create_template = st.expander("Create New Template", expanded=False)
        with create_template:
            with st.form("template_form"):
                template_type = st.selectbox(
                    "Template Type",
                    ["Service Confirmation", "Appointment Reminder", "Follow-up",
                     "Promotion", "Holiday Hours", "Newsletter", "Service Update"]
                )
                
                template_name = st.text_input("Template Name")
                
                template_content = st.text_area(
                    "Template Content",
                    height=200,
                    help="Use {customer_name}, {service_date}, {service_time}, etc. for dynamic content"
                )
                
                delivery_channels = st.selectbox(
                    "Delivery Channels",
                    ["Email", "SMS", "Both"]
                )
                
                if st.form_submit_button("Save Template"):
                    if not template_name or not template_content:
                        st.error("Please fill in all required fields")
                    else:
                        try:
                            query = """
                            INSERT INTO OPERATIONAL.CARPET.MESSAGE_TEMPLATES (
                                TEMPLATE_TYPE, TEMPLATE_NAME, TEMPLATE_CONTENT,
                                DELIVERY_CHANNELS, IS_ACTIVE
                            ) VALUES (
                                :1, :2, :3, :4, TRUE
                            )
                            """
                            params = [
                                template_type,
                                template_name,
                                template_content,
                                delivery_channels
                            ]
                            snowflake_conn.execute_query(query, params)
                            st.success("Template created successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error creating template: {str(e)}")

    # Automation Settings Tab
    with tabs[1]:
        st.header("Automation Settings")
        
        # Get current settings
        current_settings = snowflake_conn.execute_query("""
            SELECT *
            FROM OPERATIONAL.CARPET.AUTOMATION_SETTINGS
            ORDER BY CREATED_AT DESC
            LIMIT 1
        """)
        
        default_settings = current_settings[0] if current_settings else {
            'REMIND_DAYS': 2,
            'REMIND_HOURS': 24,
            'FOLLOWUP_DAYS': 3
        }
        
        with st.form("automation_form"):
            remind_days = st.number_input(
                "Reminder Days Before Appointment",
                min_value=0,
                value=int(default_settings['REMIND_DAYS'])
            )
            
            remind_hours = st.number_input(
                "Reminder Hours Before Appointment",
                min_value=0,
                value=int(default_settings['REMIND_HOURS'])
            )
            
            followup_days = st.number_input(
                "Follow-up Days After Service",
                min_value=0,
                value=int(default_settings['FOLLOWUP_DAYS'])
            )
            
            if st.form_submit_button("Save Settings"):
                try:
                    query = """
                    INSERT INTO OPERATIONAL.CARPET.AUTOMATION_SETTINGS (
                        REMIND_DAYS, REMIND_HOURS, FOLLOWUP_DAYS
                    ) VALUES (
                        :1, :2, :3
                    )
                    """
                    params = [remind_days, remind_hours, followup_days]
                    snowflake_conn.execute_query(query, params)
                    st.success("Settings saved successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error saving settings: {str(e)}")

    # Marketing Campaigns Tab
    with tabs[2]:
        st.header("Marketing Campaigns")
        
        # View existing campaigns
        view_campaigns = st.expander("View Active Campaigns", expanded=True)
        with view_campaigns:
            query = """
            SELECT *
            FROM OPERATIONAL.CARPET.MARKETING_CAMPAIGNS
            WHERE END_DATE >= CURRENT_DATE()
            ORDER BY START_DATE
            """
            campaigns = snowflake_conn.execute_query(query)
            
            if campaigns:
                for campaign in campaigns:
                    with st.expander(f"{campaign['CAMPAIGN_NAME']} ({campaign['CAMPAIGN_TYPE']})"):
                        st.write(f"**Period:** {campaign['START_DATE']} to {campaign['END_DATE']}")
                        st.write(f"**Target:** {campaign['TARGET_AUDIENCE']}")
                        st.write(f"**Message:** {campaign['MESSAGE']}")
            else:
                st.info("No active campaigns found")

        # Create new campaign
        create_campaign = st.expander("Create New Campaign", expanded=False)
        with create_campaign:
            with st.form("campaign_form"):
                campaign_name = st.text_input("Campaign Name")
                campaign_type = st.selectbox(
                    "Campaign Type",
                    ["Promotion", "New Service", "Holiday Special", "Newsletter", "Service Update"]
                )
                
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input("Start Date")
                with col2:
                    end_date = st.date_input("End Date")
                
                message = st.text_area("Campaign Message", height=200)
                target_audience = st.selectbox(
                    "Target Audience",
                    ["All Customers", "Recent Customers", "Inactive Customers", "VIP Customers"]
                )
                
                if st.form_submit_button("Create Campaign"):
                    if end_date < start_date:
                        st.error("End date must be after start date")
                    else:
                        try:
                            query = """
                            INSERT INTO OPERATIONAL.CARPET.MARKETING_CAMPAIGNS (
                                CAMPAIGN_NAME, CAMPAIGN_TYPE, START_DATE,
                                END_DATE, MESSAGE, TARGET_AUDIENCE
                            ) VALUES (
                                :1, :2, :3, :4, :5, :6
                            )
                            """
                            params = [
                                campaign_name,
                                campaign_type,
                                start_date,
                                end_date,
                                message,
                                target_audience
                            ]
                            snowflake_conn.execute_query(query, params)
                            st.success("Campaign created successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error creating campaign: {str(e)}")

    # Customer Preferences Tab
    with tabs[3]:
        st.header("Customer Communication Preferences")
        
        query = """
        SELECT 
            C.CUSTOMER_ID,
            C.FIRST_NAME || ' ' || C.LAST_NAME as CUSTOMER_NAME,
            C.EMAIL_ADDRESS,
            C.PHONE_NUMBER,
            CP.MARKETING_EMAILS,
            CP.MARKETING_SMS,
            CP.APPOINTMENT_REMINDERS,
            CP.PROMOTIONAL_MESSAGES
        FROM OPERATIONAL.CARPET.CUSTOMER C
        LEFT JOIN OPERATIONAL.CARPET.CUSTOMER_PREFERENCES CP
            ON C.CUSTOMER_ID = CP.CUSTOMER_ID
        ORDER BY C.FIRST_NAME, C.LAST_NAME
        """
        
        try:
            preferences = snowflake_conn.execute_query(query)
            if preferences:
                st.dataframe(
                    preferences,
                    hide_index=True,
                    use_container_width=True
                )
            else:
                st.info("No customer preferences found")
        except Exception as e:
            st.error(f"Error loading customer preferences: {str(e)}")