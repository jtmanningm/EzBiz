import streamlit as st
from database.connection import snowflake_conn
from config.settings import SERVICE_CATEGORIES
from utils.formatting import format_currency

def services_settings_page():
    """Services management settings page"""
    st.title("Services Management")

    tab1, tab2 = st.tabs(["Current Services", "Add New Service"])

    # Current Services Tab
    with tab1:
        st.header("Manage Services")
        
        # Fetch existing services
        query = """
        SELECT 
            SERVICE_ID,
            SERVICE_NAME,
            SERVICE_CATEGORY,
            SERVICE_DESCRIPTION,
            COST,
            ACTIVE_STATUS
        FROM OPERATIONAL.CARPET.SERVICES
        ORDER BY SERVICE_CATEGORY, SERVICE_NAME
        """
        services = snowflake_conn.execute_query(query)

        # Group services by category
        for category in SERVICE_CATEGORIES:
            st.subheader(category)
            category_services = [s for s in services if s['SERVICE_CATEGORY'] == category]
            
            for service in category_services:
                with st.expander(f"{service['SERVICE_NAME']} - {format_currency(service['COST'])}"):
                    with st.form(f"service_form_{service['SERVICE_ID']}"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            new_name = st.text_input(
                                "Service Name",
                                value=service['SERVICE_NAME']
                            )
                            new_category = st.selectbox(
                                "Category",
                                SERVICE_CATEGORIES,
                                index=SERVICE_CATEGORIES.index(service['SERVICE_CATEGORY'])
                            )
                            
                        with col2:
                            new_cost = st.number_input(
                                "Cost",
                                value=float(service['COST']),
                                min_value=0.0,
                                step=5.0
                            )
                            new_status = st.checkbox(
                                "Active",
                                value=service['ACTIVE_STATUS']
                            )
                        
                        new_description = st.text_area(
                            "Description",
                            value=service['SERVICE_DESCRIPTION'] if service['SERVICE_DESCRIPTION'] else ""
                        )

                        if st.form_submit_button("Update Service"):
                            update_query = """
                            UPDATE OPERATIONAL.CARPET.SERVICES
                            SET SERVICE_NAME = :1,
                                SERVICE_CATEGORY = :2,
                                SERVICE_DESCRIPTION = :3,
                                COST = :4,
                                ACTIVE_STATUS = :5,
                                MODIFIED_DATE = CURRENT_TIMESTAMP()
                            WHERE SERVICE_ID = :6
                            """
                            try:
                                snowflake_conn.execute_query(update_query, [
                                    new_name, new_category, new_description,
                                    new_cost, new_status, service['SERVICE_ID']
                                ])
                                st.success("Service updated successfully!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error updating service: {str(e)}")

    # Add New Service Tab
    with tab2:
        st.header("Add New Service")
        
        with st.form("new_service_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                service_name = st.text_input("Service Name")
                service_category = st.selectbox("Category", SERVICE_CATEGORIES)
                
            with col2:
                cost = st.number_input(
                    "Cost",
                    min_value=0.0,
                    step=5.0
                )
                active_status = st.checkbox("Active", value=True)
            
            service_description = st.text_area("Description")

            if st.form_submit_button("Add Service"):
                if not service_name:
                    st.error("Service name is required")
                    return
                    
                insert_query = """
                INSERT INTO OPERATIONAL.CARPET.SERVICES (
                    SERVICE_NAME,
                    SERVICE_CATEGORY,
                    SERVICE_DESCRIPTION,
                    COST,
                    ACTIVE_STATUS
                ) VALUES (:1, :2, :3, :4, :5)
                """
                try:
                    snowflake_conn.execute_query(insert_query, [
                        service_name, service_category, service_description,
                        cost, active_status
                    ])
                    st.success("New service added successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error adding service: {str(e)}")