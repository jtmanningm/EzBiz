import streamlit as st
from database.connection import snowflake_conn
from config.settings import SERVICE_CATEGORIES
from utils.formatting import format_currency

def services_settings_page():
    """Services management settings page"""
    st.title("Services Management")

    # Initialize session state for editing
    if 'editing_service' not in st.session_state:
        st.session_state.editing_service = None

    tab1, tab2 = st.tabs(["Current Services", "Add New Service"])

    # Current Services Tab
    with tab1:
        # Fetch existing services
        query = """
        SELECT 
            SERVICE_ID,
            SERVICE_NAME,
            SERVICE_CATEGORY,
            SERVICE_DESCRIPTION,
            COST,
            ACTIVE_STATUS,
            COALESCE(SERVICE_DURATION, 60) as SERVICE_DURATION
        FROM OPERATIONAL.CARPET.SERVICES
        ORDER BY SERVICE_CATEGORY, SERVICE_NAME
        """
        
        try:
            services = snowflake_conn.execute_query(query)
            if not services:
                st.warning("No services found in the database.")
                return

            # Direct display of services grouped by category
            unique_categories = list(set(service['SERVICE_CATEGORY'] for service in services))
            
            for category in unique_categories:
                st.subheader(category)
                category_services = [s for s in services if s['SERVICE_CATEGORY'] == category]
                
                for service in category_services:
                    status = "🟢" if service['ACTIVE_STATUS'] else "🔴"
                    if st.button(
                        f"{status} {service['SERVICE_NAME']} - {format_currency(service['COST'])}",
                        key=f"edit_button_{service['SERVICE_ID']}",
                        use_container_width=True
                    ):
                        st.session_state.editing_service = service['SERVICE_ID']
                    
                    if st.session_state.get('editing_service') == service['SERVICE_ID']:
                        with st.form(key=f"edit_form_{service['SERVICE_ID']}"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                new_name = st.text_input(
                                    "Service Name",
                                    value=service['SERVICE_NAME']
                                )
                                new_category = st.selectbox(
                                    "Category",
                                    unique_categories,
                                    index=unique_categories.index(service['SERVICE_CATEGORY'])
                                )
                                new_duration = st.number_input(
                                    "Duration (minutes)",
                                    min_value=30,
                                    max_value=480,
                                    value=int(service['SERVICE_DURATION']),
                                    step=15
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

                            col1, col2 = st.columns(2)
                            with col1:
                                if st.form_submit_button("Save Changes", use_container_width=True):
                                    update_query = """
                                    UPDATE OPERATIONAL.CARPET.SERVICES
                                    SET SERVICE_NAME = ?,
                                        SERVICE_CATEGORY = ?,
                                        SERVICE_DESCRIPTION = ?,
                                        COST = ?,
                                        ACTIVE_STATUS = ?,
                                        SERVICE_DURATION = ?,
                                        MODIFIED_DATE = CURRENT_TIMESTAMP()
                                    WHERE SERVICE_ID = ?
                                    """
                                    try:
                                        snowflake_conn.execute_query(update_query, [
                                            new_name, new_category, new_description,
                                            new_cost, new_status, new_duration,
                                            service['SERVICE_ID']
                                        ])
                                        st.success("Service updated successfully!")
                                        st.session_state.editing_service = None
                                        st.rerun()
                                    except Exception as e:
                                        st.error(f"Error updating service: {str(e)}")
                            
                            with col2:
                                if st.form_submit_button("Cancel", use_container_width=True):
                                    st.session_state.editing_service = None
                                    st.rerun()

        except Exception as e:
            st.error(f"Error fetching services: {str(e)}")
            return

    # Add New Service Tab
    with tab2:
        st.header("Add New Service")
        
        with st.form("new_service_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                service_name = st.text_input("Service Name")
                service_category = st.selectbox("Category", unique_categories if 'unique_categories' in locals() else SERVICE_CATEGORIES)
                service_duration = st.number_input(
                    "Duration (minutes)",
                    min_value=30,
                    max_value=480,
                    value=60,
                    step=15
                )
                
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
                else:
                    insert_query = """
                    INSERT INTO OPERATIONAL.CARPET.SERVICES (
                        SERVICE_NAME,
                        SERVICE_CATEGORY,
                        SERVICE_DESCRIPTION,
                        COST,
                        ACTIVE_STATUS,
                        SERVICE_DURATION
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """
                    try:
                        snowflake_conn.execute_query(insert_query, [
                            service_name, service_category, service_description,
                            cost, active_status, service_duration
                        ])
                        st.success("New service added successfully!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error adding service: {str(e)}")

if __name__ == "__main__":
    services_settings_page()