# utils/service_utils.py
"""
Utility functions for service management that can be used across the application.
"""

import streamlit as st
from typing import Optional, Dict, Any
from database.connection import snowflake_conn

def create_new_service(
    service_name: str,
    service_category: str,
    service_description: str,
    cost: float,
    service_duration: int = 60,
    active_status: bool = True,
    customer_bookable: bool = False
) -> Optional[int]:
    """
    Create a new service in the database.
    
    Args:
        service_name: Name of the service
        service_category: Category (Carpet Cleaning, Deep Cleaning, etc.)
        service_description: Description of the service
        cost: Service cost
        service_duration: Duration in minutes (default 60)
        active_status: Whether service is active (default True)
        customer_bookable: Whether customers can book this service (default False)
    
    Returns:
        SERVICE_ID of the created service, or None if failed
    """
    try:
        # Check if service name already exists
        check_query = """
        SELECT COUNT(*) as count 
        FROM OPERATIONAL.CARPET.SERVICES 
        WHERE UPPER(SERVICE_NAME) = UPPER(?)
        """
        result = snowflake_conn.execute_query(check_query, [service_name])
        if result and result[0]['COUNT'] > 0:
            st.error(f"⚠️ Service '{service_name}' already exists. Please choose a different name.")
            return None
        
        # Insert new service
        insert_query = """
        INSERT INTO OPERATIONAL.CARPET.SERVICES (
            SERVICE_NAME,
            SERVICE_CATEGORY,
            SERVICE_DESCRIPTION,
            COST,
            ACTIVE_STATUS,
            SERVICE_DURATION,
            CUSTOMER_BOOKABLE
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        snowflake_conn.execute_query(insert_query, [
            service_name,
            service_category,
            service_description,
            float(cost),
            active_status,
            int(service_duration),
            customer_bookable
        ])
        
        # Get the created service ID
        id_query = """
        SELECT SERVICE_ID 
        FROM OPERATIONAL.CARPET.SERVICES 
        WHERE SERVICE_NAME = ? 
        ORDER BY SERVICE_ID DESC 
        LIMIT 1
        """
        result = snowflake_conn.execute_query(id_query, [service_name])
        
        if result:
            service_id = result[0]['SERVICE_ID']
            st.success(f"Service '{service_name}' created successfully!")
            return service_id
        else:
            st.error("Failed to retrieve new service ID")
            return None
            
    except Exception as e:
        st.error(f"Error creating service: {str(e)}")
        return None

def get_service_categories() -> list:
    """Get list of existing service categories."""
    try:
        query = """
        SELECT DISTINCT SERVICE_CATEGORY 
        FROM OPERATIONAL.CARPET.SERVICES 
        WHERE SERVICE_CATEGORY IS NOT NULL 
        ORDER BY SERVICE_CATEGORY
        """
        result = snowflake_conn.execute_query(query)
        
        categories = [row['SERVICE_CATEGORY'] for row in result] if result else []
        
        # Add common categories if not present
        default_categories = [
            'Carpet Cleaning',
            'Deep Cleaning',
            'Upholstery Cleaning',
            'Area Rug Cleaning',
            'Tile & Grout Cleaning',
            'Pet Odor Treatment',
            'Stain Removal',
            'Other'
        ]
        
        for category in default_categories:
            if category not in categories:
                categories.append(category)
        
        return sorted(categories)
        
    except Exception as e:
        st.error(f"Error fetching service categories: {str(e)}")
        return ['Carpet Cleaning', 'Deep Cleaning', 'Other']

def display_create_service_form(key_suffix: str = "") -> Optional[Dict[str, Any]]:
    """
    Display a form for creating a new service.
    
    Args:
        key_suffix: Suffix to make form keys unique
    
    Returns:
        Dict with service data if form was submitted successfully, None otherwise
    """
    categories = get_service_categories()
    
    with st.form(f"create_service_form_{key_suffix}"):
        st.markdown("#### Create New Service")
        
        col1, col2 = st.columns(2)
        
        with col1:
            service_name = st.text_input(
                "Service Name*",
                help="Enter the name of the new service"
            )
            service_category = st.selectbox(
                "Category*",
                options=categories
            )
            service_duration = st.number_input(
                "Duration (minutes)*",
                min_value=15,
                max_value=480,
                value=60,
                step=15,
                help="Estimated time to complete this service"
            )
        
        with col2:
            cost = st.number_input(
                "Cost*",
                min_value=0.0,
                step=5.0,
                format="%.2f",
                help="Base cost for this service"
            )
            customer_bookable = st.checkbox(
                "Customer Bookable",
                value=False,
                help="Allow customers to book this service through the customer portal"
            )
        
        service_description = st.text_area(
            "Description",
            help="Optional description of what this service includes"
        )
        
        col1, col2 = st.columns(2)
        
        with col1:
            create_button = st.form_submit_button(
                "Create Service",
                type="primary",
                use_container_width=True
            )
        
        with col2:
            cancel_button = st.form_submit_button(
                "Cancel",
                use_container_width=True
            )
        
        if cancel_button:
            return "cancelled"
        
        if create_button:
            # Validate required fields
            if not service_name:
                st.error("Service name is required")
                return None
            
            if not service_name.strip():
                st.error("Service name cannot be empty")
                return None
            
            if len(service_name.strip()) > 100:
                st.error("Service name must be 100 characters or less")
                return None
            
            if not service_category:
                st.error("Service category is required")
                return None
            
            if cost <= 0:
                st.error("Cost must be greater than 0")
                return None
            
            if cost > 10000:
                st.error("Cost cannot exceed $10,000")
                return None
            
            # Create the service
            service_id = create_new_service(
                service_name=service_name,
                service_category=service_category,
                service_description=service_description or "",
                cost=cost,
                service_duration=service_duration,
                active_status=True,
                customer_bookable=customer_bookable
            )
            
            if service_id:
                return {
                    'service_id': service_id,
                    'service_name': service_name,
                    'service_category': service_category,
                    'service_description': service_description,
                    'cost': cost,
                    'service_duration': service_duration,
                    'customer_bookable': customer_bookable
                }
        
        return None