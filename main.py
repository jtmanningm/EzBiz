import streamlit as st

# Set page configuration must be the first Streamlit command
st.set_page_config(
    page_title="EZ Biz",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="collapsed"
)

from datetime import datetime
import sys
import os

# Add project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.append(project_root)

from database.connection import snowflake_conn
from config.settings import load_css

# Import unified login
from pages.auth.unified_login import unified_login_page
from pages.portal.auth.register import register_customer_page
from pages.portal.auth.reset import reset_password_handler

# Business portal imports
from pages.new_service import new_service_page
from pages.scheduled import scheduled_services_page
from pages.completed import completed_services_page
from pages.transaction_details import transaction_details_page

# Customer portal imports
from pages.portal.auth.register import register_customer_page
from pages.portal.home import show_customer_portal
from pages.portal.services.book import book_service_page
from pages.portal.services.history import service_history_page
from pages.portal.services.upcoming import upcoming_services_page
from pages.portal.account.profile import profile_page

# Settings imports
from pages.settings import (
    business_settings_page,
    services_settings_page,
    employees_settings_page,
    accounts_settings_page,
    customer_communications_page,
    pricing_settings_page
)

# Import middleware
from utils.auth.middleware import (
    init_customer_session,
    is_customer_authenticated,
    clear_customer_session
)

from utils.business.business_auth import verify_business_session


def get_business_name() -> str:
    """Fetch business name from database"""
    try:
        query = """
        SELECT BUSINESS_NAME 
        FROM OPERATIONAL.CARPET.BUSINESS_INFO 
        WHERE BUSINESS_NAME IS NOT NULL
        LIMIT 1
        """
        result = snowflake_conn.execute_query(query)
        if result and len(result) > 0:
            return result[0]['BUSINESS_NAME']
        return None
    except Exception as e:
        print(f"Error fetching business name: {str(e)}")
        return None

def is_mobile():
    """Helper function to detect mobile devices"""
    return st.session_state.get('_is_mobile', 
           st.query_params.get('mobile', [False])[0])

def initialize_session_state():
    """Initialize session state variables"""
    if 'page' not in st.session_state:
        st.session_state['page'] = 'login'
    if 'show_settings' not in st.session_state:
        st.session_state['show_settings'] = False
    if 'settings_page' not in st.session_state:
        st.session_state['settings_page'] = 'business'

def display_customer_navigation():
    """Display customer portal navigation"""
    with st.container():
        col1, col2, col3, col4, col5 = st.columns([2,2,2,2,1])
        
        with col1:
            if st.button("🏠 Home", use_container_width=True):
                st.session_state.page = 'portal_home'
                st.rerun()
        with col2:
            if st.button("📅 Book Service", use_container_width=True):
                st.session_state.page = 'book_service'
                st.rerun()
        with col3:
            if st.button("⏳ Upcoming", use_container_width=True):
                st.session_state.page = 'upcoming_services'
                st.rerun()
        with col4:
            if st.button("📋 History", use_container_width=True):
                st.session_state.page = 'service_history'
                st.rerun()
        with col5:
            if st.button("⚙️", use_container_width=True):
                st.session_state.page = 'profile'
                st.rerun()

def display_customer_portal():
    """Display customer portal interface"""
    # Initialize customer session
    init_customer_session()
    
    # Get business name for header
    business_name = get_business_name()
    header_text = f"{business_name} Customer Portal" if business_name else "Customer Portal"
    
    st.title(header_text)
    
    # Show navigation if authenticated
    if is_customer_authenticated():
        display_customer_navigation()
    
    # Route to appropriate page
    pages = {
        'register': register_customer_page,
        'portal_home': show_customer_portal,
        'book_service': book_service_page,
        'service_history': service_history_page,
        'upcoming_services': upcoming_services_page,
        'profile': profile_page
    }
    
    current_page = st.session_state.get('page', 'login')
    if current_page in pages:
        pages[current_page]()
    
    # Show logout button if authenticated
    if is_customer_authenticated():
        st.markdown("---")
        if st.button("Logout", key="logout_btn", use_container_width=True):
            clear_customer_session("User logout")
            st.session_state.page = 'login'
            st.rerun()

def display_main_menu():
    """Display the main menu with primary action buttons"""
    st.title("Ez Biz Management Portal")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("📝 New Service", use_container_width=True):
            st.session_state.show_settings = False  # Reset settings state
            st.session_state.page = 'new_service'
            st.rerun()
    
    with col2:
        if st.button("📅 Scheduled Services", use_container_width=True):
            st.session_state.show_settings = False  # Reset settings state
            st.session_state.page = 'scheduled_services'
            st.rerun()
    
    with col3:
        if st.button("✓ Completed Services", use_container_width=True):
            st.session_state.show_settings = False  # Reset settings state
            st.session_state.page = 'completed_services'
            st.rerun()

def display_settings_menu():
    """Display the settings menu when in settings mode"""
    # Add back to home button at the top
    col1, _ = st.columns([1, 11])
    with col1:
        if st.button("← Home"):
            st.session_state.show_settings = False  # Reset settings mode
            st.session_state.page = None  # Return to landing page
            st.rerun()
    st.markdown("---")

    st.title("Settings")
    
    if is_mobile():
        selected = st.radio(
            "",
            options=[
                "Business Info",
                "Services", 
                "Employees",
                "Accounts",
                "Customer Communications",
                "Pricing"
            ]
        )
    else:
        col1, _ = st.columns([1, 3])  # Use _ to indicate intentionally unused variable
        with col1:
            selected = st.radio(
                "",
                options=[
                    "Business Info",
                    "Services",
                    "Employees", 
                    "Accounts",
                    "Customer Communications",
                    "Pricing"   
                ]
            )
    
    # Map selected option to page
    page_mapping = {
        "Business Info": "business",
        "Services": "services",
        "Employees": "employees",
        "Accounts": "accounts", 
        "Customer Communications": "communications",
        "Pricing": "pricing"
    }
    
    st.session_state.settings_page = page_mapping[selected]
    
    # Display selected settings page
    settings_pages = {
        'business': business_settings_page,
        'services': services_settings_page,
        'employees': employees_settings_page,
        'accounts': accounts_settings_page,
        'communications': customer_communications_page,
        'pricing': pricing_settings_page
    }
    
    if st.session_state.settings_page in settings_pages:
        settings_pages[st.session_state.settings_page]()

def display_business_portal():
    """Display business portal interface"""
    # Check authentication first before creating any UI elements
    authenticated = verify_business_session(st.session_state.get('business_session_id'))
    
    if authenticated:
        # Handle settings mode
        show_settings = st.session_state.get('show_settings', False)
        
        if show_settings:
            # We're in settings mode
            display_settings_menu()
        else:
            # Regular mode - create a container for the header
            header_container = st.container()
            
            # Add settings button with a fixed layout
            st.write('<div style="text-align: right;">', unsafe_allow_html=True)
            if st.button("⚙️", help="Settings", key="settings_gear_button"):
                st.session_state.show_settings = True
                st.rerun()
            st.write('</div>', unsafe_allow_html=True)
            
            # Main content based on current page
            current_page = st.session_state.get('page')
            
            # Map pages to their corresponding functions
            pages = {
                'new_service': new_service_page,
                'scheduled_services': scheduled_services_page,
                'completed_services': completed_services_page,
                'transaction_details': transaction_details_page
            }
            
            if current_page in pages:
                pages[current_page]()
            else:
                display_main_menu()
        
        # Show logout button
        st.markdown("---")
        if st.button("Logout", key="business_logout"):
            if 'business_session_id' in st.session_state:
                del st.session_state['business_session_id']
            st.session_state.page = 'login'
            st.rerun()
    else:
        # Show login if not authenticated
        unified_login_page()

def main():
    initialize_session_state()
    load_css()
    
    # Define auth pages
    auth_pages = {
        'login': unified_login_page,
        'register': register_customer_page,
        'reset': reset_password_handler
    }
    
    current_page = st.session_state.get('page', 'login')
    
    # Route to appropriate page
    if current_page in auth_pages:
        # Handle authentication pages
        auth_pages[current_page]()
    elif 'business_session_id' in st.session_state:
        # Handle business portal
        display_business_portal()
    elif 'customer_session_id' in st.session_state:
        # Handle customer portal
        display_customer_portal()
    else:
        # Default to login page
        unified_login_page()

if __name__ == "__main__":
    main()
