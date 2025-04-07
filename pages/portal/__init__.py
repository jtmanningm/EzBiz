# pages/portal/__init__.py
import streamlit as st
from utils.auth.middleware import (
    init_customer_session,
    is_customer_authenticated,
    clear_customer_session
)

def init_portal():
    """Initialize customer portal"""
    # Initialize session state
    init_customer_session()
    
    # Define available portal pages
    portal_pages = {
        # Account Management
        'portal_home': 'pages.portal.home.portal_home',
        'profile': 'pages.portal.account.profile.profile_page',
        
        # Services
        'book_service': 'pages.portal.services.book.book_service_page',
        'service_history': 'pages.portal.services.history.service_history_page',
        'upcoming_services': 'pages.portal.services.upcoming.upcoming_services_page',
        
        # Authentication
        'login': 'pages.portal.auth.login.customer_login_page',
        'register': 'pages.portal.auth.register.register_customer_page',
        'reset_password': 'pages.portal.auth.reset.request_reset_page',
        'verify_email': 'pages.portal.auth.verify.verify_email_page'
    }
    
    # Handle page navigation
    def load_page(page_name: str):
        """Load specified page module"""
        import importlib
        
        try:
            # Dynamic import of the module
            module_path, function_name = portal_pages[page_name].rsplit('.', 1)
            module = importlib.import_module(module_path)
            
            # Get the page function
            page_function = getattr(module, function_name)
            
            # Run the page
            page_function()
            
        except Exception as e:
            st.error("Error loading page")
            print(f"Page load error: {str(e)}")
            
            # Fallback to home
            if page_name != 'portal_home':
                st.session_state.page = 'portal_home'
                st.rerun()

    # Portal navigation
    def render_nav():
        """Render portal navigation"""
        if is_customer_authenticated():
            # User info
            st.sidebar.write(f"Welcome, {st.session_state.get('customer_name', 'Customer')}")
            
            # Navigation links
            st.sidebar.markdown("### Menu")
            
            if st.sidebar.button("📋 Dashboard", use_container_width=True):
                st.session_state.page = 'portal_home'
                st.rerun()
                
            if st.sidebar.button("📅 Book Service", use_container_width=True):
                st.session_state.page = 'book_service'
                st.rerun()
                
            if st.sidebar.button("🗓️ Upcoming Services", use_container_width=True):
                st.session_state.page = 'upcoming_services'
                st.rerun()
                
            if st.sidebar.button("📜 Service History", use_container_width=True):
                st.session_state.page = 'service_history'
                st.rerun()
                
            if st.sidebar.button("👤 Profile & Settings", use_container_width=True):
                st.session_state.page = 'profile'
                st.rerun()
            
            # Logout button at bottom
            st.sidebar.markdown("---")
            if st.sidebar.button("Logout", type="secondary", use_container_width=True):
                clear_customer_session("User logout")
                st.session_state.page = 'login'
                st.rerun()

    # Initialize current page if not set
    if 'page' not in st.session_state:
        st.session_state.page = 'login'
    
    # Auth pages don't require login
    auth_pages = ['login', 'register', 'reset_password', 'verify_email']
    
    # Check authentication for non-auth pages
    current_page = st.session_state.get('page', 'login')
    if current_page not in auth_pages and not is_customer_authenticated():
        st.session_state.return_to = current_page
        st.session_state.page = 'login'
        st.rerun()
    
    # Render navigation for authenticated users
    if is_customer_authenticated():
        render_nav()
    
    # Load current page
    if current_page in portal_pages:
        load_page(current_page)
    else:
        st.error("Page not found")
        st.session_state.page = 'portal_home'
        st.rerun()

def get_portal_pages():
    """Get list of available portal pages"""
    return {
        'portal_home': 'Portal Home',
        'book_service': 'Book Service',
        'upcoming_services': 'Upcoming Services',
        'service_history': 'Service History',
        'profile': 'Profile & Settings'
    }

def is_portal_page(page_name: str) -> bool:
    """Check if page is a portal page"""
    return page_name in get_portal_pages()