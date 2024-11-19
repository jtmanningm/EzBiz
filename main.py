import streamlit as st
from datetime import datetime
from database.connection import snowflake_conn  # Make sure this doesn't call Streamlit commands
from config.settings import load_css
from models.service import (
    ServiceModel,
    fetch_services,
    fetch_upcoming_services,
    get_available_time_slots,
    check_service_availability,
    save_service_schedule,
    schedule_recurring_services,
    fetch_customer_services,
    update_service_status,
    get_service_id_by_name
)   

# Set page configuration
st.set_page_config(
    page_title="EZ Biz",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
    /* Hide specific buttons and navigation */
    .st-emotion-cache-qsoh6x {display: none !important;}
    button[kind="headerNoPadding"] {display: none !important;}
    .eyeqlp53 {display: none !important;}
    section[data-testid="stSidebarCollapsedControl"] {display: none !important;}
    
    /* Hide all default elements */
    #MainMenu {visibility: hidden;}
    header {visibility: hidden;}
    footer {visibility: hidden;}
    [data-testid="stSidebar"] {display: none !important;}
    
    /* Remove any padding/margins */
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 0rem !important;
        margin: 0;
    }
    
    /* Base container styles */
    .main-container {
        width: 100%;
        max-width: 1200px;
        margin: 0 auto;
        padding: 0 1rem;
    }
    
    /* Header styles */
    .app-header {
        text-align: center;
        padding: 1rem 0;
        position: relative;
    }
    
    .app-header h1 {
        font-size: 2rem;
        margin: 0;
    }
    
    .app-header p {
        margin: 0.5rem 0;
        opacity: 0.8;
    }
    
    /* Button styles */
    .stButton > button {
        width: 100%;
        border-radius: 8px;
        padding: 1rem;
        font-size: 1.1rem;
        margin: 0.5rem 0;
        border: 1px solid rgba(250, 250, 250, 0.2);
        background-color: rgba(250, 250, 250, 0.1);
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background-color: rgba(250, 250, 250, 0.2);
    }
    
    /* Settings icon styles */
    .settings-btn {
        position: absolute;
        top: 1rem;
        right: 1rem;
        background: transparent;
        border: none;
        font-size: 1.5rem;
        cursor: pointer;
        padding: 0.5rem;
        border-radius: 50%;
        width: 40px;
        height: 40px;
        display: flex;
        align-items: center;
        justify-content: center;
    }
    
    /* Settings menu styles */
    .settings-menu {
        display: flex;
        gap: 2rem;
        padding: 1rem;
    }
    
    /* Mobile optimizations */
    @media (max-width: 768px) {
        .app-header h1 {
            font-size: 1.75rem;
            padding-right: 3rem;  /* Make room for settings icon */
        }
        
        .app-header p {
            font-size: 1rem;
        }
        
        .stButton > button {
            padding: 0.875rem;
            font-size: 1rem;
        }
        
        /* Stack buttons vertically on mobile */
        [data-testid="column"] {
            width: 100% !important;
            margin-bottom: 0.5rem;
        }
        
        /* Adjust settings layout for mobile */
        [data-testid="stRadio"] {
            font-size: 0.9rem;
        }
        
        .settings-menu {
            flex-direction: column;
            gap: 1rem;
        }
    }
    
    /* Tablet optimizations */
    @media (min-width: 769px) and (max-width: 1024px) {
        .app-header h1 {
            font-size: 1.85rem;
        }
        
        .stButton > button {
            padding: 0.75rem;
        }
    }
</style>
""", unsafe_allow_html=True)

# Import pages
from pages.new_service import new_service_page
from pages.scheduled import scheduled_services_page
from pages.completed import completed_services_page
from pages.transaction_details import transaction_details_page

# Import settings pages
from pages.settings import (
    business_settings_page,
    services_settings_page,
    employees_settings_page,
    accounts_settings_page,
    customer_communications_page
)

def initialize_session_state():
    """Initialize session state variables"""
    if 'page' not in st.session_state:
        st.session_state['page'] = 'service_selection'
    if 'show_settings' not in st.session_state:
        st.session_state['show_settings'] = False
    if 'settings_page' not in st.session_state:
        st.session_state['settings_page'] = 'business'

def display_main_menu():
    """Display the main menu with primary action buttons"""
    st.markdown('<div class="main-container">', unsafe_allow_html=True)
    
    # For mobile, stack buttons vertically
    if is_mobile():
        if st.button("📝 New Service", 
                    key='new_service',
                    help='Schedule a new service',
                    use_container_width=True):
            st.session_state['page'] = 'new_service'
            st.rerun()
        
        if st.button("📅 Scheduled Services",
                    key='scheduled_services',
                    help='View and manage scheduled services',
                    use_container_width=True):
            st.session_state['page'] = 'scheduled_services'
            st.rerun()
        
        if st.button("✓ Completed Services",
                    key='completed_services',
                    help='View completed services and transactions',
                    use_container_width=True):
            st.session_state['page'] = 'completed_services'
            st.rerun()
    else:
        # For desktop/tablet, use columns
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("📝 New Service", 
                        key='new_service',
                        help='Schedule a new service',
                        use_container_width=True):
                st.session_state['page'] = 'new_service'
                st.rerun()
        
        with col2:
            if st.button("📅 Scheduled Services",
                        key='scheduled_services',
                        help='View and manage scheduled services',
                        use_container_width=True):
                st.session_state['page'] = 'scheduled_services'
                st.rerun()
        
        with col3:
            if st.button("✓ Completed Services",
                        key='completed_services',
                        help='View completed services and transactions',
                        use_container_width=True):
                st.session_state['page'] = 'completed_services'
                st.rerun()
    
    st.markdown('</div>', unsafe_allow_html=True)

def display_settings_menu():
    """Display the settings menu when in settings mode"""
    if is_mobile():
        # Mobile layout: Stack vertically
        st.title("Settings")
        
        selected = st.radio(
            "",
            options=[
                "Business Info",
                "Services",
                "Employees",
                "Accounts",
                "Customer Communications"
            ]
        )
        
        if st.button("← Return to Home", use_container_width=True):
            st.session_state['show_settings'] = False
            st.session_state['page'] = 'service_selection'
            st.rerun()
        
        st.markdown("<hr>", unsafe_allow_html=True)
        
    else:
        # Desktop layout: Side-by-side
        col1, col2 = st.columns([0.25, 0.75])
        
        with col1:
            st.title("Settings")
            selected = st.radio(
                "",
                options=[
                    "Business Info",
                    "Services",
                    "Employees",
                    "Accounts",
                    "Customer Communications"
                ]
            )
            
            if st.button("← Return to Home"):
                st.session_state['show_settings'] = False
                st.session_state['page'] = 'service_selection'
                st.rerun()
    
    # Common settings page handling
    page_mapping = {
        "Business Info": "business",
        "Services": "services",
        "Employees": "employees",
        "Accounts": "accounts",
        "Customer Communications": "communications"
    }
    
    st.session_state['settings_page'] = page_mapping[selected]
    
    settings_pages = {
        'business': business_settings_page,
        'services': services_settings_page,
        'employees': employees_settings_page,
        'accounts': accounts_settings_page,
        'communications': customer_communications_page
    }
    
    current_settings_page = st.session_state.get('settings_page')
    if current_settings_page in settings_pages:
        settings_pages[current_settings_page]()

def is_mobile():
    """Helper function to detect mobile devices"""
    # You can enhance this with more sophisticated detection if needed
    return st.session_state.get('_is_mobile', 
           st.query_params.get('mobile', [False])[0])

def main():
    initialize_session_state()
    
    # Display header
    st.markdown("""
        <div class="app-header">
            <h1>EZ Biz</h1>
            <p>Service Management</p>
        </div>
    """, unsafe_allow_html=True)
    
    if st.session_state['page'] == 'service_selection':
        if not st.session_state.get('show_settings', False):
            # Settings button
            cols = st.columns([0.97, 0.03])
            with cols[1]:
                if st.button("⚙️", help="Settings", key="settings_btn"):
                    st.session_state['show_settings'] = True
                    st.rerun()
            
            display_main_menu()
        else:
            display_settings_menu()
    
    elif st.session_state['page'] != 'service_selection':
        # Back button
        if st.button('← Back', use_container_width=True):
            st.session_state['page'] = 'service_selection'
            st.rerun()
        
        # Display current page
        pages = {
            'new_service': new_service_page,
            'scheduled_services': scheduled_services_page,
            'completed_services': completed_services_page,
            'transaction_details': transaction_details_page  
        }
        
        current_page = st.session_state['page']
        if current_page in pages:
            pages[current_page]()

if __name__ == "__main__":
    main()