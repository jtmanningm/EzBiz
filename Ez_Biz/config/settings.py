import streamlit as st
from typing import Dict, Any, List

# Service Categories
SERVICE_CATEGORIES = [
    "Carpet Cleaning",
    "Upholstery Cleaning",
    "Tile & Grout Cleaning",
    "Area Rug Cleaning",
    "Pet Stain Treatment",
    "Water Damage Restoration",
    "Odor Removal",
    "Commercial Cleaning"
]

# Employee Settings
JOB_TITLES = [
    "Technician",
    "Senior Technician",
    "Supervisor",
    "Manager",
    "Customer Service Representative",
    "Sales Representative",
    "Administrative Assistant"
]

DEPARTMENTS = [
    "Operations",
    "Customer Service",
    "Sales",
    "Administration",
    "Management"
]

def configure_page():
    """Configure basic Streamlit page settings"""
    st.set_page_config(
        page_title="Ez Biz",
        page_icon="💼",
        layout="wide",
        initial_sidebar_state="expanded"
    )

def load_css():
    """Load custom CSS styles"""
    st.markdown("""
        <style>
        /* Hide default Streamlit navigation menu */
        #MainMenu {visibility: hidden;}
        header {visibility: hidden;}
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {padding-top: 0;}
        section[data-testid="stSidebarContent"] > div:first-child {display: none;}
        .css-1q1n0ol.egzxvld0 {display: none;}
        
        /* Your existing CSS styles */
        .stButton button {
            width: 100%;
        }
        </style>
    """, unsafe_allow_html=True)

def initialize_session_state():
    """Initialize all session state variables"""
    defaults: Dict[str, Any] = {
        'page': 'service_selection',
        'show_settings': False,
        'settings_page': 'business',
        'customer_details': {},
        'selected_services': [],
        'selected_employees': [],
        'selected_customer_id': None,
        'service_start_time': None,
        'selected_service': None,
        'deposit_confirmation_state': None
    }
    
    for key, default_value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value