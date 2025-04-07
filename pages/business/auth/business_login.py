import streamlit as st
from utils.business.business_auth import business_login
from utils.auth.auth_utils import check_rate_limit

def get_client_info():
    """Get client information from session state or set defaults"""
    if 'client_ip' not in st.session_state:
        st.session_state.client_ip = 'unknown'
    if 'user_agent' not in st.session_state:
        st.session_state.user_agent = 'unknown'
    return st.session_state.client_ip, st.session_state.user_agent

def business_login_page():
    st.title("Business Portal Login")
    
    client_ip, user_agent = get_client_info()
    
    with st.form("login_form"):
        email = st.text_input("Email Address")
        password = st.text_input("Password", type="password")
        submit = st.form_submit_button("Log In")
        
        if submit:
            if not email or not password:
                st.error("Please enter both email and password")
                return
                
            # Attempt login
            success, message, session_id = business_login(
                email, password, client_ip, user_agent
            )
            
            if success:
                st.session_state.business_session_id = session_id
                st.session_state.page = 'service_selection'  # Set to service_selection
                st.session_state.show_settings = False  # Ensure settings are not shown
                st.rerun()
            else:
                st.error(message)
