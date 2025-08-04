# /pages/business/auth/admin_setup.py
import streamlit as st
from utils.business.business_auth import create_business_user, validate_password

def setup_admin_page():
    st.title("Setup Business Admin")
    
    with st.form("admin_setup"):
        employee_id = st.number_input("Employee ID", min_value=1)
        email = st.text_input("Admin Email")
        password = st.text_input("Password", type="password")
        confirm_password = st.text_input("Confirm Password", type="password")
        
        if st.form_submit_button("Create Admin"):
            # Validate inputs
            if not employee_id or not email or not password:
                st.error("All fields are required")
                return
                
            if password != confirm_password:
                st.error("Passwords do not match")
                return
                
            # Validate password strength  
            valid, message = validate_password(password)
            if not valid:
                st.error(message)
                return
                
            # Create admin user
            portal_user_id = create_business_user(
                employee_id, email, password, is_admin=True
            )
            
            if portal_user_id:
                st.success("Admin user created successfully!")
            else:
                st.error("Failed to create admin user")