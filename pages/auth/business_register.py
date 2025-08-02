# pages/auth/business_register.py
import streamlit as st
from database.connection import snowflake_conn
from utils.auth.auth_utils import hash_password, validate_password, validate_email
from utils.validation import validate_phone, sanitize_zip_code
import re
from typing import Optional

def validate_business_registration_data(data: dict) -> list:
    """Validate business registration form data"""
    errors = []
    
    # Required fields
    required_fields = {
        'business_name': 'Business name',
        'first_name': 'First name', 
        'last_name': 'Last name',
        'email': 'Email address',
        'phone': 'Phone number',
        'password': 'Password',
        'confirm_password': 'Password confirmation',
        'street_address': 'Street address',
        'city': 'City',
        'state': 'State',
        'zip_code': 'ZIP code'
    }
    
    for field, label in required_fields.items():
        if not data.get(field, '').strip():
            errors.append(f"{label} is required")
    
    # Email validation
    if data.get('email') and not validate_email(data['email']):
        errors.append("Invalid email format")
    
    # Phone validation  
    if data.get('phone') and not validate_phone(data['phone']):
        errors.append("Invalid phone number format")
    
    # Password validation
    if data.get('password'):
        password_errors = validate_password(data['password'])
        if password_errors:
            errors.extend(password_errors)
    
    # Password confirmation
    if data.get('password') != data.get('confirm_password'):
        errors.append("Passwords do not match")
    
    # ZIP code validation
    if data.get('zip_code'):
        zip_sanitized = sanitize_zip_code(data['zip_code'])
        if not zip_sanitized or len(zip_sanitized) != 5:
            errors.append("ZIP code must be 5 digits")
    
    return errors

def check_existing_business_user(email: str) -> bool:
    """Check if business email already exists"""
    query = """
    SELECT COUNT(*) as count 
    FROM OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS 
    WHERE EMAIL = ?
    """
    try:
        result = snowflake_conn.execute_query(query, [email.lower()])
        return result[0]['COUNT'] > 0 if result else False
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return True  # Assume exists to prevent duplicate attempts

def create_business_info(data: dict) -> Optional[int]:
    """Create business information record"""
    try:
        query = """
        INSERT INTO OPERATIONAL.CARPET.BUSINESS_INFO (
            BUSINESS_NAME,
            STREET_ADDRESS,
            CITY,
            STATE,
            ZIP_CODE,
            PHONE_NUMBER,
            EMAIL_ADDRESS,
            ACTIVE_STATUS
        ) VALUES (?, ?, ?, ?, ?, ?, ?, TRUE)
        """
        
        zip_code = sanitize_zip_code(data['zip_code'])
        
        snowflake_conn.execute_query(query, [
            data['business_name'],
            data['street_address'], 
            data['city'],
            data['state'],
            int(zip_code),
            data['phone'],
            data['email']
        ])
        
        # Get the business ID
        result = snowflake_conn.execute_query(
            "SELECT BUSINESS_ID FROM OPERATIONAL.CARPET.BUSINESS_INFO WHERE EMAIL_ADDRESS = ? ORDER BY MODIFIED_DATE DESC LIMIT 1",
            [data['email']]
        )
        
        return result[0]['BUSINESS_ID'] if result else None
        
    except Exception as e:
        st.error(f"Error creating business info: {str(e)}")
        return None

def create_employee_record(data: dict, business_id: int) -> Optional[int]:
    """Create employee record for business owner"""
    try:
        query = """
        INSERT INTO OPERATIONAL.CARPET.EMPLOYEE (
            BUSINESS_ID,
            FIRST_NAME,
            LAST_NAME,
            EMAIL_ADDRESS,
            PHONE_NUMBER,
            ROLE,
            IS_ACTIVE
        ) VALUES (?, ?, ?, ?, ?, 'Owner', TRUE)
        """
        
        snowflake_conn.execute_query(query, [
            business_id,
            data['first_name'],
            data['last_name'],
            data['email'],
            data['phone']
        ])
        
        # Get the employee ID
        result = snowflake_conn.execute_query(
            "SELECT EMPLOYEE_ID FROM OPERATIONAL.CARPET.EMPLOYEE WHERE EMAIL_ADDRESS = ? ORDER BY CREATED_AT DESC LIMIT 1",
            [data['email']]
        )
        
        return result[0]['EMPLOYEE_ID'] if result else None
        
    except Exception as e:
        st.error(f"Error creating employee record: {str(e)}")
        return None

def create_business_portal_user(data: dict, employee_id: int) -> bool:
    """Create business portal user account"""
    try:
        password_hash = hash_password(data['password'])
        
        query = """
        INSERT INTO OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS (
            EMPLOYEE_ID,
            EMAIL,
            PASSWORD_HASH,
            IS_ADMIN,
            IS_ACTIVE,
            EMAIL_VERIFIED
        ) VALUES (?, ?, ?, TRUE, TRUE, FALSE)
        """
        
        snowflake_conn.execute_query(query, [
            employee_id,
            data['email'].lower(),
            password_hash
        ])
        
        return True
        
    except Exception as e:
        st.error(f"Error creating portal user: {str(e)}")
        return False

def business_register_page():
    """Business registration page"""
    st.title("Register Your Business")
    st.markdown("Create an account to manage your carpet cleaning business with Ez Biz")
    
    with st.form("business_registration"):
        # Business Information
        st.subheader("Business Information")
        col1, col2 = st.columns(2)
        
        with col1:
            business_name = st.text_input("Business Name*")
        with col2:
            business_phone = st.text_input("Business Phone*")
        
        # Business Address
        st.subheader("Business Address")
        street_address = st.text_input("Street Address*")
        
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            city = st.text_input("City*")
        with col2:
            state = st.text_input("State*")
        with col3:
            zip_code = st.text_input("ZIP Code*")
        
        # Owner Information
        st.subheader("Business Owner Information")
        col1, col2 = st.columns(2)
        
        with col1:
            first_name = st.text_input("First Name*")
            email = st.text_input("Email Address*")
        with col2:
            last_name = st.text_input("Last Name*")
            owner_phone = st.text_input("Owner Phone*")
        
        # Account Credentials
        st.subheader("Account Credentials")
        col1, col2 = st.columns(2)
        
        with col1:
            password = st.text_input("Password*", type="password", help="Must be at least 8 characters with uppercase, lowercase, number, and special character")
        with col2:
            confirm_password = st.text_input("Confirm Password*", type="password")
        
        # Terms and Conditions
        st.subheader("Agreement")
        terms_accepted = st.checkbox("I agree to the Terms of Service and Privacy Policy*")
        
        submitted = st.form_submit_button("Register Business", type="primary", use_container_width=True)
        
        if submitted:
            if not terms_accepted:
                st.error("You must accept the Terms of Service and Privacy Policy to register")
                return
            
            # Prepare data
            registration_data = {
                'business_name': business_name,
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': owner_phone,  # Use owner phone for account
                'password': password,
                'confirm_password': confirm_password,
                'street_address': street_address,
                'city': city,
                'state': state,
                'zip_code': zip_code
            }
            
            # Validate data
            errors = validate_business_registration_data(registration_data)
            if errors:
                for error in errors:
                    st.error(error)
                return
            
            # Check for existing user
            if check_existing_business_user(email):
                st.error("An account with this email already exists")
                return
            
            try:
                # Create business records
                with st.spinner("Creating your business account..."):
                    # 1. Create business info
                    business_id = create_business_info(registration_data)
                    if not business_id:
                        st.error("Failed to create business information")
                        return
                    
                    # 2. Create employee record
                    employee_id = create_employee_record(registration_data, business_id)
                    if not employee_id:
                        st.error("Failed to create employee record")
                        return
                    
                    # 3. Create portal user
                    if not create_business_portal_user(registration_data, employee_id):
                        st.error("Failed to create portal user account")
                        return
                
                st.success("🎉 Business registration successful!")
                st.info("Your account has been created. You can now log in to access your business portal.")
                
                # Option to go to login
                if st.button("Go to Login", type="primary"):
                    st.session_state.page = 'login'
                    st.rerun()
                
            except Exception as e:
                st.error(f"Registration failed: {str(e)}")
                if st.session_state.get('debug_mode'):
                    st.exception(e)
    
    # Back to login link
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("← Back to Login", use_container_width=True):
            st.session_state.page = 'login'
            st.rerun()
    
    with col2:
        if st.button("Customer Registration", use_container_width=True):
            st.session_state.page = 'register'
            st.rerun()

if __name__ == "__main__":
    business_register_page()