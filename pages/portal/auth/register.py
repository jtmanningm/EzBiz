#pages.auth.register.py
import streamlit as st
from database.connection import snowflake_conn
from datetime import datetime
from utils.auth.auth_utils import (
    validate_password,
    hash_password,
    check_rate_limit,
    log_security_event,
    create_session
)
import re
from typing import Optional, Tuple

def get_client_info() -> Tuple[str, str]:
    """
    Get client information from session state or set defaults.
    In a production environment, you would want to implement proper
    client information gathering, possibly through a proxy or middleware.
    """
    if 'client_ip' not in st.session_state:
        st.session_state.client_ip = 'unknown'
    if 'user_agent' not in st.session_state:
        st.session_state.user_agent = 'unknown'
    
    return st.session_state.client_ip, st.session_state.user_agent

def validate_email(email: str) -> bool:
    """Validate email format using RFC 5322 standard"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def validate_phone(phone: str) -> bool:
    """
    Validate phone number format
    Accepts formats like: +1234567890, 1234567890, 123-456-7890
    """
    phone = re.sub(r'[-\s()]', '', phone)  # Remove common separators
    pattern = r'^\+?1?\d{9,15}$'
    return bool(re.match(pattern, phone))

def check_existing_customer(email: str, phone: str) -> Tuple[bool, Optional[int]]:
    """
    Check if customer already exists
    Returns: (exists: bool, customer_id: Optional[int])
    """
    query = """
    SELECT CUSTOMER_ID 
    FROM CUSTOMER 
    WHERE EMAIL_ADDRESS = :1 OR PHONE_NUMBER = :2
    """
    try:
        result = snowflake_conn.execute_query(query, [email, phone])
        if result and len(result) > 0:
            return True, result[0]['CUSTOMER_ID']
        return False, None
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return False, None

def check_existing_portal_user(email: str) -> bool:
    """Check if email is already registered in portal"""
    query = """
    SELECT COUNT(*) as count 
    FROM OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS 
    WHERE EMAIL = ? AND IS_ACTIVE = TRUE
    """
    try:
        result = snowflake_conn.execute_query(query, [email])
        return result[0]['COUNT'] > 0 if result else False
    except Exception as e:
        st.error(f"Database error: {str(e)}")
        return False

def create_customer(
    first_name: str, last_name: str, email: str, phone: str,
    street_address: str, city: str, state: str, zip_code: str,
    text_updates: bool, contact_method: str
) -> Optional[int]:
    """Create a new customer record and return the customer ID"""
    query = """
    INSERT INTO OPERATIONAL.CARPET.CUSTOMER (
        FIRST_NAME, 
        LAST_NAME, 
        EMAIL_ADDRESS, 
        PHONE_NUMBER,
        BILLING_ADDRESS, 
        BILLING_CITY, 
        BILLING_STATE, 
        BILLING_ZIP,
        TEXT_FLAG, 
        PRIMARY_CONTACT_METHOD
    ) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        snowflake_conn.execute_query(query, [
            first_name, last_name, email, phone,
            street_address, city, state, zip_code,
            text_updates, contact_method
        ])
        
        # Get new customer ID
        result = snowflake_conn.execute_query(
            "SELECT CUSTOMER_ID FROM OPERATIONAL.CARPET.CUSTOMER WHERE EMAIL_ADDRESS = ?",
            [email]
        )
        return result[0]['CUSTOMER_ID'] if result else None
    except Exception as e:
        st.error(f"Error creating customer: {str(e)}")
        return None

def register_portal_user(customer_id: int, email: str, password_hash: str) -> Optional[int]:
    """Create a new portal user and return the portal user ID"""
    portal_query = """
    INSERT INTO OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS (
        CUSTOMER_ID,
        EMAIL,
        PASSWORD_HASH,
        IS_ACTIVE,
        CREATED_AT,
        MODIFIED_AT
    )
    VALUES (?, ?, ?, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """
    try:
        snowflake_conn.execute_query(portal_query, [
            customer_id, email, password_hash
        ])
        
        # Get portal user ID
        result = snowflake_conn.execute_query(
            "SELECT PORTAL_USER_ID FROM OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS WHERE CUSTOMER_ID = ?",
            [customer_id]
        )
        return result[0]['PORTAL_USER_ID'] if result else None
    except Exception as e:
        st.error(f"Error creating portal user: {str(e)}")
        return None

# Update the registration process in register_customer_page():
# Replace the portal user creation section with:

        # Create portal user
        portal_user_id = register_portal_user(customer_id, email, hash_password(password))
        if not portal_user_id:
            st.error("Failed to create portal user")
            return

        # Create initial session
        session_id = create_session(portal_user_id, client_ip, user_agent)
        if session_id:
            # Set session state
            st.session_state.customer_session_id = session_id
            st.session_state.customer_id = customer_id
            st.session_state.portal_user_id = portal_user_id

            # Log successful registration
            log_security_event(
                portal_user_id, 'REGISTRATION_SUCCESS',
                client_ip, user_agent,
                f"New user registration: {email}"
            )

def register_customer_page():
    st.title("Customer Registration")
    
    # Get client info using the new helper function
    client_ip, user_agent = get_client_info()
    
    with st.form("registration_form"):
        # Contact Information
        st.subheader("Contact Information")
        col1, col2 = st.columns(2)
        with col1:
            first_name = st.text_input("First Name*")
            last_name = st.text_input("Last Name*")
            email = st.text_input("Email Address*")
        with col2:
            phone = st.text_input("Phone Number*")
            password = st.text_input("Password*", type="password")
            confirm_password = st.text_input("Confirm Password*", type="password")
        
        # Address Information
        st.subheader("Address")
        street_address = st.text_input("Street Address*")
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            city = st.text_input("City*")
        with col2:
            state = st.text_input("State*")
        with col3:
            zip_code = st.text_input("ZIP Code*")
            
        # Contact Preferences
        st.subheader("Contact Preferences")
        col1, col2 = st.columns(2)
        with col1:
            contact_method = st.selectbox(
                "Preferred Contact Method*",
                options=["SMS", "Phone", "Email"]
            )
        with col2:
            text_updates = st.checkbox("Opt-in to Text Messages")
        
        submit = st.form_submit_button("Register", use_container_width=True)
        
        if submit:
            # Validate required fields
            required_fields = {
                'First Name': first_name,
                'Last Name': last_name,
                'Email': email,
                'Phone': phone,
                'Password': password,
                'Confirm Password': confirm_password,
                'Street Address': street_address,
                'City': city,
                'State': state,
                'ZIP Code': zip_code
            }
            
            missing_fields = [k for k, v in required_fields.items() if not v]
            if missing_fields:
                st.error(f"Required fields missing: {', '.join(missing_fields)}")
                return

            # Validate email format
            if not validate_email(email):
                st.error("Please enter a valid email address")
                return

            # Validate phone format
            if not validate_phone(phone):
                st.error("Please enter a valid phone number")
                return

            # Validate passwords match
            if password != confirm_password:
                st.error("Passwords do not match")
                return

            # Validate password strength
            password_errors = validate_password(password)
            if password_errors:
                for error in password_errors:
                    st.error(error)
                return

            # Check rate limit for registration attempts
            rate_check, message = check_rate_limit(client_ip, 'REGISTRATION_ATTEMPT')
            if not rate_check:
                st.error(message)
                return

            # Check if email already registered
            if check_existing_portal_user(email):
                st.error("This email is already registered. Please login instead.")
                return

            try:
                # Check for existing customer
                customer_exists, customer_id = check_existing_customer(email, phone)
                
                if not customer_exists:
                    # Create new customer
                    customer_id = create_customer(
                        first_name, last_name, email, phone,
                        street_address, city, state, zip_code,
                        text_updates, contact_method
                    )
                    if not customer_id:
                        return

                # Create portal user
                portal_query = """
                INSERT INTO OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS (
                    CUSTOMER_ID, EMAIL, PASSWORD_HASH,
                    IS_ACTIVE, CREATED_AT
                )
                VALUES (?, ?, ?, TRUE, CURRENT_TIMESTAMP())
                """
                snowflake_conn.execute_query(portal_query, [
                    customer_id, email, hash_password(password)
                ])
                
                # Get portal user ID
                result = snowflake_conn.execute_query(
                    "SELECT PORTAL_USER_ID FROM OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS WHERE CUSTOMER_ID = ?",
                    [customer_id]
                )
                portal_user_id = result[0]['PORTAL_USER_ID']

                # Create initial session
                session_id = create_session(portal_user_id, client_ip, user_agent)
                if session_id:
                    # Set session state
                    st.session_state.customer_session_id = session_id
                    st.session_state.customer_id = customer_id
                    st.session_state.portal_user_id = portal_user_id

                    # Log successful registration
                    log_security_event(
                        portal_user_id, 'REGISTRATION_SUCCESS',
                        client_ip, user_agent,
                        f"New user registration: {email}"
                    )

                    st.success("Registration successful!")
                    st.session_state.page = 'portal_home'
                    st.rerun()
                else:
                    st.error("Error creating session")

            except Exception as e:
                st.error("An error occurred during registration")
                print(f"Registration error: {str(e)}")
                log_security_event(
                    None, 'REGISTRATION_FAILED',
                    client_ip, user_agent,
                    f"Registration failed for {email}: {str(e)}"
                )
    
    # Login link
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("Already have an account?")
    with col2:
        if st.button("Login", use_container_width=True):
            st.session_state.portal_mode = 'customer'
            st.session_state.page = 'login'
            st.rerun()