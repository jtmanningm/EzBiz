import streamlit as st
from utils.auth.auth_utils import hash_password
from database.connection import snowflake_conn

def setup_admin_user(employee_id: int, email: str, password: str) -> bool:
    """
    Create initial admin user for business portal
    Returns True if successful, False otherwise
    """
    try:
        # Check if employee exists
        emp_check = """
        SELECT COUNT(*) as count 
        FROM OPERATIONAL.CARPET.EMPLOYEE 
        WHERE EMPLOYEE_ID = ?
        """
        result = snowflake_conn.execute_query(emp_check, [employee_id])
        if not result or result[0]['COUNT'] == 0:
            st.error(f"Employee ID {employee_id} not found")
            return False

        # Hash password
        password_hash = hash_password(password)
        
        # Create admin user
        query = """
        INSERT INTO OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS (
            EMPLOYEE_ID,
            EMAIL,
            PASSWORD_HASH,
            IS_ADMIN,
            IS_ACTIVE,
            EMAIL_VERIFIED
        ) VALUES (?, ?, ?, TRUE, TRUE, TRUE)
        """
        
        snowflake_conn.execute_query(query, [
            employee_id,
            email.lower(),
            password_hash
        ])
        
        return True

    except Exception as e:
        st.error(f"Error creating admin user: {str(e)}")
        return False

# Usage example:
if st.button("Setup Admin User"):
    employee_id = st.number_input("Employee ID", min_value=1)
    email = st.text_input("Admin Email")
    password = st.text_input("Password", type="password")
    
    if st.button("Create Admin"):
        if setup_admin_user(employee_id, email, password):
            st.success("Admin user created successfully!")
        else:
            st.error("Failed to create admin user")