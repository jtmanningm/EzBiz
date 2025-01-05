import streamlit as st
from datetime import datetime
from database.connection import snowflake_conn
from config.settings import JOB_TITLES, DEPARTMENTS
from utils.validation import validate_email, validate_phone
from utils.formatting import format_currency

def employees_settings_page():
    """Employee management settings page"""
    st.title("Employee Management")

    tab1, tab2 = st.tabs(["Employee List", "Add Employee"])

    # Employee List Tab
    with tab1:
        st.header("Current Employees")
        
        # Fetch employees
        query = """
        SELECT *
        FROM OPERATIONAL.CARPET.EMPLOYEE
        ORDER BY LAST_NAME, FIRST_NAME
        """
        employees = snowflake_conn.execute_query(query)

        # Status filter
        status_filter = st.selectbox(
            "Filter by Status",
            ["All", "Active", "Inactive"]
        )

        filtered_employees = employees
        if status_filter == "Active":
            filtered_employees = [e for e in employees if e['ACTIVE_STATUS']]
        elif status_filter == "Inactive":
            filtered_employees = [e for e in employees if not e['ACTIVE_STATUS']]

        for employee in filtered_employees:
            with st.expander(f"{employee['FIRST_NAME']} {employee['LAST_NAME']}"):
                with st.form(f"employee_form_{employee['EMPLOYEE_ID']}"):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        first_name = st.text_input(
                            "First Name", 
                            employee['FIRST_NAME']
                        )
                        last_name = st.text_input(
                            "Last Name", 
                            employee['LAST_NAME']
                        )
                        email = st.text_input(
                            "Email", 
                            employee['EMAIL']
                        )
                        phone = st.text_input(
                            "Phone",
                            employee['PHONE_NUMBER'] if employee['PHONE_NUMBER'] else ""
                        )
                        hire_date = st.date_input(
                            "Hire Date",
                            value=employee['HIRE_DATE'] if employee['HIRE_DATE'] else datetime.now().date()
                        )
                    
                    with col2:
                        job_title = st.selectbox(
                            "Job Title",
                            JOB_TITLES,
                            index=JOB_TITLES.index(employee['JOB_TITLE']) if employee['JOB_TITLE'] in JOB_TITLES else 0
                        )
                        department = st.selectbox(
                            "Department",
                            DEPARTMENTS,
                            index=DEPARTMENTS.index(employee['DEPARTMENT']) if employee['DEPARTMENT'] in DEPARTMENTS else 0
                        )
                        hourly_wage = st.number_input(
                            "Hourly Wage",
                            value=float(employee['HOURLY_WAGE']) if employee['HOURLY_WAGE'] else 0.0,
                            min_value=0.0,
                            step=0.5
                        )
                        active_status = st.checkbox(
                            "Active",
                            value=employee['ACTIVE_STATUS']
                        )

                    # Termination details if inactive
                    if not active_status:
                        term_date = st.date_input(
                            "Termination Date",
                            value=employee['TERMINATION_DATE'] if employee['TERMINATION_DATE'] else datetime.now().date()
                        )
                        term_reason = st.text_area(
                            "Termination Reason",
                            value=employee['TERMINATION_REASON'] if employee['TERMINATION_REASON'] else ""
                        )

                    if st.form_submit_button("Update Employee"):
                        # Validate inputs
                        if not email or not validate_email(email):
                            st.error("Please enter a valid email address")
                            return
                        
                        if phone:
                            is_valid_phone, cleaned_phone = validate_phone(phone)
                            if not is_valid_phone:
                                st.error("Please enter a valid phone number")
                                return
                        
                        # Update employee
                        update_query = """
                        UPDATE OPERATIONAL.CARPET.EMPLOYEE
                        SET FIRST_NAME = :1,
                            LAST_NAME = :2,
                            EMAIL = :3,
                            PHONE_NUMBER = :4,
                            HIRE_DATE = :5,
                            JOB_TITLE = :6,
                            DEPARTMENT = :7,
                            HOURLY_WAGE = :8,
                            ACTIVE_STATUS = :9,
                            TERMINATION_DATE = :10,
                            TERMINATION_REASON = :11,
                            MODIFIED_DATE = CURRENT_TIMESTAMP()
                        WHERE EMPLOYEE_ID = :12
                        """
                        try:
                            params = [
                                first_name, last_name, email,
                                cleaned_phone if phone else None,
                                hire_date, job_title, department,
                                hourly_wage, active_status,
                                term_date if not active_status else None,
                                term_reason if not active_status else None,
                                employee['EMPLOYEE_ID']
                            ]
                            snowflake_conn.execute_query(update_query, params)
                            st.success("Employee updated successfully!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error updating employee: {str(e)}")

    # Add Employee Tab
    with tab2:
        st.header("Add New Employee")
        
        with st.form("new_employee_form"):
            col1, col2 = st.columns(2)
            
            with col1:
                new_first_name = st.text_input("First Name")
                new_last_name = st.text_input("Last Name")
                new_email = st.text_input("Email")
                new_phone = st.text_input("Phone")
                new_hire_date = st.date_input(
                    "Hire Date",
                    value=datetime.now().date()
                )
            
            with col2:
                new_job_title = st.selectbox("Job Title", JOB_TITLES)
                new_department = st.selectbox("Department", DEPARTMENTS)
                new_hourly_wage = st.number_input(
                    "Hourly Wage",
                    min_value=0.0,
                    step=0.5
                )
                new_active = st.checkbox("Active", value=True)

            if st.form_submit_button("Add Employee"):
                # Validate inputs
                if not new_email or not validate_email(new_email):
                    st.error("Please enter a valid email address")
                    return
                
                if new_phone:
                    is_valid_phone, cleaned_phone = validate_phone(new_phone)
                    if not is_valid_phone:
                        st.error("Please enter a valid phone number")
                        return

                # Insert new employee
                insert_query = """
                INSERT INTO OPERATIONAL.CARPET.EMPLOYEE (
                    FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER,
                    HIRE_DATE, JOB_TITLE, DEPARTMENT,
                    HOURLY_WAGE, ACTIVE_STATUS
                ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
                """
                try:
                    params = [
                        new_first_name, new_last_name, new_email,
                        cleaned_phone if new_phone else None,
                        new_hire_date, new_job_title,
                        new_department, new_hourly_wage, new_active
                    ]
                    snowflake_conn.execute_query(insert_query, params)
                    st.success("New employee added successfully!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Error adding employee: {str(e)}")