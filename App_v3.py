import streamlit as st
import pandas as pd
from datetime import datetime, date, time, timedelta
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import os
import time as t 
from typing import Union, Optional, List, Dict, Any, Tuple
import math

# -------------------------------
# Configuration and Setup
# -------------------------------
st.set_page_config(
    page_title="EZ Biz",
    page_icon="🧹",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# -------------------------------
# Database Connection
# -------------------------------
class SnowflakeConnection:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        self.session = self._create_session()
    
    def _create_session(self):
        try:
            private_key = self._load_private_key()
            connection_parameters = {
                "account": "uvfnphy-okb79182",
                "user": "JTMANNINGM",
                "private_key": private_key,
                "role": "ACCOUNTADMIN",
                "warehouse": "COMPUTE_WH",
                "database": "OPERATIONAL",
                "schema": "CARPET"
            }
            return Session.builder.configs(connection_parameters).create()
        except Exception as e:
            st.error(f"Failed to create Snowpark session: {e}")
            return None

    def _load_private_key(self):
        PRIVATE_KEY_PATH = os.path.expanduser('~/Documents/Key/rsa_key.p8')
        PRIVATE_KEY_PASSPHRASE = 'Lizard24'
        
        with open(PRIVATE_KEY_PATH, 'rb') as key_file:
            private_key = serialization.load_pem_private_key(
                key_file.read(),
                password=PRIVATE_KEY_PASSPHRASE.encode(),
                backend=default_backend()
            )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

    def execute_query(self, query: str, params: Optional[list] = None) -> Optional[list]:
        try:
            return self.session.sql(query, params).collect()
        except Exception as e:
            st.error(f"Error executing query: {e}")
            return None

# Initialize database connection
snowflake_conn = SnowflakeConnection.get_instance()

# -------------------------------
# Data Fetching Functions
# -------------------------------
@st.cache_data
def fetch_existing_customers():
    try:
        customers_df = snowflake_conn.execute_query("""
            SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, STREET_ADDRESS, 
                PHONE_NUMBER, EMAIL_ADDRESS, PRIMARY_CONTACT_METHOD
            FROM CUSTOMER
        """)
        if customers_df:
            df = pd.DataFrame(customers_df)
            df['FULL_NAME'] = df['FIRST_NAME'] + " " + df['LAST_NAME']
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching customers: {e}")
        return pd.DataFrame()

@st.cache_data
def fetch_existing_services():
    try:
        services_df = snowflake_conn.execute_query("""
            SELECT SERVICE_ID, SERVICE_NAME, COST
            FROM SERVICES
        """)
        if services_df:
            return pd.DataFrame(services_df)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching services: {e}")
        return pd.DataFrame()

@st.cache_data
def fetch_existing_employees():
    try:
        employees_df = snowflake_conn.execute_query("""
            SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, HOURLY_WAGE
            FROM EMPLOYEE
        """)
        if employees_df:
            df = pd.DataFrame(employees_df)
            df['FULL_NAME'] = df['FIRST_NAME'] + " " + df['LAST_NAME']
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching employees: {e}")
        return pd.DataFrame()

# -------------------------------
# Helper Functions
# -------------------------------
def validate_numeric_value(value: Optional[Union[int, float, str]], default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        float_value = float(value)
        if math.isnan(float_value) or math.isinf(float_value):
            return default
        return max(0.0, float_value)
    except (ValueError, TypeError):
        return default
        
def validate_payment_amount(amount: Optional[Union[int, float, str]]) -> float:
    """
    Validate payment amount, ensuring it's a non-negative float
    """
    return validate_numeric_value(amount, 0.0)

def get_available_time_slots(service_date: date, service_duration: int = 60) -> List[time]:
    """
    Get available time slots for a given date.

    Args:
        service_date (date): The date to check for available slots.
        service_duration (int): Duration of the service in minutes (default is 60).

    Returns:
        List[time]: List of available time slots.
    """
    try:
        # Get booked slots
        query = """
        SELECT SERVICE_TIME
        FROM UPCOMING_SERVICES
        WHERE SERVICE_DATE = :1
        ORDER BY SERVICE_TIME
        """
        booked_slots = snowflake_conn.execute_query(query, [service_date.strftime('%Y-%m-%d')])

        # Generate time slots from 8 AM to 6 PM
        all_slots = []
        current_time = datetime.combine(service_date, time(8, 0))
        end_time = datetime.combine(service_date, time(17, 30))  # Last slot at 5:30 PM
        
        while current_time <= end_time:
            is_available = True

            # Check against booked slots
            if booked_slots:
                for slot in booked_slots:
                    booked_time = datetime.strptime(slot['SERVICE_TIME'], '%H:%M:%S').time()
                    booked_dt = datetime.combine(service_date, booked_time)
                    
                    # 30-minute buffer around booked slots
                    if abs((current_time - booked_dt).total_seconds()) < service_duration * 30:
                        is_available = False
                        break

            if is_available:
                all_slots.append(current_time.time())

            current_time += timedelta(minutes=30)

        return all_slots
    except Exception as e:
        st.error(f"Error getting time slots: {str(e)}")
        return []

def save_transaction(data: Dict[str, Any]) -> bool:
    """Save transaction with proper date handling"""
    try:
        query = """
        INSERT INTO SERVICE_TRANSACTION (
            CUSTOMER_ID, SERVICE_ID, SERVICE2_ID, SERVICE3_ID,
            EMPLOYEE1_ID, EMPLOYEE2_ID, EMPLOYEE3_ID,
            AMOUNT, DISCOUNT, DEPOSIT, AMOUNT_RECEIVED,
            PYMT_MTHD_1, PYMT_MTHD_1_AMT,
            PYMT_MTHD_2, PYMT_MTHD_2_AMT,
            START_TIME, END_TIME,
            TRANSACTION_DATE, TRANSACTION_TIME,
            COMMENTS
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, 
            :12, :13, :14, :15, :16, :17, :18, :19, :20
        )
        """
        
        params = [
            data['Customer ID'], 
            data['Service ID'],
            data['Service2 ID'],
            data['Service3 ID'],
            data['Employee1 ID'],
            data['Employee2 ID'],
            data['Employee3 ID'],
            data['Amount'],
            data['Discount'],
            data['Deposit'],
            data['Amount Received'],
            data['PYMT_MTHD_1'],
            data['PYMT_MTHD_1_AMT'],
            data['PYMT_MTHD_2'],
            data['PYMT_MTHD_2_AMT'],
            data['Start Time'],
            data['End Time'],
            data['Transaction Date'],
            data['Transaction Time'],
            data['COMMENTS']
        ]

        result = snowflake_conn.execute_query(query, params)

        # If successful, delete from UPCOMING_SERVICES
        delete_query = """
        DELETE FROM UPCOMING_SERVICES 
        WHERE CUSTOMER_ID = :1 AND SERVICE_ID = :2
        """
        snowflake_conn.execute_query(delete_query, [data['Customer ID'], data['Service ID']])
        
        return True
    except Exception as e:
        st.error(f"Error saving transaction: {str(e)}")
        st.error("Full error details:")
        st.error(e)
        return False

def prepare_transaction_data(
    customer_id: int,
    service_id: Optional[int],
    service2_id: Optional[int] = None,
    service3_id: Optional[int] = None,
    employee1_id: Optional[int] = None,
    employee2_id: Optional[int] = None,
    employee3_id: Optional[int] = None,
    total_cost: float = 0.0,
    discount: float = 0.0,
    deposit: float = 0.0,
    payment_amount_1: float = 0.0,
    payment_amount_2: float = 0.0,
    payment_method_1: Optional[str] = None,
    payment_method_2: Optional[str] = None,
    notes: Optional[str] = None
) -> dict:
    """Prepare transaction data"""
    current_time = datetime.now()
    amount_received = deposit + payment_amount_1 + payment_amount_2
    transaction_date = current_time.date()
    
    return {
        'Customer ID': customer_id,
        'Service ID': service_id,
        'Service2 ID': service2_id,
        'Service3 ID': service3_id,
        'Employee1 ID': employee1_id,
        'Employee2 ID': employee2_id,
        'Employee3 ID': employee3_id,
        'Amount': float(total_cost),
        'Discount': float(discount),
        'Deposit': float(deposit),
        'Amount Received': float(amount_received),
        'PYMT_MTHD_1': payment_method_1,
        'PYMT_MTHD_1_AMT': float(payment_amount_1),
        'PYMT_MTHD_2': payment_method_2,
        'PYMT_MTHD_2_AMT': float(payment_amount_2),
        'Start Time': st.session_state.get('service_start_time', current_time).strftime('%H:%M:%S'),
        'End Time': current_time.strftime('%H:%M:%S'),
        'Transaction Date': transaction_date,
        'Transaction Time': current_time.time(),
        'COMMENTS': notes if notes else ''
    }

def get_service_id(service_name: str) -> Optional[int]:
    """
    Get the service ID for a given service name from the services DataFrame
    
    Args:
        service_name (str): Name of the service
        
    Returns:
        Optional[int]: Service ID if found, None otherwise
    """
    if not service_name:
        return None
        
    services_df = fetch_existing_services()
    if services_df.empty:
        return None
        
    service_row = services_df[services_df['SERVICE_NAME'] == service_name]
    if service_row.empty:
        return None
        
    return int(service_row.iloc[0]['SERVICE_ID'])

def get_employee_id(employee_name: str) -> Optional[int]:
    """
    Get the employee ID for a given employee name from the employees DataFrame
    
    Args:
        employee_name (str): Full name of the employee
        
    Returns:
        Optional[int]: Employee ID if found, None otherwise
    """
    if not employee_name:
        return None
        
    employees_df = fetch_existing_employees()
    if employees_df.empty:
        return None
        
    employee_row = employees_df[employees_df['FULL_NAME'] == employee_name]
    if employee_row.empty:
        return None
        
    return int(employee_row.iloc[0]['EMPLOYEE_ID'])

def save_customer_info(
    name: str,
    phone: str,
    email: str,
    address: str,
    city: str,
    state: str,
    zip_code: str,
    contact_preference: str,
    account_type: Optional[str] = None
) -> Optional[int]:
    """
    Save customer information to the database
    
    Args:
        name (str): Customer's full name
        phone (str): Phone number
        email (str): Email address
        address (str): Street address
        city (str): City
        state (str): State
        zip_code (str): ZIP code
        contact_preference (str): Preferred contact method
        account_type (Optional[str]): Account type if applicable
        
    Returns:
        Optional[int]: Customer ID if successful, None if failed
    """
    try:
        name_parts = name.split(' ', 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ""

        text_flag = contact_preference.upper() == "TEXT"
        zip_code_int = int(zip_code) if zip_code else None

        query = """
        INSERT INTO CUSTOMER (
            FIRST_NAME, LAST_NAME, STREET_ADDRESS, CITY, STATE, ZIP_CODE,
            EMAIL_ADDRESS, PHONE_NUMBER, TEXT_FLAG, SERVICE_ADDRESS,
            PRIMARY_CONTACT_METHOD
        ) VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11)
        """
        
        params = [
            first_name, last_name, address, city, state, zip_code_int,
            email, phone, text_flag, address, contact_preference
        ]
        
        snowflake_conn.execute_query(query, params)
        
        # Get the newly created customer ID
        result = snowflake_conn.execute_query(
            "SELECT CUSTOMER_ID FROM CUSTOMER WHERE FIRST_NAME = :1 AND LAST_NAME = :2 "
            "ORDER BY CREATED_AT DESC LIMIT 1",
            [first_name, last_name]
        )
        
        return result[0]['CUSTOMER_ID'] if result else None
    except Exception as e:
        st.error(f"Error saving customer information: {e}")
        return None

# def save_service_schedule_with_debug(
#     customer_id: int,
#     services: Union[str, List[str]],
#     service_date: date,
#     service_time: time,
#     deposit_amount: float = 0.0,
#     notes: Optional[str] = None,
#     is_recurring: bool = False,
#     recurrence_pattern: Optional[str] = None
# ) -> bool:
#     """Save service schedule with deposit information and debugging"""
#     try:
#         # Debug logging
#         st.write("Debug Info:")
#         st.write(f"Customer ID: {customer_id}")
#         st.write(f"Services: {services}")
#         st.write(f"Date: {service_date}")
#         st.write(f"Time: {service_time}")
#         st.write(f"Deposit Amount: {deposit_amount}")
#         st.write(f"Notes: {notes}")
#         st.write(f"Is Recurring: {is_recurring}")
#         st.write(f"Recurrence Pattern: {recurrence_pattern}")

#         # Insert main schedule
#         query = """
#         INSERT INTO UPCOMING_SERVICES (
#             CUSTOMER_ID, 
#             SERVICE_NAME, 
#             SERVICE_DATE, 
#             SERVICE_TIME,
#             IS_RECURRING, 
#             RECURRENCE_PATTERN, 
#             NOTES, 
#             DEPOSIT,
#             DEPOSIT_PAID  -- Ensure this is set to False initially
#         ) VALUES (
#             :1, :2, :3, :4, :5, :6, :7, :8, :9
#         )
#         """
        
#         service_name = services[0] if isinstance(services, list) else services
#         params = [
#             customer_id,
#             service_name,
#             service_date.strftime('%Y-%m-%d'),
#             service_time.strftime('%H:%M:%S'),
#             is_recurring,
#             recurrence_pattern if is_recurring else None,
#             notes,
#             float(deposit_amount),
#             False  # Always set DEPOSIT_PAID to False when scheduling a new service
#         ]

#         # Debug params
#         st.write("Query Parameters:")
#         st.write(params)
        
#         result = snowflake_conn.execute_query(query, params)
#         st.write("Query executed successfully")
        
#         if is_recurring:
#             st.write("Setting up recurring services...")
#             schedule_recurring_services(
#                 customer_id=customer_id,
#                 service_name=service_name,
#                 service_date=service_date,
#                 service_time=service_time,
#                 recurrence_pattern=recurrence_pattern,
#                 notes=notes
#             )
#             st.write("Recurring services scheduled")
        
#         return True
#     except Exception as e:
#         st.error(f"Error scheduling service: {str(e)}")
#         st.error("Full error details:", exc_info=True)
#         return False

def save_service_schedule_with_debug(
    customer_id: int,
    services: Union[str, List[str]],
    service_date: date,
    service_time: time,
    deposit_amount: float = 0.0,
    notes: Optional[str] = None,
    is_recurring: bool = False,
    recurrence_pattern: Optional[str] = None
) -> bool:
    """
    Save service schedule with deposit only on first service
    """
    try:
        # Insert main schedule (first service) with deposit
        query = """
        INSERT INTO UPCOMING_SERVICES (
            CUSTOMER_ID, 
            SERVICE_NAME, 
            SERVICE_DATE, 
            SERVICE_TIME,
            IS_RECURRING, 
            RECURRENCE_PATTERN, 
            NOTES, 
            DEPOSIT,
            DEPOSIT_PAID
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9
        )
        """
        
        service_name = services[0] if isinstance(services, list) else services
        params = [
            customer_id,
            service_name,
            service_date.strftime('%Y-%m-%d'),
            service_time.strftime('%H:%M:%S'),
            is_recurring,
            recurrence_pattern if is_recurring else None,
            notes,
            float(deposit_amount),  # Deposit only on first service
            False
        ]
        
        snowflake_conn.execute_query(query, params)
        
        # Schedule recurring services if needed (without deposits)
        if is_recurring:
            schedule_recurring_services(
                customer_id=customer_id,
                service_name=service_name,
                service_date=service_date,
                service_time=service_time,
                recurrence_pattern=recurrence_pattern,
                notes=notes
            )
        
        return True
    except Exception as e:
        st.error(f"Error scheduling service: {e}")
        return False

def schedule_recurring_services(
    customer_id: int,
    service_name: str,
    service_date: date,
    service_time: time,
    recurrence_pattern: str,
    notes: Optional[str] = None
) -> bool:
    """
    Schedule recurring services with deposit only on first service
    
    Args:
        customer_id (int): Customer ID
        service_name (str): Name of the service
        service_date (date): Initial service date
        service_time (time): Service time
        recurrence_pattern (str): Pattern of recurrence (Weekly, Bi-Weekly, Monthly)
        notes (Optional[str]): Service notes
    """
    try:
        future_dates = []
        current_date = service_date

        # Calculate next 6 occurrences
        for _ in range(6):
            if recurrence_pattern == "Weekly":
                current_date = current_date + timedelta(days=7)
            elif recurrence_pattern == "Bi-Weekly":
                current_date = current_date + timedelta(days=14)
            elif recurrence_pattern == "Monthly":
                current_date = current_date + timedelta(days=30)
            future_dates.append(current_date)

        # Insert recurring services - no deposit for future dates
        for future_date in future_dates:
            query = """
            INSERT INTO UPCOMING_SERVICES (
                CUSTOMER_ID,
                SERVICE_NAME,
                SERVICE_DATE,
                SERVICE_TIME,
                IS_RECURRING,
                RECURRENCE_PATTERN,
                Notes,
                DEPOSIT,
                DEPOSIT_PAID
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9
            )
            """
            params = [
                int(customer_id),
                service_name,
                future_date.strftime('%Y-%m-%d'),
                service_time.strftime('%H:%M:%S'),
                True,
                recurrence_pattern,
                notes,
                0.0,  # No deposit for recurring instances
                False
            ]
            snowflake_conn.execute_query(query, params)
        return True
    except Exception as e:
        st.error(f"Error scheduling recurring services: {e}")
        return False

def dynamic_service_selector(service_df: pd.DataFrame, selected_services: List[Dict[str, Any]]) -> None:
    """
    Helper function to handle dynamic service selection
    
    Args:
        service_df (pd.DataFrame): DataFrame containing service information
        selected_services (List[Dict[str, Any]]): List of currently selected services
    """
    for idx in range(len(selected_services)):
        service_name = st.selectbox(
            f"Select Service {idx + 1}", 
            service_df['SERVICE_NAME'].tolist(),
            key=f"service_{idx}"
        )
        selected_service_id = service_df[service_df['SERVICE_NAME'] == service_name]['SERVICE_ID'].iloc[0]
        st.session_state['selected_services'][idx] = {
            'SERVICE_ID': selected_service_id,
            'SERVICE_NAME': service_name
        }

    if st.button("+ Add Another Service", key="add_service"):
        st.session_state['selected_services'].append({
            'SERVICE_ID': None,
            'SERVICE_NAME': None,
            'index': len(selected_services)
        })

def dynamic_employee_selector(employee_df: pd.DataFrame, selected_employees: List[Dict[str, Any]]) -> None:
    """
    Helper function to handle dynamic employee selection
    
    Args:
        employee_df (pd.DataFrame): DataFrame containing employee information
        selected_employees (List[Dict[str, Any]]): List of currently selected employees
    """
    for idx in range(len(selected_employees)):
        employee_name = st.selectbox(
            f"Select Employee {idx + 1}", 
            employee_df['FULL_NAME'].tolist(),
            key=f"employee_{idx}"
        )
        selected_employee_id = employee_df[employee_df['FULL_NAME'] == employee_name]['EMPLOYEE_ID'].iloc[0]
        st.session_state['selected_employees'][idx] = {
            'EMPLOYEE_ID': selected_employee_id,
            'EMPLOYEE_NAME': employee_name,
            'HOURLY_WAGE': float(employee_df[employee_df['EMPLOYEE_ID'] == selected_employee_id]['HOURLY_WAGE'].iloc[0])
        }

    if st.button("+ Add Another Employee", key="add_employee"):
        st.session_state['selected_employees'].append({
            'EMPLOYEE_ID': None,
            'EMPLOYEE_NAME': None,
            'HOURLY_WAGE': None,
            'index': len(selected_employees)
        })

def handle_service_selection(services_df: pd.DataFrame, 
                           base_service_name: Optional[str] = None) -> Tuple[List[str], float]:
    """
    Handle service selection and calculate total cost
    
    Args:
        services_df (pd.DataFrame): DataFrame containing service information
        base_service_name (Optional[str]): Name of the base service to include
        
    Returns:
        Tuple[List[str], float]: List of selected services and total cost
    """
    selected_services = []
    total_cost = 0.0
    
    # Add base service if provided
    if base_service_name:
        selected_services.append(base_service_name)
        base_service_cost = float(services_df[services_df['SERVICE_NAME'] == base_service_name]['COST'].iloc[0])
        total_cost += base_service_cost
    
    # Additional services selection
    additional_services = st.multiselect(
        "Additional Services",
        options=[s for s in services_df['SERVICE_NAME'].tolist() 
                if s != base_service_name],
        key="additional_services"
    )
    
    # Add additional services and costs
    for service_name in additional_services:
        selected_services.append(service_name)
        service_cost = float(services_df[services_df['SERVICE_NAME'] == service_name]['COST'].iloc[0])
        total_cost += service_cost
        
    return selected_services, total_cost

def handle_employee_assignments(employees_df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Handle employee assignments and return assigned employee information
    
    Args:
        employees_df (pd.DataFrame): DataFrame containing employee information
        
    Returns:
        List[Dict[str, Any]]: List of assigned employee information
    """
    assigned_employees = []
    
    selected_employee_names = st.multiselect(
        "Assign Employees",
        options=employees_df['FULL_NAME'].tolist(),
        key="employee_assignments"
    )
    
    for emp_name in selected_employee_names:
        employee_row = employees_df[employees_df['FULL_NAME'] == emp_name].iloc[0]
        assigned_employees.append({
            'EMPLOYEE_ID': int(employee_row['EMPLOYEE_ID']),
            'EMPLOYEE_NAME': emp_name,
            'HOURLY_WAGE': float(employee_row['HOURLY_WAGE'])
        })
    
    return assigned_employees

def calculate_labor_cost(assigned_employees: List[Dict[str, Any]]) -> float:
    """
    Calculate total labor cost for assigned employees
    
    Args:
        assigned_employees (List[Dict[str, Any]]): List of assigned employee information
        
    Returns:
        float: Total labor cost
    """
    total_labor_cost = 0.0
    
    for idx, employee in enumerate(assigned_employees):
        col1, col2 = st.columns(2)
        with col1:
            hours = st.number_input(
                f"Hours worked by {employee['EMPLOYEE_NAME']}",
                min_value=0.0,
                step=0.5,
                key=f"labor_hours_{idx}"
            )
        with col2:
            rate = st.number_input(
                f"Hourly rate",
                min_value=0.0,
                value=employee['HOURLY_WAGE'],
                step=1.0,
                key=f"labor_rate_{idx}"
            )
        total_labor_cost += hours * rate
        
    return total_labor_cost

def handle_deposit(total_cost: float) -> Tuple[float, Optional[str]]:
    """
    Simple deposit handling with single input field
    
    Args:
        total_cost (float): Total cost of service
        
    Returns:
        Tuple[float, Optional[str]]: Deposit amount and payment method
    """
    deposit_amount = 0.0
    deposit_payment_method = None
    
    if st.checkbox("Require Deposit", key="require_deposit"):
        deposit_amount = st.number_input(
            "Deposit Amount",
            min_value=0.0,
            max_value=total_cost,
            value=0.0,
            step=5.0
        )
        
        if deposit_amount > 0:
            deposit_payment_method = st.selectbox(
                "Deposit Payment Method",
                ["Cash", "Credit Card", "Debit Card", "Check", "CashApp", "Venmo", "Zelle"],
                key="deposit_payment_method"
            )
            
            st.write(f"Remaining Balance: ${(total_cost - deposit_amount):.2f}")
    
    return deposit_amount, deposit_payment_method

def display_availability_selector(service_date: date, service_duration: int = 60) -> Optional[time]:
    """
    Display time slot selector.

    Args:
        service_date (date): Date for which to show available slots.
        service_duration (int): Duration of the service in minutes.

    Returns:
        Optional[time]: Selected time or None if no selection made.
    """
    available_slots = get_available_time_slots(service_date, service_duration)
    if not available_slots:
        st.warning("No available time slots for selected date")
        return None

    formatted_slots = ["Select time..."] + [slot.strftime("%I:%M %p") for slot in available_slots]
    selected_time_str = st.selectbox("Service Time", options=formatted_slots)
    if selected_time_str != "Select time...":
        return datetime.strptime(selected_time_str, "%I:%M %p").time()
    return None


def create_initial_transaction(
    customer_id: int,
    service_id: int,
    service_date: date,
    service_time: time,
    total_cost: float,
    deposit_amount: float = 0.0,
    notes: Optional[str] = None
) -> Optional[int]:
    """
    Create an initial transaction record in the SERVICE_TRANSACTION table.
    
    Args:
        customer_id (int): ID of the customer
        service_id (int): Primary service ID
        service_date (date): Scheduled date for the service
        service_time (time): Scheduled time for the service
        total_cost (float): Total cost of the service
        deposit_amount (float): Amount of deposit collected
        notes (Optional[str]): Additional notes for the transaction
    
    Returns:
        Optional[int]: Transaction ID if successful, None otherwise
    """
    try:
        # SQL insert statement for SERVICE_TRANSACTION
        query = """
        INSERT INTO OPERATIONAL.CARPET.SERVICE_TRANSACTION (
            CUSTOMER_ID,
            SERVICE_ID,
            AMOUNT,
            DEPOSIT,
            AMOUNT_RECEIVED,
            TRANSACTION_DATE,
            SERVICE_DATE,
            SERVICE_TIME,
            STATUS,
            COMMENTS
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10
        )
        """
        
        params = [
            customer_id,
            service_id,
            total_cost,
            deposit_amount,
            deposit_amount,  # Initial amount received is the deposit
            datetime.now().date(),  # Current date as transaction date
            service_date,
            service_time,
            'SCHEDULED',  # Initial status for the transaction
            notes
        ]
        
        # Execute the query
        result = snowflake_conn.execute_query(query, params)
        
        # Assuming the database returns the inserted ID (if using RETURNING)
        return result[0]['ID'] if result else None
        
    except Exception as e:
        st.error(f"Error creating initial transaction: {e}")
        return None

def check_service_availability(service_date: date, service_time: time, service_duration: int = 60) -> Tuple[bool, Optional[str]]:
    try:
        requested_start = datetime.combine(service_date, service_time)
        requested_end = requested_start + timedelta(minutes=service_duration)
        
        buffer_before = requested_start - timedelta(minutes=30)
        buffer_after = requested_end + timedelta(minutes=30)
        
        query = """
        SELECT SERVICE_TIME
        FROM UPCOMING_SERVICES
        WHERE SERVICE_DATE = :1
          AND (SERVICE_TIME BETWEEN :2 AND :3
               OR DATEADD(minute, :4, SERVICE_TIME) BETWEEN :2 AND :3)
        """
        params = [
            service_date.strftime('%Y-%m-%d'),
            buffer_before.time().strftime('%H:%M:%S'),
            buffer_after.time().strftime('%H:%M:%S'),
            service_duration
        ]
        
        existing_services = snowflake_conn.execute_query(query, params)
        
        if existing_services:
            conflict_time = datetime.strptime(existing_services[0]['SERVICE_TIME'], '%H:%M:%S').time()
            return False, f"Time slot not available. Conflict at {conflict_time.strftime('%I:%M %p')}"
        
        if service_time.hour < 8 or (service_time.hour >= 18 and service_time.minute > 0):
            return False, "Service time must be between 8 AM and 6 PM"
        
        return True, None
    except Exception as e:
        st.error(f"Error checking service availability: {e}")
        return False, str(e)

def confirm_deposit_payment(service_id: int) -> bool:
    """
    Confirm deposit payment for a scheduled service
    
    Args:
        service_id (int): ID of the service
        
    Returns:
        bool: True if confirmed successfully
    """
    try:
        query = """
        UPDATE UPCOMING_SERVICES
        SET DEPOSIT_PAID = TRUE
        WHERE SERVICE_ID = :1
        """
        snowflake_conn.execute_query(query, [service_id])
        return True
    except Exception as e:
        st.error(f"Error confirming deposit: {e}")
        return False

def handle_service_start(service_data: dict) -> bool:
    """
    Handle service start logic including deposit verification
    
    Args:
        service_data (dict): Service information
        
    Returns:
        bool: True if service can be started
    """
    if service_data.get('DEPOSIT', 0) > 0 and not service_data.get('DEPOSIT_PAID', False):
        st.error("Deposit payment must be confirmed before starting service")
        
        col1, col2 = st.columns(2)
        with col1:
            st.write(f"Required Deposit: ${float(service_data['DEPOSIT']):.2f}")
            if st.button("Confirm Deposit Payment"):
                if confirm_deposit_payment(service_data['SERVICE_ID']):
                    st.success("Deposit payment confirmed")
                    return True
                return False
        return False
    return True
# -------------------------------
# Page Components
# -------------------------------

def new_service_page():
    """
    Complete new service scheduling page with deposit handling and time selection
    """
    st.markdown("""
        <div class="page-header">
            <h2>New Service</h2>
        </div>
    """, unsafe_allow_html=True)

    # Initialize session state
    if 'selected_customer_id' not in st.session_state:
        st.session_state['selected_customer_id'] = None

    # Fetch existing customers for search
    existing_customers_df = fetch_existing_customers()
    customer_names = []
    if not existing_customers_df.empty:
        customer_names = existing_customers_df['FULL_NAME'].tolist()

    # Customer Information Section
    with st.container():
        st.markdown("### Customer Information")
        customer_name = st.text_input(
            "Customer Name",
            help="Enter customer name to search or add new",
            key="new_customer_name"
        )

        selected_customer = None
        if customer_name:
            matching_customers = [name for name in customer_names 
                                if customer_name.lower() in name.lower()]
            
            if matching_customers:
                selected_customer = st.selectbox(
                    "Select Existing Customer",
                    options=["Select..."] + matching_customers,
                    key="existing_customer_select"
                )

                if selected_customer and selected_customer != "Select...":
                    customer_details = existing_customers_df[
                        existing_customers_df['FULL_NAME'] == selected_customer
                    ].iloc[0]
                    st.session_state['selected_customer_id'] = int(customer_details['CUSTOMER_ID'])
                    st.info(f"""
                        Selected Customer Details:
                        - Phone: {customer_details['PHONE_NUMBER']}
                        - Email: {customer_details['EMAIL_ADDRESS']}
                        - Address: {customer_details['STREET_ADDRESS']}
                    """)
                else:
                    st.session_state['selected_customer_id'] = None
            else:
                st.session_state['selected_customer_id'] = None

        # New customer form
        new_customer_data = {}
        if not selected_customer or selected_customer == "Select...":
            with st.container():
                col1, col2, col3 = st.columns(3)
                with col1:
                    new_customer_data['phone'] = st.text_input(
                        "Phone Number",
                        key="new_customer_phone"
                    )
                with col2:
                    new_customer_data['email'] = st.text_input(
                        "Email",
                        key="new_customer_email"
                    )
                with col3:
                    new_customer_data['contact_preference'] = st.selectbox(
                        "Preferred Contact Method",
                        ["Phone", "Text", "Email"],
                        key="new_customer_contact_pref"
                    )

            st.markdown("### Address Information")
            col1, col2 = st.columns(2)
            with col1:
                new_customer_data['street_address'] = st.text_input(
                    "Street Address",
                    key="new_customer_street"
                )
                new_customer_data['city'] = st.text_input(
                    "City",
                    key="new_customer_city"
                )
            with col2:
                new_customer_data['state'] = st.text_input(
                    "State",
                    key="new_customer_state"
                )
                new_customer_data['zip_code'] = st.text_input(
                    "ZIP Code",
                    key="new_customer_zip"
                )

    # Service Schedule Section
    st.markdown("### Service Schedule")
    col1, col2 = st.columns(2)
    
    with col1:
        service_date = st.date_input(
            "Service Date",
            min_value=datetime.now().date(),
            value=datetime.now().date(),
            key="new_service_date"
        )
    
    with col2:
        service_time = display_availability_selector(service_date)

    # Service Selection
    services_df = fetch_existing_services()
    if not services_df.empty:
        selected_services = st.multiselect(
            "Select Services",
            options=services_df['SERVICE_NAME'].tolist(),
            key="new_service_selection"
        )

        if selected_services:
            # Calculate total cost
            total_cost = 0.0
            for service in selected_services:
                service_row = services_df[services_df['SERVICE_NAME'] == service].iloc[0]
                total_cost += float(service_row['COST'])

            # Display cost summary
            st.markdown("### Service Summary")
            st.write(f"Total Cost: ${total_cost:.2f}")

            # Deposit Section
            deposit_amount = 0.0
            if st.checkbox("Add Deposit", key="deposit_checkbox"):
                deposit_amount = st.number_input(
                    "Deposit Amount",
                    min_value=0.0,
                    max_value=total_cost,
                    value=0.0,
                    step=5.0,
                    key="deposit_amount"
                )
                
                st.write(f"Remaining Balance: ${(total_cost - deposit_amount):.2f}")

            # Recurring Service Section
            st.markdown("### Service Frequency")
            col1, col2 = st.columns(2)
            with col1:
                is_recurring = st.checkbox("Recurring Service", key="recurring_checkbox")
            with col2:
                recurrence_pattern = None
                if is_recurring:
                    recurrence_pattern = st.selectbox(
                        "Recurrence Pattern",
                        ["Weekly", "Bi-Weekly", "Monthly"],
                        key="recurrence_pattern"
                    )

            # Additional Notes
            additional_notes = st.text_area(
                "Additional Notes",
                help="Enter any special instructions or notes",
                key="service_notes"
            )

            # Schedule Service Button
            if st.button("Schedule Service", type="primary", key="schedule_button"):
                try:
                    if not service_time:
                        st.error("Please select a service time")
                        return

                    # Get or create customer ID
                    customer_id = st.session_state.get('selected_customer_id')
                    if not customer_id:
                        if not customer_name or not new_customer_data['phone']:
                            st.error("Customer name and phone number are required")
                            return
                        
                        customer_id = save_customer_info(
                            name=customer_name,
                            phone=new_customer_data['phone'],
                            email=new_customer_data['email'],
                            address=new_customer_data['street_address'],
                            city=new_customer_data['city'],
                            state=new_customer_data['state'],
                            zip_code=new_customer_data['zip_code'],
                            contact_preference=new_customer_data['contact_preference']
                        )

                    if customer_id:
                        # Schedule the service
                        service_scheduled = save_service_schedule_with_debug(
                            customer_id=customer_id,
                            services=selected_services,
                            service_date=service_date,
                            service_time=service_time,
                            deposit_amount=deposit_amount,
                            notes=additional_notes,
                            is_recurring=is_recurring,
                            recurrence_pattern=recurrence_pattern
                        )

                        if service_scheduled:
                            success_message = (
                                f"Service scheduled successfully!\n"
                                f"Deposit Amount: ${deposit_amount:.2f}\n"
                                f"Remaining Balance: ${(total_cost - deposit_amount):.2f}"
                            )
                            if is_recurring:
                                success_message += f"\nRecurring: {recurrence_pattern}"
                            
                            st.success(success_message)
                            # Redirect to scheduled services page
                            st.session_state['page'] = 'scheduled_services'
                            st.rerun()

                        else:
                            st.error("Failed to schedule service. Please try again.")
                    else:
                        st.error("Failed to save customer information.")
                except Exception as e:
                    st.error(f"Error scheduling service: {str(e)}")
    else:
        st.error("No services available. Please add services first.")

def scheduled_services_page():
    st.title('Scheduled Services')
    
    # Initialize confirmation state
    if 'deposit_confirmation_state' not in st.session_state:
        st.session_state.deposit_confirmation_state = None
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now().date())
    with col2:
        end_date = st.date_input("End Date", value=datetime.now().date() + timedelta(days=30))

    query = """
    SELECT 
        US.SERVICE_ID,
        COALESCE(C.CUSTOMER_ID, A.ACCOUNT_ID) AS CUSTOMER_OR_ACCOUNT_ID,
        COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
        US.SERVICE_NAME,
        US.SERVICE_DATE,
        US.SERVICE_TIME,
        US.NOTES,
        US.DEPOSIT,
        US.DEPOSIT_PAID,
        S.COST
    FROM UPCOMING_SERVICES US
    LEFT JOIN CUSTOMER C ON US.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN ACCOUNTS A ON US.ACCOUNT_ID = A.ACCOUNT_ID
    LEFT JOIN SERVICES S ON US.SERVICE_NAME = S.SERVICE_NAME
    WHERE US.SERVICE_DATE BETWEEN :1 AND :2
    ORDER BY US.SERVICE_DATE, US.SERVICE_TIME
    """
    
    services_df = pd.DataFrame(snowflake_conn.execute_query(
        query,
        [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
    ))

    if not services_df.empty:
        current_date = None
        
        for _, row in services_df.iterrows():
            if current_date != row['SERVICE_DATE']:
                current_date = row['SERVICE_DATE']
                st.markdown(f"**{current_date.strftime('%A, %B %d, %Y')}**")
            
            with st.container():
                col1, col2, col3 = st.columns([2, 3, 1])
                with col1:
                    st.write(f"🕒 {row['SERVICE_TIME'].strftime('%I:%M %p')}")
                with col2:
                    service_info = f"📋 {row['SERVICE_NAME']} - {row['CUSTOMER_NAME']}"
                    deposit_amount = float(row['DEPOSIT']) if pd.notnull(row['DEPOSIT']) else 0.0
                    deposit_paid = bool(row['DEPOSIT_PAID']) if pd.notnull(row['DEPOSIT_PAID']) else False

                    if deposit_amount > 0:
                        deposit_status = "✅" if deposit_paid else "❌"
                        service_info += f"\n💰 Deposit Required: ${deposit_amount:.2f} {deposit_status}"
                    if pd.notnull(row['NOTES']):
                        service_info += f"\n📝 {row['NOTES']}"
                    st.write(service_info)
                
                with col3:
                    service_id = int(row['SERVICE_ID'])

                    # Display "Confirm Deposit" button if deposit is required and not yet paid
                    if deposit_amount > 0 and not deposit_paid:
                        if st.session_state.deposit_confirmation_state == service_id:
                            # Show success message and update button
                            st.success("Deposit confirmed!")
                            if st.button("Continue", key=f"continue_{service_id}"):
                                st.session_state.deposit_confirmation_state = None
                                st.rerun()
                        else:
                            if st.button("Confirm Deposit", key=f"confirm_deposit_{service_id}"):
                                update_query = """
                                UPDATE UPCOMING_SERVICES
                                SET DEPOSIT_PAID = TRUE
                                WHERE SERVICE_ID = :1
                                """
                                snowflake_conn.execute_query(update_query, [service_id])
                                st.session_state.deposit_confirmation_state = service_id
                                st.rerun()
                    else:
                        # Display start service button when deposit is not required or has been confirmed
                        if st.button("✓ Start", key=f"start_{service_id}"):
                            service_data = {
                                'SERVICE_ID': int(row['SERVICE_ID']),
                                'CUSTOMER_OR_ACCOUNT_ID': int(row['CUSTOMER_OR_ACCOUNT_ID']),
                                'CUSTOMER_NAME': str(row['CUSTOMER_NAME']),
                                'SERVICE_NAME': str(row['SERVICE_NAME']),
                                'SERVICE_DATE': row['SERVICE_DATE'],
                                'SERVICE_TIME': row['SERVICE_TIME'],
                                'NOTES': str(row['NOTES']) if pd.notnull(row['NOTES']) else None,
                                'DEPOSIT': deposit_amount,
                                'DEPOSIT_PAID': deposit_paid,
                                'COST': float(row['COST']) if pd.notnull(row['COST']) else 0.0
                            }
                            
                            st.session_state['selected_service'] = service_data
                            st.session_state['service_start_time'] = datetime.now()
                            st.session_state['page'] = 'transaction_details'
                            st.rerun()

        # Summary statistics
        st.markdown("### Summary")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Services", len(services_df))
        with col2:
            pending_deposits = len(services_df[
                (services_df['DEPOSIT'] > 0) & 
                (~services_df['DEPOSIT_PAID'].fillna(False))
            ])
            st.metric("Pending Deposits", pending_deposits)
        with col3:
            confirmed_deposits = len(services_df[
                (services_df['DEPOSIT'] > 0) & 
                (services_df['DEPOSIT_PAID'] == True)
            ])
            st.metric("Confirmed Deposits", confirmed_deposits)
    else:
        st.info("No services scheduled for the selected date range.")


def completed_services_page():
    st.title('Completed Services')
    
    # Date range selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date", 
            value=datetime.now().date() - timedelta(days=30)
        )
    with col2:
        end_date = st.date_input(
            "End Date", 
            value=datetime.now().date()
        )

    # Filter options
    payment_status = st.selectbox(
        "Filter by Payment Status", 
        ["All", "Paid", "Unpaid"]
    )

    # Query to fetch completed services with COMMENTS
    query = """
    SELECT 
        ST.ID AS TRANSACTION_ID,
        COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
        S1.SERVICE_NAME as SERVICE1_NAME,
        S2.SERVICE_NAME as SERVICE2_NAME,
        S3.SERVICE_NAME as SERVICE3_NAME,
        ST.TRANSACTION_DATE,
        ST.START_TIME,
        ST.END_TIME,
        ST.AMOUNT,
        ST.DISCOUNT,
        ST.AMOUNT_RECEIVED,
        ST.DEPOSIT,
        E1.FIRST_NAME || ' ' || E1.LAST_NAME AS EMPLOYEE1_NAME,
        E2.FIRST_NAME || ' ' || E2.LAST_NAME AS EMPLOYEE2_NAME,
        E3.FIRST_NAME || ' ' || E3.LAST_NAME AS EMPLOYEE3_NAME,
        ST.COMMENTS,
        CASE 
            WHEN (ST.AMOUNT_RECEIVED + COALESCE(ST.DEPOSIT, 0)) >= 
                (ST.AMOUNT - COALESCE(ST.DISCOUNT, 0)) THEN 'Paid'
            ELSE 'Unpaid'
        END AS PAYMENT_STATUS
    FROM SERVICE_TRANSACTION ST
    LEFT JOIN CUSTOMER C ON ST.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN ACCOUNTS A ON ST.ACCOUNT_ID = A.ACCOUNT_ID
    LEFT JOIN SERVICES S1 ON ST.SERVICE_ID = S1.SERVICE_ID
    LEFT JOIN SERVICES S2 ON ST.SERVICE2_ID = S2.SERVICE_ID
    LEFT JOIN SERVICES S3 ON ST.SERVICE3_ID = S3.SERVICE_ID
    LEFT JOIN EMPLOYEE E1 ON ST.EMPLOYEE1_ID = E1.EMPLOYEE_ID
    LEFT JOIN EMPLOYEE E2 ON ST.EMPLOYEE2_ID = E2.EMPLOYEE_ID
    LEFT JOIN EMPLOYEE E3 ON ST.EMPLOYEE3_ID = E3.EMPLOYEE_ID
    WHERE ST.TRANSACTION_DATE BETWEEN :1 AND :2
    ORDER BY ST.TRANSACTION_DATE DESC, ST.TRANSACTION_TIME DESC
    """
    
    completed_df = pd.DataFrame(snowflake_conn.execute_query(query, [
        start_date.strftime('%Y-%m-%d'), 
        end_date.strftime('%Y-%m-%d')
    ]))

    if not completed_df.empty:
        # Apply payment status filter
        if payment_status != "All":
            completed_df = completed_df[completed_df['PAYMENT_STATUS'] == payment_status]

        # Convert decimal columns to float for calculations
        decimal_columns = ['AMOUNT', 'DISCOUNT', 'AMOUNT_RECEIVED', 'DEPOSIT']
        for col in decimal_columns:
            completed_df[col] = completed_df[col].astype(float)

        # Display services with expandable details
        for _, row in completed_df.iterrows():
            with st.expander(
                f"{row['TRANSACTION_DATE'].strftime('%Y-%m-%d')} - {row['CUSTOMER_NAME']}"
            ):
                col1, col2 = st.columns(2)
                with col1:
                    services_list = [
                        name for name in [row['SERVICE1_NAME'], row['SERVICE2_NAME'], row['SERVICE3_NAME']]
                        if name is not None
                    ]
                    st.write("**Services:**")
                    for service in services_list:
                        st.write(f"- {service}")
                    
                    employees_list = [
                        name for name in [row['EMPLOYEE1_NAME'], row['EMPLOYEE2_NAME'], row['EMPLOYEE3_NAME']]
                        if name is not None
                    ]
                    st.write("**Employees:**")
                    for employee in employees_list:
                        st.write(f"- {employee}")

                with col2:
                    st.write("**Payment Details:**")
                    st.write(f"Amount: ${row['AMOUNT']:.2f}")
                    if row['DEPOSIT'] > 0:
                        st.write(f"Deposit: ${row['DEPOSIT']:.2f}")
                    if row['DISCOUNT'] > 0:
                        st.write(f"Discount: ${row['DISCOUNT']:.2f}")
                    st.write(f"Total Received: ${row['AMOUNT_RECEIVED']:.2f}")
                    st.write(f"Status: {row['PAYMENT_STATUS']}")
                
                if row['COMMENTS']:
                    st.write("**Service Comments:**")
                    st.write(row['COMMENTS'])

        # Summary statistics
        st.markdown("### Summary")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Services", len(completed_df))
        with col2:
            total_amount = completed_df['AMOUNT'].sum()
            st.metric("Total Amount", f"${total_amount:,.2f}")
        with col3:
            total_received = completed_df['AMOUNT_RECEIVED'].sum() + completed_df['DEPOSIT'].sum()
            st.metric("Total Received", f"${total_received:,.2f}")
        with col4:
            total_outstanding = total_amount - total_received
            st.metric("Outstanding Balance", f"${total_outstanding:,.2f}")
    else:
        st.info("No completed services found for the selected date range.")

def transaction_details_page():
    """Handle selected service transaction details"""
    st.title("Service Details")
    
    selected_service = st.session_state.get('selected_service')
    if not selected_service:
        st.error("No service selected. Please select a service from scheduled services.")
        return

    # Display service information
    st.write(f"### Current Service: {selected_service['SERVICE_NAME']}")
    st.write(f"Customer: {selected_service['CUSTOMER_NAME']}")
    if selected_service.get('NOTES'):
        st.write(f"Notes: {selected_service['NOTES']}")

    # Initial Payment Summary
    st.markdown("### Current Payment Status")
    service_cost = float(selected_service.get('COST', 0))
    deposit = float(selected_service.get('DEPOSIT', 0))
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Service Cost", f"${service_cost:.2f}")
        if deposit > 0:
            st.metric("Deposit Received", f"${deposit:.2f}")
    with col2:
        st.metric("Initial Balance", f"${service_cost:.2f}")
        st.metric("Current Balance", f"${(service_cost - deposit):.2f}")

    # Employee Assignment Section
    st.markdown("### Employee Assignment")
    employees_df = fetch_existing_employees()
    selected_employees = st.multiselect(
        "Assign Employees",
        options=employees_df["FULL_NAME"].tolist(),
        help="Select employees who performed the service",
        key="transaction_employee_selection"
    )

    # Service Timing
    st.markdown("### Service Timing")
    start_time = st.session_state.get('service_start_time', datetime.now()).time()
    end_time = datetime.now().time()
    st.write(f"Start Time: {start_time.strftime('%I:%M %p')}")
    st.write(f"End Time: {end_time.strftime('%I:%M %p')}")

    # Additional Payment Details
    st.markdown("### Additional Payment Details")
    remaining_balance = service_cost - deposit
    
    col1, col2 = st.columns(2)
    with col1:
        payment_method_1 = st.selectbox(
            "Payment Method 1",
            ["Select Method", "Cash", "Credit Card", "Debit Card", "Check", "CashApp", "Venmo", "Zelle"],
            key="payment_method_1"
        )
    with col2:
        payment_amount_1 = st.number_input(
            "Amount for Payment 1",
            min_value=0.0,
            max_value=remaining_balance,
            value=remaining_balance if payment_method_1 != "Select Method" else 0.0,
            key="payment_amount_1"
        )

    use_second_payment = st.checkbox("Add Second Payment Method", key="use_second_payment")
    payment_method_2 = None
    payment_amount_2 = 0.0
    
    if use_second_payment and remaining_balance - payment_amount_1 > 0:
        col1, col2 = st.columns(2)
        with col1:
            payment_method_2 = st.selectbox(
                "Payment Method 2",
                ["Select Method", "Cash", "Credit Card", "Debit Card", "Check", "CashApp", "Venmo", "Zelle"],
                key="payment_method_2"
            )
        with col2:
            payment_amount_2 = st.number_input(
                "Amount for Payment 2",
                min_value=0.0,
                max_value=remaining_balance - payment_amount_1,
                value=remaining_balance - payment_amount_1 if payment_method_2 != "Select Method" else 0.0,
                key="payment_amount_2"
            )

    # Final Payment Summary
    total_new_payment = payment_amount_1 + payment_amount_2
    final_total_received = deposit + total_new_payment
    final_remaining_balance = service_cost - final_total_received

    st.markdown("### Final Payment Summary")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Service Cost", f"${service_cost:.2f}")
        st.metric("Deposit Applied", f"${deposit:.2f}")
        st.metric("Additional Payments", f"${total_new_payment:.2f}")
    with col2:
        st.metric("Total Received", f"${final_total_received:.2f}")
        st.metric("Final Balance", f"${final_remaining_balance:.2f}")

    # Additional Notes
    st.markdown("### Additional Notes")
    notes = st.text_area(
        "Transaction Notes",
        value=selected_service.get('NOTES', ''),
        help="Add any additional notes about the service"
    )

    # Action Buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Complete Transaction", type="primary", key="complete_transaction"):
            if not selected_employees:
                st.error("Please assign at least one employee")
                return

            if payment_method_1 == "Select Method" and total_new_payment > 0:
                st.error("Please select a payment method for Payment 1")
                return
                
            if use_second_payment and payment_method_2 == "Select Method" and payment_amount_2 > 0:
                st.error("Please select a payment method for Payment 2")
                return

            # Prepare transaction data
            transaction_data = {
                'Customer ID': selected_service['CUSTOMER_OR_ACCOUNT_ID'],
                'Service ID': selected_service['SERVICE_ID'],
                'Amount': service_cost,
                'Discount': 0.0,  # Add discount field if needed
                'Deposit': deposit,
                'Amount Received': final_total_received,
                'PYMT_MTHD_1': payment_method_1 if payment_method_1 != "Select Method" else None,
                'PYMT_MTHD_1_AMT': payment_amount_1,
                'PYMT_MTHD_2': payment_method_2 if payment_method_2 != "Select Method" else None,
                'PYMT_MTHD_2_AMT': payment_amount_2,
                'Employee1 ID': get_employee_id(selected_employees[0]) if len(selected_employees) > 0 else None,
                'Employee2 ID': get_employee_id(selected_employees[1]) if len(selected_employees) > 1 else None,
                'Employee3 ID': get_employee_id(selected_employees[2]) if len(selected_employees) > 2 else None,
                'Start Time': start_time.strftime('%H:%M:%S'),
                'End Time': end_time.strftime('%H:%M:%S'),
                'Transaction Date': datetime.now().date(),
                'Transaction Time': datetime.now().time(),
                'COMMENTS': notes,
                'Service2 ID': None,  # Add if needed
                'Service3 ID': None,  # Add if needed
            }

            if save_transaction(transaction_data):
                st.success("Transaction completed successfully!")
                
                # Generate receipt
                receipt_data = {
                    "customer_name": selected_service['CUSTOMER_NAME'],
                    "service_date": selected_service['SERVICE_DATE'],
                    "services": [selected_service['SERVICE_NAME']],
                    "total_cost": service_cost,
                    "deposit": deposit,
                    "payment1": payment_amount_1,
                    "payment1_method": payment_method_1,
                    "payment2": payment_amount_2,
                    "payment2_method": payment_method_2,
                    "discount": 0.0,
                    "final_total_received": final_total_received,
                    "remaining_balance": final_remaining_balance,
                    "notes": notes
                }
                generate_receipt(receipt_data)
                
                # Clear session state and redirect
                for key in ['service_start_time', 'selected_service']:
                    st.session_state.pop(key, None)
                    
                st.session_state['page'] = 'scheduled_services'
                st.rerun()
            else:
                st.error("Failed to complete transaction. Please try again.")

    with col2:
        if st.button("Cancel", type="secondary", key="cancel_transaction"):
            # Clear session state and return to scheduled services
            for key in ['service_start_time', 'selected_service']:
                st.session_state.pop(key, None)
            st.session_state['page'] = 'scheduled_services'
            st.rerun()

# # Supporting functions

def fetch_upcoming_services(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Fetch upcoming services scheduled between the specified start and end dates.

    Args:
        start_date (date): The start date for filtering upcoming services.
        end_date (date): The end date for filtering upcoming services.

    Returns:
        pd.DataFrame: DataFrame containing details of upcoming services.
    """
    query = """
    SELECT 
        US.SERVICE_ID,
        COALESCE(C.CUSTOMER_ID, A.ACCOUNT_ID) AS CUSTOMER_OR_ACCOUNT_ID,
        COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
        US.SERVICE_NAME,
        US.SERVICE_DATE,
        US.SERVICE_TIME,
        US.NOTES,
        US.DEPOSIT,  -- Corrected to use actual column name `DEPOSIT`
        CASE 
            WHEN C.CUSTOMER_ID IS NOT NULL THEN 'Residential'
            ELSE 'Commercial'
        END AS SERVICE_TYPE
    FROM OPERATIONAL.CARPET.UPCOMING_SERVICES US
    LEFT JOIN OPERATIONAL.CARPET.CUSTOMER C ON US.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN OPERATIONAL.CARPET.ACCOUNTS A ON US.ACCOUNT_ID = A.ACCOUNT_ID
    WHERE US.SERVICE_DATE BETWEEN :1 AND :2
    ORDER BY US.SERVICE_DATE, US.SERVICE_TIME
    """

    try:
        # Execute the query and pass the date range parameters
        results = snowflake_conn.execute_query(query, [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')])

        # Return results as DataFrame if results are found, else return an empty DataFrame
        return pd.DataFrame(results) if results else pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching upcoming services: {str(e)}")
        return pd.DataFrame()

def fetch_transaction_details(transaction_id: int) -> Optional[dict]:
    """
    Fetches transaction details from the database based on transaction ID.
    """
    query = """
    SELECT ST.ID, ST.SERVICE_DATE, ST.SERVICE_TIME, ST.CUSTOMER_ID,
           COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
           S.SERVICE_NAME, ST.AMOUNT, ST.DISCOUNT, ST.DEPOSIT, ST.AMOUNT_RECEIVED, ST.NOTES
    FROM SERVICE_TRANSACTION ST
    JOIN SERVICES S ON ST.SERVICE_ID = S.SERVICE_ID
    LEFT JOIN CUSTOMER C ON ST.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN ACCOUNTS A ON ST.ACCOUNT_ID = A.ACCOUNT_ID
    WHERE ST.ID = :1
    """
    result = snowflake_conn.execute_query(query, [transaction_id])
    return result[0] if result else None

def update_transaction_details(data: dict) -> bool:
    """
    Updates the SERVICE_TRANSACTION table with the completed transaction details.
    """
    query = """
    UPDATE SERVICE_TRANSACTION
    SET EMPLOYEE1_ID = :1, 
        AMOUNT_RECEIVED = :2, 
        START_TIME = :3, 
        END_TIME = :4, 
        COMMENTS = :5, 
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE ID = :6
    """
    employee_ids = data["employee_ids"] + [None] * (3 - len(data["employee_ids"]))
    params = [*employee_ids, data["amount_received"], data["start_time"], data["end_time"], data["notes"], data["transaction_id"]]
    try:
        snowflake_conn.execute_query(query, params)
        return True
    except Exception as e:
        st.error(f"Error updating transaction: {e}")
        return False

def generate_receipt(data: dict) -> None:
    """
    Generates a downloadable receipt for the transaction.
    """
    receipt_text = format_receipt(data)
    st.download_button("Download Receipt", data=receipt_text, file_name="receipt.txt", mime="text/plain")
    st.text_area("Receipt Preview", value=receipt_text, height=300)

def format_receipt(data: dict) -> str:
    """
    Formats a receipt as a plain text string.
    """
    receipt = f"""
    EZ Biz Service Receipt
    ----------------------
    Customer: {data["customer_name"]}
    Service Date: {data["service_date"]}

    Services:
    {chr(10).join(f"- {service}" for service in data["services"])}

    Payment Details:
    ----------------
    Total Cost: ${data["total_cost"]:.2f}
    Deposit: ${data["deposit"]:.2f}
    Payment 1: ${data["payment1"]:.2f} ({data["payment1_method"]})
    """
    if data["payment2"] > 0:
        receipt += f"Payment 2: ${data['payment2']:.2f} ({data['payment2_method']})\n"
    receipt += f"""
    Discount: ${data["discount"]:.2f}
    Final Total Received: ${data["final_total_received"]:.2f}
    Remaining Balance: ${data["remaining_balance"]:.2f}

    Notes:
    {data["notes"]}
    """
    return receipt


# -------------------------------
# Session State Management
# -------------------------------
def initialize_session_state():
    if 'page' not in st.session_state:
        st.session_state['page'] = 'service_selection'
    if 'customer_details' not in st.session_state:
        st.session_state['customer_details'] = {}
    if 'selected_services' not in st.session_state:
        st.session_state['selected_services'] = []
    if 'selected_employees' not in st.session_state:
        st.session_state['selected_employees'] = []

# -------------------------------
# Main Application
# -------------------------------

def main():
    initialize_session_state()
    
    st.markdown("""
        <div class="main-header">
            <h1>EZ Biz</h1>
            <p>Service Management</p>
        </div>
    """, unsafe_allow_html=True)
    
    if st.session_state['page'] == 'service_selection':
        st.markdown('<div class="main-container">', unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("📝 New",
                        key='new_service',
                        help='Schedule a new service',
                        use_container_width=True):
                st.session_state['page'] = 'new_service'
                st.rerun()
        
        with col2:
            if st.button("📅 Scheduled",
                        key='scheduled_services',
                        help='View and manage scheduled services',
                        use_container_width=True):
                st.session_state['page'] = 'scheduled_services'
                st.rerun()
        
        with col3:
            if st.button("✓ Completed",
                        key='completed_services',
                        help='View completed services and transactions',
                        use_container_width=True):
                st.session_state['page'] = 'completed_services'
                st.rerun()
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    else:
        if st.button('← Back to Home', key='back_home'):
            st.session_state['page'] = 'service_selection'
            st.rerun()
        
        pages = {
            'new_service': new_service_page,
            'scheduled_services': scheduled_services_page,
            'completed_services': completed_services_page,
            'transaction_details': transaction_details_page
        }
        
        if st.session_state['page'] in pages:
            pages[st.session_state['page']]()

if __name__ == "__main__":
    main()
