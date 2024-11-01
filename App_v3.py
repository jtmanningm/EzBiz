import streamlit as st
import pandas as pd
from datetime import datetime, date, time, timedelta
import base64
from snowflake.snowpark.functions import col
from snowflake.snowpark import Session
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import os


# @app.route("/")
# def home():
#     return "Hello from Flask!"

# if __name__ == "__main__":
#     app.run(host="192.168.1.71", port=5000)  # Change 'localhost' to '0.0.0.0'


# Page configuration
st.set_page_config(
    page_title="EZ Biz",
    page_icon="🧹",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
    <style>
        /* Center container */
        .main-container {
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }
        
        /* Header styling */
        .main-header {
            text-align: center;
            padding: 10px 0;
            margin-bottom: 30px;
            color: #333;
        }
        
        .main-header h1 {
            font-size: 1.5rem;
            margin: 0;
            font-weight: 500;
        }
        
        .main-header p {
            font-size: 0.9rem;
            margin: 5px 0 0 0;
            color: #666;
        }
        
        /* Button styling */
        .stButton > button {
            width: 100%;
            height: 80px;
            background-color: white;
            border: 1px solid #eee;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            transition: all 0.2s ease;
            margin: 8px 0;
            padding: 15px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
            font-size: 0.9rem;
            color: #333;
        }
        
        .stButton > button:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            border-color: #ddd;
        }
        
        /* Hide fullscreen button */
        [data-testid="StyledFullScreenButton"] {
            display: none;
        }
        
        /* Mobile optimization */
        @media screen and (max-width: 768px) {
            .main-container {
                padding: 15px;
            }
            
            .stButton > button {
                height: 70px;
                font-size: 0.85rem;
            }
        }
    </style>
""", unsafe_allow_html=True)

# Initialize session state
def initialize_session_state():
    if 'page' not in st.session_state:
        st.session_state['page'] = 'service_selection'
    if 'customer_details' not in st.session_state:
        st.session_state['customer_details'] = {}
    if 'selected_services' not in st.session_state:
        st.session_state['selected_services'] = []
    if 'selected_employees' not in st.session_state:
        st.session_state['selected_employees'] = []

# Snowflake connection setup
private_key_passphrase = 'Lizard24'
if private_key_passphrase:
    private_key_passphrase = private_key_passphrase.encode()

with open(os.path.expanduser('~/Documents/Key/rsa_key.p8'), 'rb') as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=private_key_passphrase,
        backend=default_backend()
    )

private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

connection_parameters = {
    "account": "uvfnphy-okb79182",
    "user": "JTMANNINGM",
    "private_key": private_key_bytes,
    "role": "ACCOUNTADMIN",
    "warehouse": "COMPUTE_WH",
    "database": "OPERATIONAL",
    "schema": "CARPET"
}

try:
    session = Session.builder.configs(connection_parameters).create()
except Exception as e:
    st.error(f"Failed to create Snowpark session: {e}")
    session = None

# Database query execution
def execute_sql(query, params=None):
    try:
        result = session.sql(query, params).collect()
        return result
    except Exception as e:
        st.error(f"Error executing SQL: {e}")
        return None

@st.cache_data
def fetch_existing_customers():
    try:
        customers_df = session.table("CUSTOMER").select(
            "CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "STREET_ADDRESS", 
            "PHONE_NUMBER", "EMAIL_ADDRESS", "PRIMARY_CONTACT_METHOD"
        ).to_pandas()
        if not customers_df.empty:
            customers_df['FULL_NAME'] = customers_df['FIRST_NAME'] + " " + customers_df['LAST_NAME']
        return customers_df
    except Exception as e:
        st.error(f"Error fetching customers: {e}")
        return pd.DataFrame()

@st.cache_data
def fetch_existing_services():
    try:
        services_df = session.table("SERVICES").select(
            "SERVICE_ID", "SERVICE_NAME", "COST"
        ).to_pandas()
        return services_df
    except Exception as e:
        st.error(f"Error fetching services: {e}")
        return pd.DataFrame()

@st.cache_data
def fetch_existing_employees():
    try:
        employees_df = session.table("EMPLOYEE").select(
            "EMPLOYEE_ID", "FIRST_NAME", "LAST_NAME", "HOURLY_WAGE"
        ).to_pandas()
        employees_df['FULL_NAME'] = employees_df['FIRST_NAME'] + " " + employees_df['LAST_NAME']
        return employees_df
    except Exception as e:
        st.error(f"Error fetching employees: {e}")
        return pd.DataFrame()

def save_service_schedule(customer_id, services, employees, service_date, service_time, 
                        is_recurring, recurrence_pattern=None, notes=None, deposit_info=None):
    """
    Save service schedule and create initial transaction record
    """
    try:
        if not customer_id or not services:
            st.error("Customer ID and at least one service are required")
            return False

        # Get service IDs and calculate total cost
        services_df = fetch_existing_services()
        service_ids = [None, None, None]  # Initialize array for up to 3 services
        total_cost = 0
        
        # Debug services
        st.write("=== Debug Information ===")
        st.write(f"Services to save: {services}")
        
        # Process each service (up to 3)
        for idx, service_name in enumerate(services[:3]):
            service_row = services_df[services_df['SERVICE_NAME'] == service_name]
            if not service_row.empty:
                service_ids[idx] = int(service_row.iloc[0]['SERVICE_ID'])
                cost = float(service_row.iloc[0]['COST'])
                total_cost += cost
                st.write(f"Service {idx+1}: {service_name} -> ID={service_ids[idx]}, Cost=${cost:.2f}")

        # Get employee IDs
        employees_df = fetch_existing_employees()
        employee_ids = [None, None, None]  # Initialize array for up to 3 employees
        
        # Process each employee (up to 3)
        for idx, employee_name in enumerate(employees[:3]):
            employee_row = employees_df[employees_df['FULL_NAME'] == employee_name]
            if not employee_row.empty:
                employee_ids[idx] = int(employee_row.iloc[0]['EMPLOYEE_ID'])
                st.write(f"Employee {idx+1}: {employee_name} -> ID={employee_ids[idx]}")

        # Handle deposit information
        deposit_amount = 0
        deposit_method = None
        if deposit_info and 'amount' in deposit_info and deposit_info['amount'] > 0:
            deposit_amount = float(deposit_info['amount'])
            deposit_method = deposit_info.get('payment_method')
            st.write(f"Processing deposit: ${deposit_amount:.2f} via {deposit_method}")

        # Format dates for database
        formatted_date = service_date.strftime('%Y-%m-%d')
        formatted_time = service_time.strftime('%H:%M:%S')

        # Create transaction record
        transaction_query = """
        INSERT INTO SERVICE_TRANSACTION (
            CUSTOMER_ID,
            SERVICE_ID,
            SERVICE2_ID,
            SERVICE3_ID,
            EMPLOYEE1_ID,
            EMPLOYEE2_ID,
            EMPLOYEE3_ID,
            TRANSACTION_DATE,
            TRANSACTION_TIME,
            AMOUNT,
            DEPOSIT,
            PYMT_MTHD_1,
            PYMT_MTHD_1_AMT
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13
        )
        """
        
        transaction_params = [
            customer_id,
            service_ids[0],
            service_ids[1],
            service_ids[2],
            employee_ids[0],
            employee_ids[1],
            employee_ids[2],
            formatted_date,
            formatted_time,
            total_cost,
            deposit_amount,
            deposit_method,
            deposit_amount
        ]
        
        # Debug output before insert
        st.write("\nFinal values for insert:")
        st.write(f"Service IDs: {service_ids}")
        st.write(f"Employee IDs: {employee_ids}")
        st.write(f"Total Cost: ${total_cost:.2f}")
        st.write(f"Deposit Amount: ${deposit_amount:.2f}")
        st.write(f"Payment Method: {deposit_method}")
        
        execute_sql(transaction_query, transaction_params)

        # Create upcoming service record
        service_query = """
        INSERT INTO UPCOMING_SERVICES (
            CUSTOMER_ID,
            SERVICE_NAME,
            SERVICE_DATE,
            SERVICE_TIME,
            IS_RECURRING,
            RECURRENCE_PATTERN,
            NOTES
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7
        )
        """
        
        service_params = [
            int(customer_id),
            services[0],  # Primary service name
            formatted_date,
            formatted_time,
            is_recurring,
            recurrence_pattern if is_recurring else None,
            notes
        ]
        
        execute_sql(service_query, service_params)

        # Handle recurring services
        if is_recurring and recurrence_pattern:
            schedule_recurring_services(
                customer_id=customer_id,
                service_name=services[0],
                service_date=service_date,
                service_time=service_time,
                recurrence_pattern=recurrence_pattern,
                notes=notes
            )

        return True

    except Exception as e:
        st.error(f"Error scheduling service: {e}")
        import traceback
        st.write("Full error details:")
        st.write(traceback.format_exc())
        return False


def update_service_transaction(transaction_id, update_data):
    """
    Update an existing service transaction with new data
    """
    try:
        query = """
        UPDATE SERVICE_TRANSACTION
        SET 
            SERVICE2_ID = :1,
            SERVICE3_ID = :2,
            EMPLOYEE1_ID = :3,
            EMPLOYEE2_ID = :4,
            EMPLOYEE3_ID = :5,
            AMOUNT = :6,
            DISCOUNT = :7,
            AMOUNT_RECEIVED = :8,
            PYMT_MTHD_1 = :9,
            PYMT_MTHD_1_AMT = :10,
            PYMT_MTHD_2 = :11,
            PYMT_MTHD_2_AMT = :12,
            START_TIME = :13,
            END_TIME = :14,
            COMMENTS = :15,
            STATUS = :16,
            LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
        WHERE ID = :17
        """
        
        params = [
            update_data['service2_id'],
            update_data['service3_id'],
            update_data['employee1_id'],
            update_data['employee2_id'],
            update_data['employee3_id'],
            update_data['total_amount'],
            update_data['discount'],
            update_data['amount_received'],
            update_data['payment_method1'],
            update_data['payment_amount1'],
            update_data['payment_method2'],
            update_data['payment_amount2'],
            update_data['start_time'],
            update_data['end_time'],
            update_data['comments'],  # Changed from 'notes' to 'comments'
            update_data['status'],
            transaction_id
        ]

        # Execute the update
        execute_sql(query, params)

        # Debug output
        st.write("Debug - Update parameters:")
        st.write(f"Transaction ID: {transaction_id}")
        st.write(f"Services: {update_data['service2_id']}, {update_data['service3_id']}")
        st.write(f"Employees: {update_data['employee1_id']}, {update_data['employee2_id']}, {update_data['employee3_id']}")
        st.write(f"Payments: {update_data['payment_method1']}: ${update_data['payment_amount1']}, {update_data['payment_method2']}: ${update_data['payment_amount2']}")
        st.write(f"Comments: {update_data['comments']}")
        
        return True

    except Exception as e:
        st.error(f"Error in update_service_transaction: {str(e)}")
        # Print the full error traceback for debugging
        import traceback
        st.write("Full error details:")
        st.write(traceback.format_exc())
        return False

def save_new_service(customer_id, services, employees, service_date, service_time, deposit_info=None, notes=None, is_recurring=False, recurrence_pattern=None):
    """
    Save a new service directly to the database with all related details
    """
    try:
        # Get service IDs
        services_df = fetch_existing_services()
        service_ids = []
        for service in services[:3]:  # Limit to 3 services
            service_row = services_df[services_df['SERVICE_NAME'] == service]
            if not service_row.empty:
                service_ids.append(int(service_row.iloc[0]['SERVICE_ID']))
        
        # Pad service_ids with None if less than 3
        while len(service_ids) < 3:
            service_ids.append(None)

        # Get employee IDs
        employees_df = fetch_existing_employees()
        employee_ids = []
        for employee in employees[:3]:  # Limit to 3 employees
            employee_row = employees_df[employees_df['FULL_NAME'] == employee]
            if not employee_row.empty:
                employee_ids.append(int(employee_row.iloc[0]['EMPLOYEE_ID']))
        
        # Pad employee_ids with None if less than 3
        while len(employee_ids) < 3:
            employee_ids.append(None)

        # Create record in SERVICE_TRANSACTION
        query = """
        INSERT INTO SERVICE_TRANSACTION (
            CUSTOMER_ID,
            SERVICE_ID,
            SERVICE2_ID,
            SERVICE3_ID,
            EMPLOYEE1_ID,
            EMPLOYEE2_ID,
            EMPLOYEE3_ID,
            SERVICE_DATE,
            SERVICE_TIME,
            DEPOSIT,
            PYMT_MTHD_1,
            PYMT_MTHD_1_AMT,
            NOTES,
            IS_RECURRING,
            RECURRENCE_PATTERN,
            CREATED_DATE
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, CURRENT_TIMESTAMP()
        )
        """
        
        # If there's a deposit, use it, otherwise set to 0
        deposit_amount = deposit_info['amount'] if deposit_info else 0
        
        params = [
            customer_id,
            service_ids[0],
            service_ids[1],
            service_ids[2],
            employee_ids[0],
            employee_ids[1],
            employee_ids[2],
            service_date.strftime('%Y-%m-%d'),
            service_time.strftime('%H:%M:%S'),
            deposit_amount,
            deposit_info['payment_method'] if deposit_info else None,
            deposit_amount,
            notes,
            is_recurring,
            recurrence_pattern
        ]
        
        execute_sql(query, params)
        return True
    except Exception as e:
        st.error(f"Error saving service: {e}")
        return False

def new_service_page():
    """
    Display the new service scheduling page with customer search and deposit handling.
    """
    st.markdown("""
        <div class="page-header">
            <h2>New Service</h2>
        </div>
    """, unsafe_allow_html=True)

    # Fetch existing customers for search
    existing_customers_df = fetch_existing_customers()
    customer_names = []
    if not existing_customers_df.empty:
        customer_names = existing_customers_df['FULL_NAME'].tolist()

    with st.container():
        st.markdown("### Customer Information")
        
        # Customer search with autocomplete
        customer_name = st.text_input(
            "Customer Name", 
            help="Enter customer name to search or add new"
        )

        matching_customers = []
        selected_customer = None
        
        if customer_name:
            matching_customers = [
                name for name in customer_names 
                if customer_name.lower() in name.lower()
            ]
            if matching_customers:
                selected_customer = st.selectbox(
                    "Select Existing Customer",
                    options=["Select..."] + matching_customers
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

        # New customer details form
        if not selected_customer or selected_customer == "Select...":
            col1, col2, col3 = st.columns(3)
            with col1:
                phone = st.text_input("Phone Number")
            with col2:
                email = st.text_input("Email")
            with col3:
                contact_preference = st.selectbox(
                    "Preferred Contact Method",
                    ["Phone", "Text", "Email"]
                )

            st.markdown("### Service Address")
            col1, col2 = st.columns(2)
            with col1:
                street_address = st.text_input("Street Address")
                city = st.text_input("City")
            with col2:
                state = st.text_input("State")
                zip_code = st.text_input("ZIP Code")

        # Service Details
        st.markdown("### Service Details")
        col1, col2 = st.columns(2)
        with col1:
            service_date = st.date_input(
                "Service Date",
                min_value=datetime.now().date()
            )
        with col2:
            service_time = st.time_input("Service Time")

        # Service Selection
        services_df = fetch_existing_services()
        if not services_df.empty:
            selected_services = st.multiselect(
                "Select Services",
                options=services_df['SERVICE_NAME'].tolist(),
                help="Choose one or more services"
            )

            # Display service prices
            if selected_services:
                st.markdown("### Service Prices")
                total_price = 0
                for service_name in selected_services:
                    service_cost = services_df[
                        services_df['SERVICE_NAME'] == service_name
                    ]['COST'].iloc[0]
                    st.write(f"{service_name}: ${service_cost:.2f}")
                    total_price += service_cost
                st.write(f"**Total Price: ${total_price:.2f}**")
        else:
            st.error("No services available. Please add services first.")
            return

        # Employee Assignment
        employees_df = fetch_existing_employees()
        selected_employees = st.multiselect(
            "Assign Employees",
            options=employees_df['FULL_NAME'].tolist() if not employees_df.empty else [],
            help="Assign employees to this service"
        )

        # Recurring Service Options
        col1, col2 = st.columns(2)
        with col1:
            is_recurring = st.checkbox("Recurring Service")
        with col2:
            if is_recurring:
                recurrence_pattern = st.selectbox(
                    "Recurrence Pattern",
                    ["Weekly", "Bi-Weekly", "Monthly"]
                )
            else:
                recurrence_pattern = None

        # Deposit Section
        st.markdown("### Deposit Information")
        collect_deposit = st.checkbox("Collect Deposit")
        deposit_info = None
        
        if collect_deposit:
            col1, col2 = st.columns(2)
            with col1:
                deposit_amount = st.number_input(
                    "Deposit Amount",
                    min_value=0.0,
                    max_value=total_price if 'total_price' in locals() else 0.0,
                    step=0.01
                )
            with col2:
                deposit_method = st.selectbox(
                    "Payment Method",
                    ["Cash", "Credit Card", "Debit Card", "Check", "CashApp", "Venmo", "Zelle", "Other"]
                )
            
            if deposit_amount > 0:
                deposit_info = {
                    'amount': deposit_amount,
                    'payment_method': deposit_method,
                    'date': datetime.now().date(),
                    'time': datetime.now().time()
                }

        # Additional Notes
        additional_notes = st.text_area(
            "Additional Notes",
            help="Enter any special instructions or notes for this service"
        )

        # Submit Button
        # Submit Button
    if st.button("Schedule Service", type="primary", use_container_width=True):
        if not selected_services:
            st.error("Please select at least one service.")
            return

        try:
            # Get or create customer ID
            if st.session_state.get('selected_customer_id'):
                customer_id = st.session_state['selected_customer_id']
            else:
                customer_id = save_customer_info(
                    customer_name, phone, email, street_address,
                    city, state, zip_code, contact_preference
                )

            if customer_id:
                # Schedule the service
                success = save_service_schedule(
                    customer_id=customer_id,
                    services=selected_services,
                    employees=selected_employees,
                    service_date=service_date,
                    service_time=service_time,
                    is_recurring=is_recurring,
                    recurrence_pattern=recurrence_pattern if is_recurring else None,
                    notes=additional_notes,
                    deposit_info=deposit_info
                )

                if success:
                    success_message = "Service scheduled successfully!"
                    if deposit_info:
                        success_message += f"\nDeposit of ${deposit_info['amount']:.2f} recorded"
                    st.success(success_message)
                    
                    # Navigate to scheduled services page
                    st.session_state['page'] = 'scheduled_services'
                    st.rerun()
                else:
                    st.error("Failed to schedule service. Please try again.")
            else:
                st.error("Failed to save customer information. Please try again.")
        except Exception as e:
            st.error(f"Error scheduling service: {e}")

def schedule_recurring_services(customer_id, service_name, service_date, service_time, 
                              recurrence_pattern, notes):
    """
    Schedule recurring services based on the pattern
    """
    try:
        future_dates = []
        current_date = service_date

        # Calculate future dates based on recurrence pattern
        for _ in range(6):  # Schedule next 6 occurrences
            if recurrence_pattern == "Weekly":
                current_date = current_date + timedelta(days=7)
            elif recurrence_pattern == "Bi-Weekly":
                current_date = current_date + timedelta(days=14)
            elif recurrence_pattern == "Monthly":
                current_date = current_date + timedelta(days=30)
            future_dates.append(current_date)

        # Insert each recurring service
        for future_date in future_dates:
            service_query = """
            INSERT INTO UPCOMING_SERVICES (
                CUSTOMER_ID,
                SERVICE_NAME,
                SERVICE_DATE,
                SERVICE_TIME,
                IS_RECURRING,
                RECURRENCE_PATTERN,
                NOTES
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7
            )
            """
            
            params = [
                int(customer_id),
                service_name,
                future_date.strftime('%Y-%m-%d'),
                service_time.strftime('%H:%M:%S'),
                True,
                recurrence_pattern,
                notes
            ]

            execute_sql(service_query, params)

    except Exception as e:
        st.error(f"Error scheduling recurring services: {e}")

def get_service_id(service_name):
    """
    Get the service ID from a service name.
    """
    services_df = fetch_existing_services()
    if not services_df.empty:
        service_row = services_df[services_df['SERVICE_NAME'] == service_name]
        if not service_row.empty:
            return int(service_row.iloc[0]['SERVICE_ID'])  # Convert to int
    return None

def get_employee_id(employee_name):
    """
    Get the employee ID from an employee name.
    """
    employees_df = fetch_existing_employees()
    if not employees_df.empty:
        employee_row = employees_df[employees_df['FULL_NAME'] == employee_name]
        if not employee_row.empty:
            return employee_row.iloc[0]['EMPLOYEE_ID']
    return None

def save_customer_info(name, phone, email, address, city, state, zip_code, contact_preference, account_type=None):
    try:
        name_parts = name.split(' ', 1)
        first_name = name_parts[0]
        last_name = name_parts[1] if len(name_parts) > 1 else ""

        # Set TEXT_FLAG based on contact preference
        text_flag = contact_preference.upper() == "TEXT"

        # Convert zip_code to integer or None if empty
        zip_code_int = None
        if zip_code:
            try:
                zip_code_int = int(zip_code)
            except ValueError:
                st.warning("ZIP code must be a number. Saving as empty.")

        query = """
        INSERT INTO CUSTOMER (
            FIRST_NAME, 
            LAST_NAME, 
            STREET_ADDRESS,
            CITY,
            STATE,
            ZIP_CODE,
            EMAIL_ADDRESS,
            PHONE_NUMBER,
            TEXT_FLAG,
            SERVICE_ADDRESS,
            PRIMARY_CONTACT_METHOD
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11
        )
        """
        params = [
            first_name,
            last_name,
            address,
            city,
            state,
            zip_code_int,  # Using the converted zip code
            email,
            phone,
            text_flag,
            address,  # Using street_address as service_address
            contact_preference
        ]
        
        execute_sql(query, params)
        
        # Get the newly created customer ID
        result = execute_sql("""
            SELECT CUSTOMER_ID 
            FROM CUSTOMER 
            WHERE FIRST_NAME = :1 
            AND LAST_NAME = :2 
            ORDER BY CREATED_AT DESC 
            LIMIT 1
        """, [first_name, last_name])
        
        if result:
            return result[0]['CUSTOMER_ID']
        return None
    except Exception as e:
        st.error(f"Error saving customer information: {e}")
        return None

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
            'transaction_details': transaction_details_page  # Add transaction_details_page here
        }
        
        if st.session_state['page'] in pages:
            pages[st.session_state['page']]()

def handle_deposit():
    """Handle deposit collection"""
    with st.expander("Collect Deposit"):
        deposit_amount = st.number_input("Deposit Amount", min_value=0.0, step=0.01)
        payment_method = st.selectbox(
            "Payment Method",
            ["Cash", "Credit Card", "Debit Card", "Check", "CashApp", "Venmo", "Zelle", "Other"]
        )
        
        if deposit_amount > 0:
            return {
                'amount': deposit_amount,
                'payment_method': payment_method
            }
        return None

def get_or_create_customer(customer_name, phone, email, address, city, state, zip_code, contact_preference):
    existing_customers_df = fetch_existing_customers()
    customer = existing_customers_df[(existing_customers_df['FULL_NAME'] == customer_name) & 
                                     (existing_customers_df['EMAIL_ADDRESS'] == email)]
    if not customer.empty:
        return int(customer.iloc[0]['CUSTOMER_ID'])
    
    # Insert new customer if not found
    customer_id = save_customer_info(customer_name, phone, email, address, city, state, zip_code, contact_preference)
    return customer_id

def create_initial_transaction(customer_id, services, employees, service_date, service_time, deposit_info=None, notes=None):
    """
    Create a new service transaction and return its ID
    """
    try:
        # Get service IDs
        services_df = fetch_existing_services()
        service_ids = []
        # Changed loop variable from 'service' to 'service_name'
        for service_name in services[:3]:
            service_row = services_df[services_df['SERVICE_NAME'] == service_name]
            if not service_row.empty:
                service_ids.append(int(service_row.iloc[0]['SERVICE_ID']))
        
        # Pad service_ids with None if less than 3
        while len(service_ids) < 3:
            service_ids.append(None)

        # Get employee IDs
        employees_df = fetch_existing_employees()
        employee_ids = []
        # Changed loop variable from 'employee' to 'employee_name'
        for employee_name in employees[:3]:
            employee_row = employees_df[employees_df['FULL_NAME'] == employee_name]
            if not employee_row.empty:
                employee_ids.append(int(employee_row.iloc[0]['EMPLOYEE_ID']))
        
        # Pad employee_ids with None if less than 3
        while len(employee_ids) < 3:
            employee_ids.append(None)

        # Calculate initial total cost
        total_cost = 0
        for service_name in services:
            service_cost = float(services_df[services_df['SERVICE_NAME'] == service_name]['COST'].iloc[0])
            total_cost += service_cost

        query = """
        INSERT INTO SERVICE_TRANSACTION (
            CUSTOMER_ID,
            SERVICE_ID,
            SERVICE2_ID,
            SERVICE3_ID,
            EMPLOYEE1_ID,
            EMPLOYEE2_ID,
            EMPLOYEE3_ID,
            SERVICE_DATE,
            SERVICE_TIME,
            AMOUNT,
            DEPOSIT,
            PYMT_MTHD_1,
            PYMT_MTHD_1_AMT,
            NOTES,
            STATUS,
            CREATED_DATE
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, CURRENT_TIMESTAMP()
        ) RETURNING ID
        """
        
        params = [
            customer_id,
            service_ids[0],
            service_ids[1],
            service_ids[2],
            employee_ids[0],
            employee_ids[1],
            employee_ids[2],
            service_date.strftime('%Y-%m-%d'),
            service_time.strftime('%H:%M:%S'),
            total_cost,
            deposit_info['amount'] if deposit_info else 0,
            deposit_info['payment_method'] if deposit_info else None,
            deposit_info['amount'] if deposit_info else 0,
            notes,
            'Scheduled'
        ]

        result = execute_sql(query, params)
        return result[0]['ID'] if result else None

    except Exception as e:
        st.error(f"Error creating service transaction: {e}")
        return None


def save_initial_transaction(customer_id, services, employees, service_date, service_time, deposit_info=None):
    """Create initial service transaction record with services, employees, and deposit"""
    try:
        # Clear debug output
        st.write("=== Debug Information ===")
        st.write(f"Services to save: {services}")
        st.write(f"Deposit info: {deposit_info}")

        # Get service IDs and calculate total cost
        services_df = fetch_existing_services()
        service_ids = [None, None, None]  # Initialize with None
        total_cost = 0

        # Debug each service lookup
        for idx, service_name in enumerate(services[:3]):
            st.write(f"Looking up service: {service_name}")
            service_row = services_df[services_df['SERVICE_NAME'] == service_name]
            if not service_row.empty:
                service_ids[idx] = int(service_row.iloc[0]['SERVICE_ID'])
                cost = float(service_row.iloc[0]['COST'])
                total_cost += cost
                st.write(f"Found service {idx+1}: ID={service_ids[idx]}, Cost=${cost:.2f}")

        # Handle deposit
        deposit_amount = 0
        deposit_method = None
        if deposit_info and 'amount' in deposit_info and deposit_info['amount'] > 0:
            deposit_amount = float(deposit_info['amount'])
            deposit_method = deposit_info.get('payment_method')
            st.write(f"Processing deposit: ${deposit_amount:.2f} via {deposit_method}")

        # Get employee IDs
        employees_df = fetch_existing_employees()
        employee_ids = [None, None, None]
        for idx, employee_name in enumerate(employees[:3]):
            employee_row = employees_df[employees_df['FULL_NAME'] == employee_name]
            if not employee_row.empty:
                employee_ids[idx] = int(employee_row.iloc[0]['EMPLOYEE_ID'])
                st.write(f"Found employee {idx+1}: ID={employee_ids[idx]}")

        # Construct the query with proper Snowflake parameter syntax
        query = """
        INSERT INTO SERVICE_TRANSACTION (
            CUSTOMER_ID,
            SERVICE_ID,
            SERVICE2_ID,
            SERVICE3_ID,
            EMPLOYEE1_ID,
            EMPLOYEE2_ID,
            EMPLOYEE3_ID,
            TRANSACTION_DATE,
            TRANSACTION_TIME,
            AMOUNT,
            DEPOSIT,
            PYMT_MTHD_1,
            PYMT_MTHD_1_AMT
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13
        )
        """

        current_date = datetime.now().date()
        current_time = datetime.now().time()

        # Create parameters list with explicit values
        params = [
            customer_id,                            # :1  CUSTOMER_ID
            service_ids[0],                         # :2  SERVICE_ID
            service_ids[1],                         # :3  SERVICE2_ID
            service_ids[2],                         # :4  SERVICE3_ID
            employee_ids[0],                        # :5  EMPLOYEE1_ID
            employee_ids[1],                        # :6  EMPLOYEE2_ID
            employee_ids[2],                        # :7  EMPLOYEE3_ID
            current_date.strftime('%Y-%m-%d'),      # :8  TRANSACTION_DATE
            current_time.strftime('%H:%M:%S'),      # :9  TRANSACTION_TIME
            total_cost,                             # :10 AMOUNT
            deposit_amount,                         # :11 DEPOSIT
            deposit_method,                         # :12 PYMT_MTHD_1
            deposit_amount                          # :13 PYMT_MTHD_1_AMT
        ]

        # Debug output before execution
        st.write("\nFinal values for insert:")
        st.write(f"Service IDs: {service_ids}")
        st.write(f"Employee IDs: {employee_ids}")
        st.write(f"Total Cost: ${total_cost:.2f}")
        st.write(f"Deposit Amount: ${deposit_amount:.2f}")
        st.write(f"Payment Method: {deposit_method}")

        # Execute the insert
        execute_sql(query, params)

        # Verify the insert with a select query
        verify_query = """
        SELECT 
            ID,
            SERVICE_ID,
            SERVICE2_ID,
            SERVICE3_ID,
            DEPOSIT,
            PYMT_MTHD_1,
            AMOUNT
        FROM SERVICE_TRANSACTION
        WHERE CUSTOMER_ID = :1
            AND TRANSACTION_DATE = :2
            AND TRANSACTION_TIME = :3
        ORDER BY CREATED_DATE DESC
        LIMIT 1
        """
        
        verify_params = [
            customer_id,
            current_date.strftime('%Y-%m-%d'),
            current_time.strftime('%H:%M:%S')
        ]
        
        verify_result = execute_sql(verify_query, verify_params)
        if verify_result:
            st.write("\nVerification of saved transaction:")
            st.write(verify_result[0])

        return True

    except Exception as e:
        st.error(f"Error in save_initial_transaction: {str(e)}")
        import traceback
        st.write("Full error details:")
        st.write(traceback.format_exc())
        return False

def verify_transaction_save(transaction_id):
    """Verify that a transaction was saved correctly"""
    query = """
    SELECT 
        ST.ID,
        ST.AMOUNT,
        ST.DEPOSIT,
        ST.PYMT_MTHD_1,
        S1.SERVICE_NAME as SERVICE1,
        S2.SERVICE_NAME as SERVICE2,
        S3.SERVICE_NAME as SERVICE3,
        E1.FIRST_NAME || ' ' || E1.LAST_NAME as EMPLOYEE1,
        E2.FIRST_NAME || ' ' || E2.LAST_NAME as EMPLOYEE2,
        E3.FIRST_NAME || ' ' || E3.LAST_NAME as EMPLOYEE3
    FROM SERVICE_TRANSACTION ST
    LEFT JOIN SERVICES S1 ON ST.SERVICE_ID = S1.SERVICE_ID
    LEFT JOIN SERVICES S2 ON ST.SERVICE2_ID = S2.SERVICE_ID
    LEFT JOIN SERVICES S3 ON ST.SERVICE3_ID = S3.SERVICE_ID
    LEFT JOIN EMPLOYEE E1 ON ST.EMPLOYEE1_ID = E1.EMPLOYEE_ID
    LEFT JOIN EMPLOYEE E2 ON ST.EMPLOYEE2_ID = E2.EMPLOYEE_ID
    LEFT JOIN EMPLOYEE E3 ON ST.EMPLOYEE3_ID = E3.EMPLOYEE_ID
    WHERE ST.ID = :1
    """
    
    result = execute_sql(query, [transaction_id])
    if result:
        return result[0]
    return None

def add_new_service_form():
    """Add new service form component"""
    with st.expander("Add New Service"):
        with st.form("new_service_form"):
            service_name = st.text_input("Service Name")
            service_category = st.selectbox(
                "Service Category",
                ["Residential", "Commercial", "Other"]
            )
            service_description = st.text_area("Service Description")
            cost = st.number_input("Base Cost", min_value=0.0, step=0.01)
            
            if st.form_submit_button("Add Service"):
                if service_name and cost:
                    query = """
                    INSERT INTO SERVICES (
                        SERVICE_NAME, 
                        SERVICE_CATEGORY, 
                        SERVICE_DESCRIPTION, 
                        COST, 
                        ACTIVE_STATUS
                    ) VALUES (
                        :1, :2, :3, :4, :5
                    )
                    """
                    try:
                        execute_sql(query, [
                            service_name,
                            service_category,
                            service_description,
                            cost,
                            True
                        ])
                        st.success("Service added successfully!")
                        # Clear the cache to refresh services list
                        fetch_existing_services.clear()
                        return True
                    except Exception as e:
                        st.error(f"Error adding service: {e}")
                        return False
                else:
                    st.error("Service name and cost are required")
                    return False

def scheduled_services_page():
    st.title('Scheduled Services')
    
    # Date range selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=datetime.now().date())
    with col2:
        end_date = st.date_input("End Date", value=datetime.now().date() + timedelta(days=30))

    query = """
    SELECT 
        US.SERVICE_ID,
        ST.ID AS TRANSACTION_ID,
        COALESCE(C.CUSTOMER_ID, A.ACCOUNT_ID) AS CUSTOMER_OR_ACCOUNT_ID,
        COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
        US.SERVICE_NAME,
        US.SERVICE_DATE,
        US.SERVICE_TIME,
        US.NOTES,
        ST.DEPOSIT,
        CASE 
            WHEN C.CUSTOMER_ID IS NOT NULL THEN 'Residential'
            ELSE 'Commercial'
        END AS SERVICE_TYPE
    FROM UPCOMING_SERVICES US
    LEFT JOIN SERVICE_TRANSACTION ST ON 
        ST.CUSTOMER_ID = US.CUSTOMER_ID 
        AND ST.TRANSACTION_DATE = US.SERVICE_DATE 
        AND ST.TRANSACTION_TIME = US.SERVICE_TIME
    LEFT JOIN CUSTOMER C ON US.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN ACCOUNTS A ON US.ACCOUNT_ID = A.ACCOUNT_ID
    WHERE US.SERVICE_DATE BETWEEN :1 AND :2
    ORDER BY US.SERVICE_DATE, US.SERVICE_TIME
    """
    
    services_df = pd.DataFrame(execute_sql(query, [start_date.strftime('%Y-%m-%d'), 
                                                 end_date.strftime('%Y-%m-%d')]))

    if not services_df.empty:
        # Display services in calendar view
        st.markdown("### Calendar View")
        current_date = None
        
        for _, row in services_df.sort_values(['SERVICE_DATE', 'SERVICE_TIME']).iterrows():
            if current_date != row['SERVICE_DATE']:
                current_date = row['SERVICE_DATE']
                st.markdown(f"**{current_date.strftime('%A, %B %d, %Y')}**")
            
            with st.container():
                col1, col2, col3 = st.columns([2,3,1])
                with col1:
                    st.write(f"🕒 {row['SERVICE_TIME'].strftime('%I:%M %p')}")
                with col2:
                    deposit_info = ""
                    if row['DEPOSIT'] and float(row['DEPOSIT']) > 0:
                        deposit_info = f"💰 Deposit: ${float(row['DEPOSIT']):.2f}\n"
                    
                    st.write(f"""
                    📋 {row['SERVICE_NAME']} - {row['CUSTOMER_NAME']}
                    {deposit_info}
                    {f"📝 {row['NOTES']}" if row['NOTES'] else ""}
                    """)
                with col3:
                    if st.button("✓ Start", 
                                key=f"start_{row['SERVICE_ID']}",
                                type="primary"):
                        # Only store transaction_id in session state when starting the service
                        st.session_state['service_transaction_id'] = row['TRANSACTION_ID']
                        st.session_state['page'] = 'transaction_details'
                        st.rerun()

def create_service_transaction(customer_id, service_name, service_date, service_time):
    """
    Create a service transaction record if one doesn't exist
    """
    try:
        # Get service ID and cost
        services_df = fetch_existing_services()
        service_row = services_df[services_df['SERVICE_NAME'] == service_name]
        if not service_row.empty:
            service_id = int(service_row.iloc[0]['SERVICE_ID'])
            service_cost = float(service_row.iloc[0]['COST'])
        else:
            st.error("Service not found")
            return False, None

        # Create transaction record
        query = """
        INSERT INTO SERVICE_TRANSACTION (
            CUSTOMER_ID,
            SERVICE_ID,
            TRANSACTION_DATE,
            TRANSACTION_TIME,
            AMOUNT
        )
        SELECT
            :1, :2, 
            TO_DATE(:3, 'YYYY-MM-DD'),
            TO_TIME(:4, 'HH24:MI:SS'),
            :5
        """
        
        params = [
            customer_id,
            service_id,
            service_date.strftime('%Y-%m-%d'),
            service_time.strftime('%H:%M:%S'),
            service_cost
        ]
        
        execute_sql(query, params)
        
        # Get the transaction ID
        id_query = """
        SELECT ID 
        FROM SERVICE_TRANSACTION 
        WHERE CUSTOMER_ID = :1 
        AND SERVICE_ID = :2 
        AND TRANSACTION_DATE = TO_DATE(:3, 'YYYY-MM-DD')
        AND TRANSACTION_TIME = TO_TIME(:4, 'HH24:MI:SS')
        ORDER BY CREATED_DATE DESC 
        LIMIT 1
        """
        
        result = execute_sql(id_query, params[:4])
        if result:
            return True, result[0]['ID']
        return False, None

    except Exception as e:
        st.error(f"Error creating service transaction: {e}")
        return False, None

def transaction_details_page():
    """
    Display and handle transaction details for a service, including updates and completion
    """
    st.title('Service Details')

    # Ensure we have a transaction ID
    if 'service_transaction_id' not in st.session_state:
        st.error("No service transaction selected.")
        return

    transaction_id = st.session_state['service_transaction_id']

    # Fetch current transaction details
    query = """
    SELECT 
        ST.*,
        S1.SERVICE_NAME as SERVICE1_NAME,
        S2.SERVICE_NAME as SERVICE2_NAME,
        S3.SERVICE_NAME as SERVICE3_NAME,
        S1.COST as SERVICE1_COST,
        S2.COST as SERVICE2_COST,
        S3.COST as SERVICE3_COST,
        E1.FIRST_NAME || ' ' || E1.LAST_NAME as EMPLOYEE1_NAME,
        E2.FIRST_NAME || ' ' || E2.LAST_NAME as EMPLOYEE2_NAME,
        E3.FIRST_NAME || ' ' || E3.LAST_NAME as EMPLOYEE3_NAME,
        C.FIRST_NAME || ' ' || C.LAST_NAME as CUSTOMER_NAME,
        C.CUSTOMER_ID
    FROM SERVICE_TRANSACTION ST
    LEFT JOIN SERVICES S1 ON ST.SERVICE_ID = S1.SERVICE_ID
    LEFT JOIN SERVICES S2 ON ST.SERVICE2_ID = S2.SERVICE_ID
    LEFT JOIN SERVICES S3 ON ST.SERVICE3_ID = S3.SERVICE_ID
    LEFT JOIN EMPLOYEE E1 ON ST.EMPLOYEE1_ID = E1.EMPLOYEE_ID
    LEFT JOIN EMPLOYEE E2 ON ST.EMPLOYEE2_ID = E2.EMPLOYEE_ID
    LEFT JOIN EMPLOYEE E3 ON ST.EMPLOYEE3_ID = E3.EMPLOYEE_ID
    LEFT JOIN CUSTOMER C ON ST.CUSTOMER_ID = C.CUSTOMER_ID
    WHERE ST.ID = :1
    """
    
    result = execute_sql(query, [transaction_id])
    if not result:
        st.error("Transaction details not found.")
        return

    transaction = result[0]

    # Display basic information
    st.subheader(f"Customer: {transaction['CUSTOMER_NAME']}")
    
    # Service Selection
    services_df = fetch_existing_services()
    current_services = [svc for svc in [
        transaction['SERVICE1_NAME'],
        transaction['SERVICE2_NAME'],
        transaction['SERVICE3_NAME']
    ] if svc]

    col1, col2 = st.columns(2)
    with col1:
        st.write("### Services")
        selected_services = st.multiselect(
            "Select Services",
            options=services_df['SERVICE_NAME'].tolist(),
            default=current_services,
            key="services_multiselect"
        )

    # Employee Selection
    employees_df = fetch_existing_employees()
    current_employees = [emp for emp in [
        transaction['EMPLOYEE1_NAME'],
        transaction['EMPLOYEE2_NAME'],
        transaction['EMPLOYEE3_NAME']
    ] if emp]

    with col2:
        st.write("### Employees")
        selected_employees = st.multiselect(
            "Assign Employees",
            options=employees_df['FULL_NAME'].tolist(),
            default=current_employees,
            key="employees_multiselect"
        )

    # Calculate total cost based on selected services
    total_cost = sum(
        float(services_df[services_df['SERVICE_NAME'] == service]['COST'].iloc[0])
        for service in selected_services
    )

    # Time tracking
    st.write("### Time Tracking")
    col1, col2 = st.columns(2)
    with col1:
        start_time = st.time_input(
            "Start Time",
            value=transaction['START_TIME'] if transaction['START_TIME'] else datetime.now().time(),
            key="start_time_input"
        )
    with col2:
        end_time = st.time_input(
            "End Time",
            value=transaction['END_TIME'] if transaction['END_TIME'] else datetime.now().time(),
            key="end_time_input"
        )

    # Payment Section
    st.write("---")
    st.write("### Payment Details")
    
    # Display existing deposit if any
    existing_deposit = float(transaction['DEPOSIT'] or 0)
    if existing_deposit > 0:
        st.info(f"""
            💰 Deposit Information:
            Amount: ${existing_deposit:.2f}
            Payment Method: {transaction['PYMT_MTHD_1']}
        """)

    col1, col2 = st.columns(2)
    with col1:
        discount = st.number_input(
            "Discount",
            min_value=0.0,
            max_value=total_cost,
            value=float(transaction['DISCOUNT'] or 0),
            step=0.01,
            key="discount_input"
        )
        
    with col2:
        st.metric("Total Amount Due", f"${(total_cost - discount):.2f}")

    # Payment Methods
    st.write("### Payment Collection")
    payment_options = ["Cash", "Credit Card", "Debit Card", "Check", "CashApp", "Venmo", "Zelle"]
    
    col1, col2 = st.columns(2)
    with col1:
        payment_method1 = st.selectbox(
            "Payment Method 1",
            options=[""] + payment_options,
            index=payment_options.index(transaction['PYMT_MTHD_1']) + 1 if transaction['PYMT_MTHD_1'] in payment_options else 0,
            key="payment_method1_select"
        )
        if payment_method1:
            payment_amount1 = st.number_input(
                "Amount",
                min_value=0.0,
                max_value=total_cost - discount - existing_deposit,
                value=float(transaction['PYMT_MTHD_1_AMT'] or 0),
                step=0.01,
                key="payment_amount1_input"
            )
        else:
            payment_amount1 = 0

    with col2:
        payment_method2 = st.selectbox(
            "Payment Method 2",
            options=[""] + payment_options,
            index=payment_options.index(transaction['PYMT_MTHD_2']) + 1 if transaction['PYMT_MTHD_2'] in payment_options else 0,
            key="payment_method2_select"
        )
        if payment_method2:
            payment_amount2 = st.number_input(
                "Amount 2",
                min_value=0.0,
                max_value=total_cost - discount - existing_deposit - payment_amount1,
                value=float(transaction['PYMT_MTHD_2_AMT'] or 0),
                step=0.01,
                key="payment_amount2_input"
            )
        else:
            payment_amount2 = 0

    # Calculate total received (deposit + additional payments)
    total_received = existing_deposit

    # Display payment summary
    st.write("### Payment Summary")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Cost", f"${total_cost:.2f}")
    with col2:
        st.metric("Total Received", f"${total_received:.2f}")
    with col3:
        balance = total_cost - discount - total_received
        st.metric("Balance", f"${balance:.2f}")

    # Display payment breakdown
    st.write("#### Payment Breakdown:")
    if existing_deposit > 0:
        st.write(f"Deposit: ${existing_deposit:.2f} ({transaction['PYMT_MTHD_1']})")
    if payment_amount1 > 0:
        st.write(f"Payment 1: ${payment_amount1:.2f} ({payment_method1})")
    if payment_amount2 > 0:
        st.write(f"Payment 2: ${payment_amount2:.2f} ({payment_method2})")
    st.write(f"**Total Payments: ${total_received:.2f}**")

    # Add comments
    comments = st.text_area(
        "Service Comments",
        value=transaction['COMMENTS'] if transaction['COMMENTS'] else "",
        key="service_comments"
    )

    # Determine status
    if balance <= 0:
        status = 'Completed'
    else:
        status = 'Partial Payment' if total_received > 0 else 'Pending Payment'

    # Save Changes Button
    if st.button("Update Transaction", type="primary", key="update_transaction_button"):
        try:
            # Get service IDs
            service_ids = [None, None, None]
            for i, service in enumerate(selected_services[:3]):
                service_row = services_df[services_df['SERVICE_NAME'] == service]
                if not service_row.empty:
                    service_ids[i] = int(service_row.iloc[0]['SERVICE_ID'])

            # Get employee IDs
            employee_ids = [None, None, None]
            for i, employee in enumerate(selected_employees[:3]):
                employee_row = employees_df[employees_df['FULL_NAME'] == employee]
                if not employee_row.empty:
                    employee_ids[i] = int(employee_row.iloc[0]['EMPLOYEE_ID'])

            # Prepare update data
            update_data = {
                'service2_id': service_ids[1],
                'service3_id': service_ids[2],
                'employee1_id': employee_ids[0],
                'employee2_id': employee_ids[1],
                'employee3_id': employee_ids[2],
                'total_amount': total_cost,
                'discount': discount,
                'amount_received': total_received - existing_deposit,  # Don't double-count deposit
                'payment_method1': payment_method1 if payment_method1 else None,
                'payment_amount1': payment_amount1,
                'payment_method2': payment_method2 if payment_method2 else None,
                'payment_amount2': payment_amount2,
                'start_time': start_time,
                'end_time': end_time,
                'comments': comments,
                'status': status
            }

            # Update transaction
            success = update_service_transaction(transaction_id, update_data)
            
            if success:
                st.success(f"Transaction updated successfully! Status: {status}")
                # Clear session state and return to service selection
                st.session_state['page'] = 'service_selection'
                st.rerun()
            else:
                st.error("Failed to update transaction.")
        except Exception as e:
            st.error(f"Error updating transaction: {e}")

def save_to_snowflake(data):
    try:
        # First, check if there's an existing transaction with deposit
        deposit_query = """
        SELECT 
            ID,
            COALESCE(DEPOSIT, 0) as TOTAL_DEPOSIT
        FROM SERVICE_TRANSACTION 
        WHERE CUSTOMER_ID = :1 
        AND SERVICE_ID = :2
        ORDER BY CREATED_DATE ASC
        LIMIT 1
        """
        
        deposit_params = [data['Customer ID'], data['Service ID']]
        deposit_result = execute_sql(deposit_query, deposit_params)
        
        total_deposit = float(deposit_result[0]['TOTAL_DEPOSIT']) if deposit_result else 0.0
        transaction_id = deposit_result[0]['ID'] if deposit_result else None

        # Calculate final amounts considering deposit
        final_amount = float(data['Amount']) if data['Amount'] is not None else 0.0
        total_received = float(data['Amount Received']) if data['Amount Received'] is not None else 0.0

        current_date = datetime.now().date()
        payment_date = current_date if (total_received + total_deposit) >= (final_amount - float(data['Discount'] or 0)) else None

        # Prepare parameters for either update or insert
        transaction_params = [
            data.get('Service2 ID'),
            data.get('Service3 ID'),
            data.get('Employee1 ID'),
            data.get('Employee2 ID'),
            data.get('Employee3 ID'),
            final_amount,
            float(data['Discount']) if data['Discount'] is not None else 0.0,
            total_received,
            data['PYMT_MTHD_1'],
            float(data['PYMT_MTHD_1_AMT']) if data['PYMT_MTHD_1_AMT'] is not None else 0.0,
            data.get('PYMT_MTHD_2'),
            float(data['PYMT_MTHD_2_AMT']) if data['PYMT_MTHD_2_AMT'] is not None else 0.0,
            data['Start Time'],
            data['End Time'],
            data['Transaction Date'],
            data['Transaction Time'],
            data['COMMENTS'],
            payment_date.strftime('%Y-%m-%d') if payment_date else None
        ]

        # If we found an existing transaction with deposit, update it
        if transaction_id:
            update_query = """
            UPDATE SERVICE_TRANSACTION
            SET 
                SERVICE2_ID = :1,
                SERVICE3_ID = :2,
                EMPLOYEE1_ID = :3,
                EMPLOYEE2_ID = :4,
                EMPLOYEE3_ID = :5,
                AMOUNT = :6,
                DISCOUNT = :7,
                AMOUNT_RECEIVED = :8,
                PYMT_MTHD_1 = :9,
                PYMT_MTHD_1_AMT = :10,
                PYMT_MTHD_2 = :11,
                PYMT_MTHD_2_AMT = :12,
                START_TIME = :13,
                END_TIME = :14,
                TRANSACTION_DATE = :15,
                TRANSACTION_TIME = :16,
                COMMENTS = :17,
                PYMT_DATE = :18,
                LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
            WHERE ID = :19
            """
            # Add transaction_id to the parameters
            update_params = transaction_params + [transaction_id]
            execute_sql(update_query, update_params)
        else:
            # If no existing transaction, create a new one
            insert_query = """
            INSERT INTO SERVICE_TRANSACTION (
                CUSTOMER_ID,
                SERVICE_ID,
                SERVICE2_ID,
                SERVICE3_ID,
                EMPLOYEE1_ID,
                EMPLOYEE2_ID,
                EMPLOYEE3_ID,
                AMOUNT,
                DISCOUNT,
                AMOUNT_RECEIVED,
                DEPOSIT,
                PYMT_MTHD_1,
                PYMT_MTHD_1_AMT,
                PYMT_MTHD_2,
                PYMT_MTHD_2_AMT,
                START_TIME,
                END_TIME,
                TRANSACTION_DATE,
                TRANSACTION_TIME,
                COMMENTS,
                PYMT_DATE,
                CREATED_DATE
            ) VALUES (
                :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, 
                :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, CURRENT_TIMESTAMP()
            )
            """
            # Prepare insert parameters
            insert_params = [
                data['Customer ID'],
                data['Service ID']
            ] + transaction_params + [total_deposit]  # Add deposit amount
            
            execute_sql(insert_query, insert_params)
        
        # Display success message with deposit info
        success_message = "Transaction saved successfully!"
        if total_deposit > 0:
            success_message += f"\nDeposit amount: ${total_deposit:.2f}"
            remaining_balance = final_amount - total_received - total_deposit - float(data['Discount'] or 0)
            if remaining_balance > 0:
                success_message += f"\nRemaining balance: ${remaining_balance:.2f}"
            else:
                success_message += "\nService fully paid"
        st.success(success_message)
        
        # Clear session state and return to service selection
        st.session_state.pop('service_start_time', None)
        st.session_state.pop('selected_service', None)
        st.session_state.pop('selected_services', None)
        st.session_state.pop('selected_employees', None)
        
        st.session_state['page'] = 'service_selection'
        st.rerun()
        
    except Exception as e:
        error_message = f"Error saving transaction: {e}"
        if 'update_params' in locals():
            error_message += f"\nUpdate parameters: {update_params}"
        elif 'insert_params' in locals():
            error_message += f"\nInsert parameters: {insert_params}"
        st.error(error_message)

def get_existing_deposit(customer_id, service_id):
    """Fetch any existing deposit for this service"""
    query = """
    SELECT 
        COALESCE(SUM(AMOUNT_RECEIVED), 0) as TOTAL_DEPOSIT,
        ARRAY_AGG(OBJECT_CONSTRUCT(
            'date', TRANSACTION_DATE,
            'amount', AMOUNT_RECEIVED,
            'method', PYMT_MTHD_1
        )) as DEPOSIT_HISTORY
    FROM SERVICE_TRANSACTION 
    WHERE CUSTOMER_ID = :1 
    AND SERVICE_ID = :2
    GROUP BY CUSTOMER_ID, SERVICE_ID
    """
    result = execute_sql(query, [customer_id, service_id])
    if result:
        return {
            'amount': float(result[0]['TOTAL_DEPOSIT']),
            'history': result[0]['DEPOSIT_HISTORY']
        }
    return {'amount': 0.0, 'history': []}

def display_deposit_info(deposit_info):
    """Display deposit information"""
    if deposit_info['amount'] > 0:
        st.info(f"Previous deposit: ${deposit_info['amount']:.2f}")
        with st.expander("View Deposit History"):
            for deposit in deposit_info['history']:
                st.write(f"Date: {deposit['date']}, Amount: ${deposit['amount']:.2f}, Method: {deposit['method']}")

def dynamic_service_selector(service_df, selected_services):
    # Changed to enumerate to avoid unused variable
    for i in range(len(selected_services)):
        service_name = st.selectbox(f"Select Service {i + 1}", service_df['SERVICE_NAME'])
        selected_service_id = service_df[service_df['SERVICE_NAME'] == service_name]['SERVICE_ID'].values[0]
        st.session_state['selected_services'][i] = {'SERVICE_ID': selected_service_id}

    if st.button("+ Add Another Service"):
        st.session_state['selected_services'].append({'SERVICE_ID': None, 'index': len(selected_services)})

def dynamic_employee_selector(employee_df, selected_employees):
    # Changed to enumerate to avoid unused variable
    for i in range(len(selected_employees)):
        employee_name = st.selectbox(f"Select Employee {i + 1}", employee_df['FULL_NAME'])
        selected_employee_id = employee_df[employee_df['FULL_NAME'] == employee_name]['EMPLOYEE_ID'].values[0]
        st.session_state['selected_employees'][i] = {'EMPLOYEE_ID': selected_employee_id}

    if st.button("+ Add Another Employee"):
        st.session_state['selected_employees'].append({'EMPLOYEE_ID': None, 'index': len(selected_employees)})

def completed_services_page():
    st.title('Completed Services')

    # Date range selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", 
                                 value=datetime.now().date() - timedelta(days=30))
    with col2:
        end_date = st.date_input("End Date", 
                               value=datetime.now().date())

    # Filter options
    payment_status = st.selectbox("Filter by Payment Status", 
                                ["All", "Paid", "Unpaid"])

    # Query to fetch completed services
    query = """
    SELECT 
        ST.ID AS TRANSACTION_ID,
        COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
        S.SERVICE_NAME,
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
        CASE 
            WHEN (ST.AMOUNT_RECEIVED + COALESCE(ST.DEPOSIT, 0)) >= (ST.AMOUNT - COALESCE(ST.DISCOUNT, 0)) THEN 'Paid'
            ELSE 'Unpaid'
        END AS PAYMENT_STATUS
    FROM SERVICE_TRANSACTION ST
    LEFT JOIN CUSTOMER C ON ST.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN ACCOUNTS A ON ST.ACCOUNT_ID = A.ACCOUNT_ID
    LEFT JOIN SERVICES S ON ST.SERVICE_ID = S.SERVICE_ID
    LEFT JOIN EMPLOYEE E1 ON ST.EMPLOYEE1_ID = E1.EMPLOYEE_ID
    LEFT JOIN EMPLOYEE E2 ON ST.EMPLOYEE2_ID = E2.EMPLOYEE_ID
    LEFT JOIN EMPLOYEE E3 ON ST.EMPLOYEE3_ID = E3.EMPLOYEE_ID
    WHERE ST.TRANSACTION_DATE BETWEEN :1 AND :2
    ORDER BY ST.TRANSACTION_DATE DESC, ST.TRANSACTION_TIME DESC
    """
    
    completed_df = pd.DataFrame(execute_sql(query, [start_date.strftime('%Y-%m-%d'), 
                                                  end_date.strftime('%Y-%m-%d')]))

    if not completed_df.empty:
        # Convert decimal columns to float
        decimal_columns = ['AMOUNT', 'DISCOUNT', 'AMOUNT_RECEIVED', 'DEPOSIT']
        for col in decimal_columns:
            completed_df[col] = completed_df[col].astype(float)

        # Apply payment status filter
        if payment_status != "All":
            completed_df = completed_df[completed_df['PAYMENT_STATUS'] == payment_status]

        # Display services
        st.dataframe(
            completed_df,
            column_config={
                "AMOUNT": st.column_config.NumberColumn(
                    "Amount",
                    format="$%.2f"
                ),
                "DISCOUNT": st.column_config.NumberColumn(
                    "Discount",
                    format="$%.2f"
                ),
                "AMOUNT_RECEIVED": st.column_config.NumberColumn(
                    "Received",
                    format="$%.2f"
                ),
                "DEPOSIT": st.column_config.NumberColumn(
                    "Deposit",
                    format="$%.2f"
                ),
                "TRANSACTION_DATE": st.column_config.DateColumn(
                    "Date",
                    format="MM/DD/YYYY"
                ),
                "START_TIME": st.column_config.TimeColumn(
                    "Start Time",
                    format="hh:mm A"
                ),
                "END_TIME": st.column_config.TimeColumn(
                    "End Time",
                    format="hh:mm A"
                ),
                "CUSTOMER_NAME": "Customer",
                "EMPLOYEE1_NAME": "Employee 1",
                "EMPLOYEE2_NAME": "Employee 2",
                "EMPLOYEE3_NAME": "Employee 3",
                "PAYMENT_STATUS": "Status"
            },
            hide_index=True
        )

        # Summary statistics
        st.markdown("### Summary")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Services", len(completed_df))
        with col2:
            total_amount = completed_df['AMOUNT'].sum()
            st.metric("Total Amount", f"${total_amount:,.2f}")
        with col3:
            # Calculate total received using float values
            total_received = (completed_df['AMOUNT_RECEIVED'].fillna(0) + 
                            completed_df['DEPOSIT'].fillna(0)).sum()
            st.metric("Total Received", f"${total_received:,.2f}")
        with col4:
            total_outstanding = total_amount - total_received
            st.metric("Outstanding Balance", f"${total_outstanding:,.2f}")
    else:
        st.info("No completed services found for the selected date range.")

if __name__ == "__main__":
    main()


