# # new_service
import streamlit as st
from datetime import datetime, date, time, timedelta
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
import pandas as pd
import json

from models.transaction import save_transaction
from models.customer import CustomerModel, fetch_all_customers, save_customer, search_customers
from models.service import (
    ServiceModel,
    schedule_recurring_services,
    fetch_services,
    check_service_availability,
    save_service_schedule,
    get_available_time_slots
)
from database.connection import snowflake_conn
from utils.formatting import format_currency
# from utils.email import send_service_scheduled_email, send_service_completed_email
from utils.validation import validate_phone, validate_email, validate_zip_code, sanitize_zip_code
from utils.email import generate_service_scheduled_email
from utils.email import generate_service_completed_email
from utils.sms import send_service_notification_sms 
from pages.settings.business import fetch_business_info  # Add this import

# In new_service.py
from typing import Optional, Dict, Any
from dataclasses import dataclass
import streamlit as st
from datetime import datetime, date, time
import pandas as pd

from models.account import (
    save_account, fetch_all_accounts, search_accounts, 
    fetch_account, validate_account_data
)

from models.service import (
    save_service_schedule, get_available_time_slots,
    check_service_availability, fetch_services
)
from utils.formatting import format_currency
from utils.email import generate_service_scheduled_email
from database.connection import SnowflakeConnection


def debug_print(msg: str) -> None:
    """Helper function for debug logging with defensive access to debug_mode."""
    if st.session_state.get('debug_mode', False):  # Default to False if not set
        print(f"DEBUG: {msg}")
        st.write(f"DEBUG: {msg}")


def initialize_session_state() -> None:
    """Initialize required session state variables"""
    if 'debug_mode' not in st.session_state:
        st.session_state['debug_mode'] = False
    if 'selected_services' not in st.session_state:
        st.session_state.selected_services = []
    if 'service_costs' not in st.session_state:
        st.session_state.service_costs = {}
    if 'is_recurring' not in st.session_state:
        st.session_state.is_recurring = False
    if 'recurrence_pattern' not in st.session_state:
        st.session_state.recurrence_pattern = None
    if 'deposit_amount' not in st.session_state:
        st.session_state.deposit_amount = 0.0
    if 'service_notes' not in st.session_state:
        st.session_state.service_notes = ''
    if 'service_date' not in st.session_state:
        st.session_state.service_date = datetime.now().date()
    if 'service_time' not in st.session_state:
        st.session_state.service_time = None
    if 'selected_customer_id' not in st.session_state:
        st.session_state.selected_customer_id = None

    debug_print("Session state initialized")


def reset_session_state() -> None:
    """Reset all session state variables related to service scheduling"""
    keys_to_clear = [
        'selected_services', 
        'is_recurring', 
        'recurrence_pattern',
        'deposit_amount', 
        'service_notes', 
        'service_date',
        'service_time', 
        'scheduler',
        'service_costs',
        'customer_type',
        'form_data',
        'selected_customer_id',  # Clear customer selection
        'new_customer_name',
        'new_customer_phone',
        'new_customer_email',
        'new_customer_street',
        'new_customer_city',
        'new_customer_state',
        'new_customer_zip',
        'customer_select',  # Clear customer selection dropdown
        'search_customer',  # Clear search field
        'old_customer_type'
    ]
    for key in keys_to_clear:
        if key in st.session_state:
            del st.session_state[key]
    debug_print("Session state reset")
    
    # Re-initialize session state but preserve debug_mode
    initialize_session_state()


@dataclass
class ServiceFormData:
    customer_data: Dict[str, Any]
    service_selection: Dict[str, Any]
    service_schedule: Dict[str, Any]
    account_data: Optional[Dict[str, Any]] = None

    @classmethod
    def initialize(cls) -> 'ServiceFormData':
        """Initialize form data with default values"""
        return cls(
            customer_data={
                'customer_id': None,
                'account_id': None,
                'business_name': '',
                'contact_person': '',
                'first_name': '',
                'last_name': '',
                'phone_number': '',
                'email_address': '',
                'billing_address': '',     # For commercial "BILLING_ADDRESS"
                'billing_city': '',
                'billing_state': '',
                'billing_zip': '',
                'primary_contact_method': 'SMS',
                'text_flag': False,
                'comments': '',
                'member_flag': False,
                'is_commercial': False,
                'different_billing': False  # Keep track if user selected a different billing address
            },
            service_selection={
                'selected_services': st.session_state.get('selected_services', []),
                'is_recurring': st.session_state.get('is_recurring', False),
                'recurrence_pattern': st.session_state.get('recurrence_pattern'),
                'deposit_amount': st.session_state.get('deposit_amount', 0.0),
                'notes': st.session_state.get('service_notes', ''),
            },
            service_schedule={
                'date': datetime.now().date(),
                'time': None
            },
            account_data=None
        )


class ServiceScheduler:
    def __init__(self):
        if not hasattr(st.session_state, 'form_data'):
            st.session_state.form_data = ServiceFormData.initialize()
        self.form_data = st.session_state.form_data

    def display_customer_details(self, customer: Dict[str, Any]) -> None:
        """Minimal method to display/edit the selected customer's basic details."""
        st.markdown("### Customer Details")
        col1, col2 = st.columns(2)
        with col1:
            self.form_data.customer_data['first_name'] = st.text_input(
                "First Name",
                value=customer.get('FIRST_NAME', ''),
                key="edit_first_name"
            )
            self.form_data.customer_data['last_name'] = st.text_input(
                "Last Name",
                value=customer.get('LAST_NAME', ''),
                key="edit_last_name"
            )
        with col2:
            self.form_data.customer_data['phone_number'] = st.text_input(
                "Phone",
                value=customer.get('PHONE_NUMBER', ''),
                key="edit_phone"
            )
            self.form_data.customer_data['email_address'] = st.text_input(
                "Email",
                value=customer.get('EMAIL_ADDRESS', ''),
                key="edit_email"
            )

    def save_service_address(self, snowflake_conn: Any, customer_id: int, data: Dict[str, Any], is_primary: bool = False) -> Optional[int]:
        """Save service address (STREET_ADDRESS) to SERVICE_ADDRESSES for the 'service location'."""
        try:
            service_zip = sanitize_zip_code(data.get('service_zip'))
            if not service_zip:
                st.error("Invalid service address ZIP code format. Please enter a 5-digit number.")
                return None

            try:
                customer_id_int = int(customer_id)
                zip_code_int = int(service_zip)
                square_footage = data.get('service_addr_sq_ft', 0)
                square_footage_int = int(square_footage if square_footage is not None else 0)
            except (ValueError, TypeError) as e:
                st.error(f"Error converting numeric values: {str(e)}")
                return None

            query = """
            INSERT INTO OPERATIONAL.CARPET.SERVICE_ADDRESSES (
                CUSTOMER_ID,
                STREET_ADDRESS,
                CITY,
                STATE,
                ZIP_CODE,
                SQUARE_FOOTAGE,
                IS_PRIMARY_SERVICE
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """
            params = [
                customer_id_int,
                str(data.get('service_address', '')).strip(),
                str(data.get('service_city', '')).strip(),
                str(data.get('service_state', '')).strip(),
                zip_code_int,
                square_footage_int,
                bool(is_primary)
            ]

            if st.session_state.get('debug_mode'):
                debug_print(f"Service Address Query: {query}")
                debug_print(f"Service Address Params: {params}")
                debug_print(f"Parameter types: {[type(p) for p in params]}")

            snowflake_conn.execute_query(query, params)
            result = snowflake_conn.execute_query(
                """
                SELECT ADDRESS_ID 
                FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES 
                WHERE CUSTOMER_ID = ? 
                ORDER BY CREATED_AT DESC 
                LIMIT 1
                """,
                [customer_id_int]
            )
            return result[0]['ADDRESS_ID'] if result else None

        except Exception as e:
            st.error(f"Error saving service address: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)
            return None

    def handle_account_search(self) -> None:
        """Process business account search and selection."""
        try:
            search_term = st.text_input(
                "Search Business Account",
                value=st.session_state.get('account_search', ''),
                help="Enter business name, email, or phone to search"
            )
            if search_term:
                st.session_state['account_search'] = search_term.strip()
                accounts_df = search_accounts(search_term)
                if not accounts_df.empty:
                    selected_account = st.selectbox(
                        "Select Business",
                        options=["Select..."] + accounts_df['ACCOUNT_DETAILS'].tolist(),
                        key="business_select"
                    )
                    if selected_account != "Select...":
                        account_details = accounts_df[
                            accounts_df['ACCOUNT_DETAILS'] == selected_account
                        ].iloc[0]

                        # Update form data with the DB columns for ACCOUNTS
                        self.form_data.customer_data.update({
                            'account_id': int(account_details['ACCOUNT_ID']),
                            'business_name': account_details.get('ACCOUNT_NAME', ''),
                            'contact_person': account_details.get('CONTACT_PERSON', ''),
                            'phone_number': account_details.get('CONTACT_PHONE', ''),
                            'email_address': account_details.get('CONTACT_EMAIL', ''),
                            # For the commercial 'billing' fields
                            'billing_address': account_details.get('BILLING_ADDRESS', ''),
                            'billing_city': account_details.get('CITY', ''),
                            'billing_state': account_details.get('STATE', ''),
                            'billing_zip': account_details.get('ZIP_CODE', ''),
                            'is_commercial': True,
                            # Optionally treat the "service address" the same as billing
                            'service_address': account_details.get('BILLING_ADDRESS', ''),
                            'service_city': account_details.get('CITY', ''),
                            'service_state': account_details.get('STATE', ''),
                            'service_zip': account_details.get('ZIP_CODE', '')
                        })
                        self.display_account_details(account_details)
                else:
                    st.info("No matching accounts found.")
                    if st.button("Create New Account"):
                        self.display_account_form()
        except Exception as e:
            st.error(f"Error during account search: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)

    def display_account_details(self, account: Dict[str, Any]) -> None:
        """Display editable account details for commercial usage.
        Uses consistent billing address keys and includes debug logging.
        """
        account_id = account.get('ACCOUNT_ID')
        
        # Fetch service address if available
        service_address = None
        if account_id:
            query = """
            SELECT 
                ADDRESS_ID, STREET_ADDRESS, CITY, STATE, 
                ZIP_CODE, SQUARE_FOOTAGE, IS_PRIMARY_SERVICE
            FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES
            WHERE ACCOUNT_ID = ?
            AND IS_PRIMARY_SERVICE = TRUE
            """
            from database.connection import snowflake_conn
            try:
                result = snowflake_conn.execute_query(query, [account_id])
                if result:
                    service_address = result[0]
            except Exception as e:
                print(f"DEBUG: Error fetching service address: {str(e)}")
        
        st.markdown("### Account Details")
        col1, col2 = st.columns(2)
        with col1:
            self.form_data.customer_data['business_name'] = st.text_input(
                "Business Name",
                value=account.get('ACCOUNT_NAME', ''),
                key="edit_business_name"
            )
            self.form_data.customer_data['contact_person'] = st.text_input(
                "Contact Person",
                value=account.get('CONTACT_PERSON', ''),
                key="edit_contact_person"
            )
        with col2:
            self.form_data.customer_data['phone_number'] = st.text_input(
                "Phone",
                value=account.get('CONTACT_PHONE', ''),
                key="edit_business_phone"
            )
            self.form_data.customer_data['email_address'] = st.text_input(
                "Email",
                value=account.get('CONTACT_EMAIL', ''),
                key="edit_business_email"
            )
        
        st.markdown("### Billing Address")
        self.form_data.customer_data['billing_address'] = st.text_input(
            "Street Address",
            value=account.get('BILLING_ADDRESS', ''),
            key="edit_billing_address"
        )
        col1, col2 = st.columns(2)
        with col1:
            self.form_data.customer_data['billing_city'] = st.text_input(
                "City",
                value=account.get('CITY', ''),
                key="edit_billing_city"
            )
        with col2:
            self.form_data.customer_data['billing_state'] = st.text_input(
                "State",
                value=account.get('STATE', ''),
                key="edit_billing_state"
            )
        self.form_data.customer_data['billing_zip'] = st.text_input(
            "ZIP Code",
            value=account.get('ZIP_CODE', ''),
            key="edit_billing_zip"
        )
        
        # Service Address Section
        st.markdown("### Service Address")
        
        # Default to billing address if no service address found
        if not service_address:
            use_billing_as_service = True
            service_address = {
                'STREET_ADDRESS': account.get('BILLING_ADDRESS', ''),
                'CITY': account.get('CITY', ''),
                'STATE': account.get('STATE', ''),
                'ZIP_CODE': account.get('ZIP_CODE', ''),
                'SQUARE_FOOTAGE': 0,
                'ADDRESS_ID': None
            }
        else:
            # Check if service address matches billing address
            use_billing_as_service = (
                service_address.get('STREET_ADDRESS') == account.get('BILLING_ADDRESS') and
                service_address.get('CITY') == account.get('CITY') and
                service_address.get('STATE') == account.get('STATE') and
                service_address.get('ZIP_CODE') == account.get('ZIP_CODE')
            )
            
        self.form_data.customer_data['use_billing_as_service'] = use_billing_as_service
        use_billing_as_service = st.checkbox(
            "Same as Billing Address", 
            value=use_billing_as_service,
            key="edit_use_billing_as_service"
        )
        
        if not use_billing_as_service:
            self.form_data.customer_data['service_address'] = st.text_input(
                "Street Address",
                value=service_address.get('STREET_ADDRESS', ''),
                key="edit_service_address"
            )
            col1, col2 = st.columns(2)
            with col1:
                self.form_data.customer_data['service_city'] = st.text_input(
                    "City",
                    value=service_address.get('CITY', ''),
                    key="edit_service_city"
                )
            with col2:
                self.form_data.customer_data['service_state'] = st.text_input(
                    "State",
                    value=service_address.get('STATE', ''),
                    key="edit_service_state"
                )
            self.form_data.customer_data['service_zip'] = st.text_input(
                "ZIP Code",
                value=service_address.get('ZIP_CODE', ''),
                key="edit_service_zip"
            )
            self.form_data.customer_data['service_addr_sq_ft'] = st.number_input(
                "Square Footage",
                min_value=0,
                step=100,
                value=int(service_address.get('SQUARE_FOOTAGE', 0) or 0),
                key="edit_service_sq_ft"
            )
        else:
            # If using billing address as service address, copy values
            self.form_data.customer_data['service_address'] = self.form_data.customer_data['billing_address']
            self.form_data.customer_data['service_city'] = self.form_data.customer_data['billing_city']
            self.form_data.customer_data['service_state'] = self.form_data.customer_data['billing_state']
            self.form_data.customer_data['service_zip'] = self.form_data.customer_data['billing_zip']
            self.form_data.customer_data['service_addr_sq_ft'] = 0
        
        # Save service address ID if available
        self.form_data.customer_data['service_address_id'] = service_address.get('ADDRESS_ID')
        
        # Save account ID to form data
        self.form_data.customer_data['account_id'] = account_id
        self.form_data.customer_data['is_commercial'] = True
        
        # Add save button for current account
        if st.button("Update Account", type="primary"):
            self.save_account_and_get_id()
        
        debug_print("Displayed account details in display_account_details")


    def display_account_form(self) -> None:
        st.markdown("### New Business Account")
        
        with st.form("account_form"):
            # Business Information Inputs
            business_name = st.text_input(
                "Business Name",
                value=self.form_data.customer_data.get('business_name', ''),
                key="new_business_name"
            )
            contact_person = st.text_input(
                "Contact Person",
                value=self.form_data.customer_data.get('contact_person', ''),
                key="new_contact_person"
            )
            phone_number = st.text_input(
                "Phone Number",
                value=self.form_data.customer_data.get('phone_number', ''),
                key="new_business_phone"
            )
            email_address = st.text_input(
                "Email",
                value=self.form_data.customer_data.get('email_address', ''),
                key="new_business_email"
            )
            
            # Billing Address Inputs
            st.markdown("### Billing Address")
            billing_address = st.text_input(
                "Street Address",
                value=self.form_data.customer_data.get('billing_address', ''),
                key="new_billing_street"
            )
            col1, col2 = st.columns(2)
            with col1:
                billing_city = st.text_input(
                    "City",
                    value=self.form_data.customer_data.get('billing_city', ''),
                    key="new_city"
                )
            with col2:
                billing_state = st.text_input(
                    "State",
                    value=self.form_data.customer_data.get('billing_state', ''),
                    key="new_state"
                )
            billing_zip = st.text_input(
                "ZIP Code",
                value=self.form_data.customer_data.get('billing_zip', ''),
                key="new_zip_code"
            )
            
            # Service Address Inputs
            st.markdown("### Service Address")
            
            # Use same as billing address option
            use_billing_as_service = st.checkbox(
                "Same as Billing Address",
                value=self.form_data.customer_data.get('use_billing_as_service', True),
                key="use_billing_as_service"
            )
            
            if not use_billing_as_service:
                service_address = st.text_input(
                    "Street Address",
                    value=self.form_data.customer_data.get('service_address', ''),
                    key="new_service_street"
                )
                col1, col2 = st.columns(2)
                with col1:
                    service_city = st.text_input(
                        "City",
                        value=self.form_data.customer_data.get('service_city', ''),
                        key="new_service_city"
                    )
                with col2:
                    service_state = st.text_input(
                        "State",
                        value=self.form_data.customer_data.get('service_state', ''),
                        key="new_service_state"
                    )
                service_zip = st.text_input(
                    "ZIP Code",
                    value=self.form_data.customer_data.get('service_zip', ''),
                    key="new_service_zip"
                )
                service_addr_sq_ft = st.number_input(
                    "Square Footage",
                    min_value=0,
                    step=100,
                    value=self.form_data.customer_data.get('service_addr_sq_ft', 0),
                    key="new_service_sq_ft"
                )
            
            submitted = st.form_submit_button("Save Account")
        
        if submitted:
            # Immediately update session state with billing-specific field names
            self.form_data.customer_data['business_name'] = business_name
            self.form_data.customer_data['contact_person'] = contact_person
            self.form_data.customer_data['phone_number'] = phone_number
            self.form_data.customer_data['email_address'] = email_address
            self.form_data.customer_data['billing_address'] = billing_address
            self.form_data.customer_data['billing_city'] = billing_city
            self.form_data.customer_data['billing_state'] = billing_state
            self.form_data.customer_data['billing_zip'] = billing_zip
            
            # Update use_billing_as_service flag in form data
            self.form_data.customer_data['use_billing_as_service'] = use_billing_as_service
            
            # Set service address from billing if needed
            if use_billing_as_service:
                self.form_data.customer_data['service_address'] = billing_address
                self.form_data.customer_data['service_city'] = billing_city
                self.form_data.customer_data['service_state'] = billing_state
                self.form_data.customer_data['service_zip'] = billing_zip
                self.form_data.customer_data['service_addr_sq_ft'] = 0
            else:
                self.form_data.customer_data['service_address'] = service_address
                self.form_data.customer_data['service_city'] = service_city
                self.form_data.customer_data['service_state'] = service_state
                self.form_data.customer_data['service_zip'] = service_zip
                self.form_data.customer_data['service_addr_sq_ft'] = service_addr_sq_ft

            # Create account data dictionary with correct field names for account.py
            account_data = {
                'account_name': business_name,
                'account_type': 'Commercial',
                'contact_person': contact_person,
                'contact_email': email_address,
                'contact_phone': phone_number,
                'billing_address': billing_address,
                'city': billing_city,     # Using billing_city field
                'state': billing_state,   # Using billing_state field
                'zip_code': billing_zip,  # Using billing_zip field
                'active_flag': True
            }
            debug_print(f"Account data to be saved: {account_data}")
            
            validation_errors = validate_account_data(account_data)
            if validation_errors:
                for error in validation_errors:
                    st.error(error)
                debug_print(f"Validation errors: {validation_errors}")
                return
            
            # Direct call to account.py save_account function
            from models.account import save_account, save_account_service_address
            account_id = save_account(account_data)
            
            if account_id:
                self.form_data.customer_data['account_id'] = account_id
                self.form_data.customer_data['is_commercial'] = True
                
                # Save service address for the account
                service_address_id = save_account_service_address(
                    account_id=account_id,
                    data=self.form_data.customer_data,
                    is_primary=True
                )
                
                if service_address_id:
                    self.form_data.customer_data['service_address_id'] = service_address_id
                    st.success(f"Account created successfully with ID: {account_id}")
                else:
                    st.warning("Account created but failed to save service address.")
                    
                st.rerun()  # Refresh the page to show updated state
            else:
                st.error("Failed to create account. Please check the logs for errors.")



    @staticmethod
    def format_zip_code(zip_code: Any) -> str:
        """Utility to format a ZIP code to 5 digits."""
        if not zip_code:
            return ""
        zip_str = ''.join(filter(str.isdigit, str(zip_code)))
        return zip_str[:5]

    def display_customer_form(self) -> None:
        """Display form for entering/editing a residential customer's details."""
        st.markdown("### Customer Information")
        col1, col2 = st.columns(2)
        with col1:
            first_name = st.text_input(
                "First Name",
                value=self.form_data.customer_data.get('first_name', ''),
                key="new_first_name"
            )
            self.form_data.customer_data['first_name'] = first_name
            last_name = st.text_input(
                "Last Name",
                value=self.form_data.customer_data.get('last_name', ''),
                key="new_last_name"
            )
            self.form_data.customer_data['last_name'] = last_name
        with col2:
            phone_number = st.text_input(
                "Phone Number",
                value=self.form_data.customer_data.get('phone_number', ''),
                key="new_phone"
            )
            self.form_data.customer_data['phone_number'] = phone_number
            email_address = st.text_input(
                "Email",
                value=self.form_data.customer_data.get('email_address', ''),
                key="new_email"
            )
            self.form_data.customer_data['email_address'] = email_address

        col1, col2 = st.columns(2)
        with col1:
            primary_contact_method = st.selectbox(
                "Preferred Contact Method",
                ["SMS", "Phone", "Email"],
                key="new_contact_method"
            )
            self.form_data.customer_data['primary_contact_method'] = primary_contact_method
        with col2:
            text_flag = st.checkbox(
                "Opt-in to Text Messages",
                value=self.form_data.customer_data.get('text_flag', False),
                key="new_text_flag"
            )
            self.form_data.customer_data['text_flag'] = text_flag

        st.markdown("### Service Address")
        service_address = st.text_input(
            "Street Address",
            value=self.form_data.customer_data.get('service_address', ''),
            key="service_street"
        )
        self.form_data.customer_data['service_address'] = service_address
        col1, col2 = st.columns(2)
        with col1:
            service_city = st.text_input(
                "City",
                value=self.form_data.customer_data.get('service_city', ''),
                key="service_city"
            )
            self.form_data.customer_data['service_city'] = service_city
            service_state = st.text_input(
                "State",
                value=self.form_data.customer_data.get('service_state', ''),
                key="service_state"
            )
            self.form_data.customer_data['service_state'] = service_state
        with col2:
            service_zip = st.text_input(
                "ZIP Code",
                value=self.form_data.customer_data.get('service_zip', ''),
                key="service_zip"
            )
            self.form_data.customer_data['service_zip'] = service_zip
            service_addr_sq_ft = st.number_input(
                "Square Footage",
                min_value=0,
                step=100,
                value=self.form_data.customer_data.get('service_addr_sq_ft', 0),
                key="service_sq_ft"
            )
            self.form_data.customer_data['service_addr_sq_ft'] = service_addr_sq_ft

        different_billing = st.checkbox(
            "Different Billing Address",
            value=False,
            key="different_billing_checkbox"
        )
        self.form_data.customer_data['different_billing'] = different_billing
        if different_billing:
            st.markdown("### Billing Address")
            billing_address = st.text_input(
                "Street Address",
                value=self.form_data.customer_data.get('billing_address', ''),
                key="billing_street"
            )
            self.form_data.customer_data['billing_address'] = billing_address
            col1, col2 = st.columns(2)
            with col1:
                billing_city = st.text_input(
                    "City",
                    value=self.form_data.customer_data.get('billing_city', ''),
                    key="billing_city"
                )
                self.form_data.customer_data['billing_city'] = billing_city
                billing_state = st.text_input(
                    "State",
                    value=self.form_data.customer_data.get('billing_state', ''),
                    key="billing_state"
                )
                self.form_data.customer_data['billing_state'] = billing_state
            with col2:
                billing_zip = st.text_input(
                    "ZIP Code",
                    value=self.form_data.customer_data.get('billing_zip', ''),
                    key="billing_zip"
                )
                self.form_data.customer_data['billing_zip'] = billing_zip
        else:
            self.form_data.customer_data.update({
                'billing_address': self.form_data.customer_data.get('service_address', ''),
                'billing_city': self.form_data.customer_data.get('service_city', ''),
                'billing_state': self.form_data.customer_data.get('service_state', ''),
                'billing_zip': self.form_data.customer_data.get('service_zip', '')
            })

    def save_account_and_get_id(self) -> Optional[int]:
        """Save or update account details and return the account ID.
        Captures the latest form values using consistent keys and logs debug information.
        """
        try:
            debug_print("Starting save_account_and_get_id")
            debug_print(f"Form data customer data: {self.form_data.customer_data}")

            # Log all form data for debugging
            print("DEBUG: All customer data keys:", self.form_data.customer_data.keys())
            
            # Directly import the save_account function to avoid any potential import issues
            from models.account import save_account
            
            account_data = {
                'account_name': self.form_data.customer_data.get('business_name', ''),
                'account_type': 'Commercial',
                'contact_person': self.form_data.customer_data.get('contact_person', ''),
                'contact_email': self.form_data.customer_data.get('email_address', ''),
                'contact_phone': self.form_data.customer_data.get('phone_number', ''),
                'billing_address': self.form_data.customer_data.get('billing_address', ''),
                'city': self.form_data.customer_data.get('billing_city', ''),  # Using billing_city
                'state': self.form_data.customer_data.get('billing_state', ''), # Using billing_state
                'zip_code': self.form_data.customer_data.get('billing_zip', ''), # Using billing_zip
                'active_flag': True
            }
            
            # Log the data that will be saved
            print("DEBUG: Final account data to be saved:")
            for key, value in account_data.items():
                print(f"  {key}: {value}")
            
            debug_print(f"Prepared account data in save_account_and_get_id: {account_data}")

            # Validate the data
            from models.account import validate_account_data
            validation_errors = validate_account_data(account_data)
            if validation_errors:
                for error in validation_errors:
                    st.error(error)
                debug_print(f"Validation errors: {validation_errors}")
                return None

            # Get the account_id if we're updating an existing account
            account_id = self.form_data.customer_data.get('account_id')
            debug_print(f"Calling save_account with account_id: {account_id}")
            
            # Call save_account with the data
            saved_account_id = save_account(account_data, account_id)
            
            if not saved_account_id:
                st.error("Failed to save account information")
                debug_print("save_account returned None")
                return None
                    
            debug_print(f"Account saved with ID: {saved_account_id}")
            
            # Display a success message
            st.success(f"Account {'updated' if account_id else 'created'} successfully!")
            
            # Update the form data with the new account ID
            self.form_data.customer_data['account_id'] = saved_account_id
            self.form_data.customer_data['is_commercial'] = True
            
            return saved_account_id

        except KeyError as e:
            st.error(f"Missing required field: {str(e)}")
            debug_print(f"KeyError: {str(e)}")
            st.exception(e)
            return None
        except ValueError as e:
            st.error(f"Invalid data format: {str(e)}")
            debug_print(f"ValueError: {str(e)}")
            st.exception(e)
            return None
        except Exception as e:
            st.error(f"Error saving account: {str(e)}")
            debug_print(f"Exception: {str(e)}")
            import traceback
            debug_print(f"Traceback: {traceback.format_exc()}")
            st.exception(e)
            return None

    def process_service_scheduling(self) -> bool:
        """Handle service scheduling workflow."""
        if not self.form_data.service_selection['selected_services']:
            st.error("Please select at least one service")
            return False
        try:
            col1, col2 = st.columns(2)
            with col1:
                service_date = st.date_input(
                    "Service Date",
                    min_value=datetime.now().date(),
                    value=self.form_data.service_schedule['date'],
                    key="service_date_input"
                )
                self.form_data.service_schedule['date'] = service_date
            with col2:
                available_slots = get_available_time_slots(
                    service_date,
                    self.form_data.service_selection['selected_services']
                )
                if not available_slots:
                    st.warning(f"No available time slots for {service_date.strftime('%Y-%m-%d')}.")
                    return False
                formatted_slots = [slot.strftime("%I:%M %p") for slot in available_slots]
                selected_time_str = st.selectbox(
                    "Select Time",
                    options=formatted_slots,
                    key="time_select"
                )
                if selected_time_str:
                    service_time = datetime.strptime(selected_time_str, "%I:%M %p").time()
                    available, message = check_service_availability(
                        service_date,
                        service_time,
                        self.form_data.service_selection['selected_services']
                    )
                    if available:
                        self.form_data.service_schedule['time'] = service_time
                        return True
                    else:
                        st.error(message)
                        return False
            return False
        except Exception as e:
            st.error(f"Error in service scheduling: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)
            return False

    def save_customer_and_get_id(self, customer_data: Dict[str, Any]) -> Optional[int]:
        """Save or update a residential customer's info + service address in SERVICE_ADDRESSES."""
        try:
            snowflake_conn = SnowflakeConnection.get_instance()
            billing_zip = sanitize_zip_code(customer_data.get('billing_zip'))
            if not billing_zip:
                st.error("Invalid billing ZIP code format. Please enter a 5-digit number.")
                return None
            billing_zip_int = int(billing_zip)

            clean_data = {
                'first_name': str(customer_data.get('first_name', '')).strip(),
                'last_name': str(customer_data.get('last_name', '')).strip(),
                'phone_number': str(customer_data.get('phone_number', '')).strip(),
                'email_address': str(customer_data.get('email_address', '')),
                'billing_address': str(customer_data.get('billing_address', '')),
                'billing_city': str(customer_data.get('billing_city', '')),
                'billing_state': str(customer_data.get('billing_state', '')),
                'billing_zip': billing_zip_int,
                'text_flag': bool(customer_data.get('text_flag', False)),
                'primary_contact_method': str(customer_data.get('primary_contact_method', 'Phone'))[:50],
                'comments': str(customer_data.get('comments', '')),
                'member_flag': bool(customer_data.get('member_flag', False))
            }

            if customer_data.get('customer_id'):
                query = """
                UPDATE OPERATIONAL.CARPET.CUSTOMER
                SET FIRST_NAME = ?,
                    LAST_NAME = ?,
                    BILLING_ADDRESS = ?,
                    BILLING_CITY = ?,
                    BILLING_STATE = ?,
                    BILLING_ZIP = ?,
                    EMAIL_ADDRESS = ?,
                    PHONE_NUMBER = ?,
                    TEXT_FLAG = ?,
                    COMMENTS = ?,
                    PRIMARY_CONTACT_METHOD = ?,
                    MEMBER_FLAG = ?,
                    LAST_UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE CUSTOMER_ID = ?
                """
                customer_id_int = int(customer_data['customer_id'])
                params = [
                    clean_data['first_name'],
                    clean_data['last_name'],
                    clean_data['billing_address'],
                    clean_data['billing_city'],
                    clean_data['billing_state'],
                    clean_data['billing_zip'],
                    clean_data['email_address'],
                    clean_data['phone_number'],
                    clean_data['text_flag'],
                    clean_data['comments'],
                    clean_data['primary_contact_method'],
                    clean_data['member_flag'],
                    customer_id_int
                ]
                if st.session_state.get('debug_mode'):
                    debug_print(f"Update Query: {query}")
                    debug_print(f"Update Params: {params}")
                snowflake_conn.execute_query(query, params)
                saved_customer_id = customer_id_int
            else:
                query = """
                INSERT INTO OPERATIONAL.CARPET.CUSTOMER (
                    FIRST_NAME,
                    LAST_NAME,
                    BILLING_ADDRESS,
                    BILLING_CITY,
                    BILLING_STATE,
                    BILLING_ZIP,
                    EMAIL_ADDRESS,
                    PHONE_NUMBER,
                    TEXT_FLAG,
                    COMMENTS,
                    PRIMARY_CONTACT_METHOD,
                    MEMBER_FLAG
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
                params = [
                    clean_data['first_name'],
                    clean_data['last_name'],
                    clean_data['billing_address'],
                    clean_data['billing_city'],
                    clean_data['billing_state'],
                    clean_data['billing_zip'],
                    clean_data['email_address'],
                    clean_data['phone_number'],
                    clean_data['text_flag'],
                    clean_data['comments'],
                    clean_data['primary_contact_method'],
                    clean_data['member_flag']
                ]
                if st.session_state.get('debug_mode'):
                    debug_print(f"Insert Query: {query}")
                    debug_print(f"Insert Params: {params}")
                snowflake_conn.execute_query(query, params)
                result = snowflake_conn.execute_query(
                    """
                    SELECT CUSTOMER_ID 
                    FROM OPERATIONAL.CARPET.CUSTOMER 
                    WHERE FIRST_NAME = ? 
                    AND LAST_NAME = ? 
                    AND PHONE_NUMBER = ?
                    ORDER BY CREATED_AT DESC 
                    LIMIT 1
                    """,
                    [clean_data['first_name'], clean_data['last_name'], clean_data['phone_number']]
                )
                saved_customer_id = result[0]['CUSTOMER_ID'] if result else None

            # Save the service address if the customer was saved successfully
            if saved_customer_id:
                is_primary = not bool(customer_data.get('different_billing', False))
                address_id = self.save_service_address(
                    snowflake_conn=snowflake_conn,
                    customer_id=saved_customer_id,
                    data=customer_data,
                    is_primary=is_primary
                )
                if not address_id:
                    st.error("Failed to save service address")
                    return None
                return saved_customer_id
            return None
        except Exception as e:
            st.error(f"Error saving customer: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)
            return None

    def display_account_service_addresses(self) -> None:
        """Display a section on the page to add one or more service addresses for an account.
        Each address includes street, city, state, ZIP code, square footage, and a primary flag.
        The entered addresses are stored in session state and can be saved with a button click.
        Debug logging is added.
        """
        st.markdown("### Service Addresses for Account")
        
        # Initialize the list in session state if it doesn't exist
        if "account_service_addresses" not in st.session_state:
            st.session_state["account_service_addresses"] = []

        # Input fields for a new service address
        new_service_address = st.text_input("Service Street Address", key="new_service_address")
        new_service_city = st.text_input("Service City", key="new_service_city")
        new_service_state = st.text_input("Service State", key="new_service_state")
        new_service_zip = st.text_input("Service ZIP Code", key="new_service_zip")
        new_service_sqft = st.number_input("Square Footage", min_value=0, step=100, key="new_service_sqft")
        is_primary_service = st.checkbox("Primary Service Address", key="is_primary_service")
        
        if st.button("Add Service Address"):
            address = {
                "service_address": new_service_address,
                "service_city": new_service_city,
                "service_state": new_service_state,
                "service_zip": new_service_zip,
                "service_sq_ft": new_service_sqft,
                "is_primary": is_primary_service
            }
            debug_print(f"Adding new service address: {address}")
            st.session_state["account_service_addresses"].append(address)
            st.success("Service address added.")
            st.experimental_rerun()  # Refresh to show updated list

        # Display current addresses
        if st.session_state["account_service_addresses"]:
            st.write("Current Service Addresses:")
            for idx, addr in enumerate(st.session_state["account_service_addresses"], 1):
                st.write(f"{idx}. {addr['service_address']}, {addr['service_city']}, {addr['service_state']} {addr['service_zip']} (Sq Ft: {addr['service_sq_ft']}) - {'Primary' if addr['is_primary'] else 'Secondary'}")
        
        # Button to save all service addresses for the account
        if st.button("Save Service Addresses"):
            account_id = self.form_data.customer_data.get('account_id')
            if not account_id:
                st.error("Please save the account first.")
            else:
                saved_any = False
                from database.connection import SnowflakeConnection  # Ensure connection import is available
                snowflake_conn = SnowflakeConnection.get_instance()
                for addr in st.session_state["account_service_addresses"]:
                    result = save_account_service_address(snowflake_conn, account_id, addr)
                    if result:
                        saved_any = True
                        debug_print(f"Saved service address: {addr}")
                    else:
                        st.error(f"Failed to save service address: {addr}")
                if saved_any:
                    st.success("Service addresses saved successfully.")
                    # Clear the list after saving
                    st.session_state["account_service_addresses"] = []
                    st.experimental_rerun()

    def display_service_selection(self) -> bool:
        """Display service selection and pricing section."""
        try:
            debug_print("Starting service selection display...")
            services_df = fetch_services()
            debug_print(f"Services DataFrame shape: {services_df.shape if not services_df.empty else 'empty'}")
            
            if services_df.empty:
                st.error("No services available")
                return False
            if 'service_costs' not in st.session_state:
                st.session_state.service_costs = {}
            
            # Initialize create service state
            if 'show_create_service' not in st.session_state:
                st.session_state.show_create_service = False

            col1, col2 = st.columns([3, 1])
            
            with col1:
                selected_services = st.multiselect(
                    "Select Services",
                    options=services_df['SERVICE_NAME'].tolist(),
                    default=st.session_state.get('selected_services', []),
                    key="services_select"
                )
            
            with col2:
                if st.button("➕ Create New Service", use_container_width=True, help="Create a new service type if it's not in the list above"):
                    st.session_state.show_create_service = True
                    st.rerun()
            
            # Handle create new service
            if st.session_state.show_create_service:
                st.markdown("---")
                st.info("💡 **Tip:** Use this to create a new service type that doesn't exist in the list above. The new service will be available for future bookings.")
                from utils.service_utils import display_create_service_form
                
                create_result = display_create_service_form(key_suffix="new_service_page")
                
                if create_result == "cancelled":
                    st.session_state.show_create_service = False
                    st.rerun()
                elif create_result:
                    # Service was created successfully
                    st.session_state.show_create_service = False
                    # Add the new service to selected services
                    new_service_name = create_result['service_name']
                    if new_service_name not in selected_services:
                        selected_services.append(new_service_name)
                        st.session_state.selected_services = selected_services
                    # Clear the services cache so it refreshes with the new service
                    try:
                        fetch_services.clear()  # Clear Streamlit cache for fetch_services function
                    except Exception as cache_error:
                        debug_print(f"Cache clear error (non-critical): {cache_error}")
                        # This is not critical, continue execution
                    st.success(f"Service '{new_service_name}' created and added to selection!")
                    st.rerun()
                
                # Don't continue with the rest of the form while creating service
                return False
            
            st.session_state.selected_services = selected_services
            self.form_data.service_selection['selected_services'] = selected_services

            if selected_services:
                # Re-fetch services to ensure newly created services are included
                current_services_df = fetch_services()
                try:
                    total_cost = sum(
                        float(current_services_df.loc[current_services_df['SERVICE_NAME'] == service, 'COST'].iloc[0])
                        for service in selected_services
                    )
                except (IndexError, KeyError):
                    # If a service is not found, re-fetch again (race condition handling)
                    fetch_services.clear()
                    current_services_df = fetch_services()
                    total_cost = sum(
                        float(current_services_df.loc[current_services_df['SERVICE_NAME'] == service, 'COST'].iloc[0])
                        for service in selected_services
                    )
                st.write(f"Total Cost: ${total_cost:.2f}")

                # Recurring Service
                if 'is_recurring' not in st.session_state:
                    st.session_state.is_recurring = False
                is_recurring = st.checkbox(
                    "Recurring Service",
                    key="recurring_checkbox",
                    value=st.session_state.is_recurring
                )
                st.session_state.is_recurring = is_recurring
                self.form_data.service_selection['is_recurring'] = is_recurring

                if 'recurrence_pattern' not in st.session_state or st.session_state.recurrence_pattern is None:
                    st.session_state.recurrence_pattern = "Weekly"

                if is_recurring:
                    current_pattern = st.session_state.recurrence_pattern or "Weekly"
                    pattern_options = ["Weekly", "Bi-Weekly", "Monthly"]
                    try:
                        pattern_index = pattern_options.index(current_pattern)
                    except ValueError:
                        pattern_index = 0
                    recurrence_pattern = st.selectbox(
                        "Recurrence Pattern",
                        options=pattern_options,
                        index=pattern_index,
                        key="recurrence_select"
                    )
                    st.session_state.recurrence_pattern = recurrence_pattern
                    self.form_data.service_selection['recurrence_pattern'] = recurrence_pattern
                else:
                    self.form_data.service_selection['recurrence_pattern'] = None

                # Deposit
                if 'deposit_amount' not in st.session_state:
                    st.session_state.deposit_amount = 0.0
                add_deposit = st.checkbox("Add Deposit", key="deposit_checkbox")
                deposit_amount = 0.0
                if add_deposit:
                    if is_recurring:
                        st.info("For recurring services, deposit only applies to the first service.")
                    deposit_amount = st.number_input(
                        "Deposit Amount",
                        min_value=0.0,
                        max_value=total_cost,
                        value=st.session_state.deposit_amount,
                        step=5.0,
                        key="deposit_input"
                    )
                    st.session_state.deposit_amount = deposit_amount
                    st.write(f"Remaining Balance: ${total_cost - deposit_amount:.2f}")
                    if is_recurring:
                        st.write(f"Future Service Cost: ${total_cost:.2f}")

                # Notes
                if 'service_notes' not in st.session_state:
                    st.session_state.service_notes = ''
                notes = st.text_area(
                    "Additional Instructions or Requirements",
                    value=st.session_state.service_notes,
                    key="notes_input"
                )
                st.session_state.service_notes = notes

                self.form_data.service_selection.update({
                    'selected_services': selected_services,
                    'is_recurring': is_recurring,
                    'recurrence_pattern': st.session_state.recurrence_pattern if is_recurring else None,
                    'deposit_amount': deposit_amount,
                    'notes': notes
                })
                return True
            return False
        except Exception as e:
            st.error(f"Error in service selection: {str(e)}")
            st.error(f"Error type: {type(e).__name__}")
            import traceback
            st.error(f"Traceback: {traceback.format_exc()}")
            
            # Try to identify the specific issue
            if "SERVICE_NAME" in str(e):
                st.error("Issue with service name column - check database schema")
            elif "COST" in str(e):
                st.error("Issue with service cost column - check database schema")
            elif "clear" in str(e):
                st.error("Issue with cache clearing - this is non-critical")
                
            if st.session_state.get('debug_mode'):
                st.exception(e)
            return False

    def save_service(self) -> bool:
        """Save complete service booking and send confirmation email if needed."""
        try:
            # Commercial accounts
            if self.form_data.customer_data['is_commercial']:
                account_id = self.save_account_and_get_id()
                if not account_id:
                    return False
                self.form_data.customer_data['account_id'] = account_id
            else:
                # Residential
                validation_errors = self.validate_customer_data()
                if validation_errors:
                    for error in validation_errors:
                        st.error(error)
                    return False
                customer_id = self.save_customer_and_get_id(self.form_data.customer_data)
                if not customer_id:
                    return False
                self.form_data.customer_data['customer_id'] = customer_id

            # Calculate total cost
            services_df = fetch_services()
            total_cost = sum(
                float(services_df[services_df['SERVICE_NAME'] == service]['COST'].iloc[0])
                for service in self.form_data.service_selection['selected_services']
            )
            service_data = {
                'customer_id': self.form_data.customer_data.get('customer_id'),
                'account_id': self.form_data.customer_data.get('account_id'),
                'service_name': self.form_data.service_selection['selected_services'][0],
                'service_date': self.form_data.service_schedule['date'],
                'service_time': self.form_data.service_schedule['time'],
                'deposit': float(self.form_data.service_selection['deposit_amount']),
                'notes': self.form_data.service_selection.get('notes'),
                'is_recurring': bool(self.form_data.service_selection['is_recurring']),
                'recurrence_pattern': self.form_data.service_selection['recurrence_pattern']
            }

            # Save service schedule
            service_scheduled = save_service_schedule(
                customer_id=service_data['customer_id'],
                account_id=service_data['account_id'],
                services=self.form_data.service_selection['selected_services'],
                service_date=service_data['service_date'],
                service_time=service_data['service_time'],
                deposit_amount=service_data['deposit'],
                notes=service_data['notes'],
                is_recurring=service_data['is_recurring'],
                recurrence_pattern=service_data['recurrence_pattern'],
                customer_data=self.form_data.customer_data
            )
            if not service_scheduled:
                st.error("Failed to schedule service")
                return False

            success_message = [
                "Service scheduled successfully!",
                f"Deposit Amount: {format_currency(service_data['deposit'])}",
                f"Remaining Balance: {format_currency(total_cost - service_data['deposit'])}"
            ]
            if service_data['is_recurring']:
                success_message.append(f"Recurring: {service_data['recurrence_pattern']}")

            # Optional: send email if we have an address
            if self.form_data.customer_data.get('email_address'):
                service_details = {
                    'customer_name': (
                        self.form_data.customer_data.get('business_name')
                        if self.form_data.customer_data['is_commercial']
                        else f"{self.form_data.customer_data.get('first_name', '')} {self.form_data.customer_data.get('last_name', '')}"
                    ).strip(),
                    'customer_email': self.form_data.customer_data['email_address'],
                    'service_type': ', '.join(self.form_data.service_selection['selected_services']),
                    'date': service_data['service_date'].strftime('%Y-%m-%d'),
                    'time': service_data['service_time'].strftime('%I:%M %p'),
                    'deposit_required': service_data['deposit'] > 0,
                    'deposit_amount': service_data['deposit'],
                    'deposit_paid': False,
                    'notes': service_data['notes'],
                    'total_cost': total_cost
                }
                business_info = fetch_business_info()
                if not business_info:
                    success_message.append("Note: Unable to send confirmation - missing business info")
                else:
                    # Get customer's preferred contact method
                    preferred_method = self.form_data.customer_data.get('primary_contact_method', 'SMS')
                    
                    # Try to send via preferred method first
                    notification_sent = False
                    
                    if preferred_method == 'SMS' and self.form_data.customer_data.get('phone_number'):
                        sms_result = send_service_notification_sms(
                            customer_phone=self.form_data.customer_data['phone_number'],
                            service_details=service_details,
                            business_info=business_info,
                            notification_type="scheduled"
                        )
                        if sms_result and sms_result.success:
                            success_message.append("Confirmation SMS sent!")
                            notification_sent = True
                        else:
                            error_msg = sms_result.message if sms_result else "Unknown SMS error"
                            success_message.append(f"SMS failed ({error_msg}), trying email...")
                    
                    # If SMS failed or email is preferred, try email
                    if not notification_sent and self.form_data.customer_data.get('email_address'):
                        email_result = generate_service_scheduled_email(service_details, business_info)
                        if email_result and email_result.success:
                            success_message.append("Confirmation email sent!")
                            notification_sent = True
                        else:
                            error_msg = email_result.message if email_result else "Unknown email error"
                            success_message.append(f"Email also failed: {error_msg}")
                    
                    # If both failed or no contact info
                    if not notification_sent:
                        if preferred_method == 'Phone':
                            success_message.append("Phone confirmation preferred - please call customer")
                        else:
                            success_message.append("Note: Unable to send automatic confirmation")

            st.session_state['success_message'] = '\n'.join(success_message)
            st.session_state['show_notification'] = True
            st.session_state['page'] = 'scheduled_services'

            # Clear form data
            st.session_state.form_data = ServiceFormData.initialize()

            # Trigger rerun to navigate to scheduled services page
            st.rerun()
            return True

        except Exception as e:
            st.error(f"Error saving service: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)
            return False

    def validate_customer_data(self) -> List[str]:
        """Validate customer form data and return list of error messages."""
        errors = []
        
        try:
            customer_data = self.form_data.customer_data
            
            # Basic validation
            if not customer_data.get('first_name'):
                errors.append("First name is required")
            if not customer_data.get('last_name'):
                errors.append("Last name is required")
            if not customer_data.get('phone_number'):
                errors.append("Phone number is required")
            
            # Phone validation
            phone = customer_data.get('phone_number', '')
            if phone and not validate_phone(phone):
                errors.append("Please enter a valid phone number")
            
            # Email validation (if provided)
            email = customer_data.get('email_address', '')
            if email and not validate_email(email):
                errors.append("Please enter a valid email address")
            
            # Address validation
            if not customer_data.get('primary_street'):
                errors.append("Primary address street is required")
            if not customer_data.get('primary_city'):
                errors.append("Primary address city is required")
            if not customer_data.get('primary_state'):
                errors.append("Primary address state is required")
            if not customer_data.get('primary_zip'):
                errors.append("Primary address ZIP code is required")
            else:
                zip_code = customer_data.get('primary_zip', '')
                if not validate_zip_code(zip_code):
                    errors.append("Please enter a valid ZIP code")
            
            return errors
            
        except Exception as e:
            st.error(f"Error validating customer data: {str(e)}")
            return ["Validation error occurred"]

def save_account_service_address(snowflake_conn: Any, account_id: int, data: Dict[str, Any]) -> Optional[int]:
    """Save a service address for a commercial account.
    Although the SERVICE_ADDRESSES table uses CUSTOMER_ID, for commercial accounts we pass the account_id.
    Debug logging is added.
    """
    try:
        debug_print(f"Saving service address for account_id {account_id} with data: {data}")
        # Sanitize ZIP code (assuming similar helper exists)
        service_zip = sanitize_zip_code(data.get('service_zip') or data.get('service_zip_code'))
        if not service_zip:
            st.error("Invalid service address ZIP code format. Please enter a 5-digit number.")
            return None
        
        query = """
        INSERT INTO OPERATIONAL.CARPET.SERVICE_ADDRESSES (
            ACCOUNT_ID,
            STREET_ADDRESS,
            CITY,
            STATE,
            ZIP_CODE,
            SQUARE_FOOTAGE,
            IS_PRIMARY_SERVICE
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        params = [
            account_id,  # Using proper ACCOUNT_ID field for commercial accounts
            str(data.get('service_address', '')).strip(),
            str(data.get('service_city', '')).strip(),
            str(data.get('service_state', '')).strip(),
            service_zip,
            int(data.get('service_sq_ft', 0)),
            bool(data.get('is_primary'))
        ]
        debug_print(f"Service address query params: {params}")
        snowflake_conn.execute_query(query, params)
        
        # Retrieve the newly created address ID
        result = snowflake_conn.execute_query(
            """
            SELECT ADDRESS_ID 
            FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES 
            WHERE ACCOUNT_ID = ? 
            ORDER BY CREATED_AT DESC 
            LIMIT 1
            """,
            [account_id]
        )
        address_id = result[0]['ADDRESS_ID'] if result else None
        debug_print(f"Service address saved with ADDRESS_ID: {address_id}")
        return address_id
    except Exception as e:
        st.error(f"Error saving service address: {str(e)}")
        debug_print(f"Exception in save_account_service_address: {str(e)}")
        return None


def new_service_page():
    """Main service scheduling page with improved UI organization."""
    try:
                # Residential
                validation_errors = self.validate_customer_data()
                if validation_errors:
                    for error in validation_errors:
                        st.error(error)
                    return False
                customer_id = self.save_customer_and_get_id(self.form_data.customer_data)
                if not customer_id:
                    return False
                self.form_data.customer_data['customer_id'] = customer_id

            # Calculate total cost
            services_df = fetch_services()
            total_cost = sum(
                float(services_df[services_df['SERVICE_NAME'] == service]['COST'].iloc[0])
                for service in self.form_data.service_selection['selected_services']
            )
            service_data = {
                'customer_id': self.form_data.customer_data.get('customer_id'),
                'account_id': self.form_data.customer_data.get('account_id'),
                'service_name': self.form_data.service_selection['selected_services'][0],
                'service_date': self.form_data.service_schedule['date'],
                'service_time': self.form_data.service_schedule['time'],
                'deposit': float(self.form_data.service_selection['deposit_amount']),
                'notes': self.form_data.service_selection.get('notes'),
                'is_recurring': bool(self.form_data.service_selection['is_recurring']),
                'recurrence_pattern': self.form_data.service_selection['recurrence_pattern']
            }

            # Save service schedule
            service_scheduled = save_service_schedule(
                customer_id=service_data['customer_id'],
                account_id=service_data['account_id'],
                services=self.form_data.service_selection['selected_services'],
                service_date=service_data['service_date'],
                service_time=service_data['service_time'],
                deposit_amount=service_data['deposit'],
                notes=service_data['notes'],
                is_recurring=service_data['is_recurring'],
                recurrence_pattern=service_data['recurrence_pattern'],
                customer_data=self.form_data.customer_data
            )
            if not service_scheduled:
                st.error("Failed to schedule service")
                return False

            success_message = [
                "Service scheduled successfully!",
                f"Deposit Amount: {format_currency(service_data['deposit'])}",
                f"Remaining Balance: {format_currency(total_cost - service_data['deposit'])}"
            ]
            if service_data['is_recurring']:
                success_message.append(f"Recurring: {service_data['recurrence_pattern']}")

            # Optional: send email if we have an address
            if self.form_data.customer_data.get('email_address'):
                service_details = {
                    'customer_name': (
                        self.form_data.customer_data.get('business_name')
                        if self.form_data.customer_data['is_commercial']
                        else f"{self.form_data.customer_data.get('first_name', '')} {self.form_data.customer_data.get('last_name', '')}"
                    ).strip(),
                    'customer_email': self.form_data.customer_data['email_address'],
                    'service_type': ', '.join(self.form_data.service_selection['selected_services']),
                    'date': service_data['service_date'].strftime('%Y-%m-%d'),
                    'time': service_data['service_time'].strftime('%I:%M %p'),
                    'deposit_required': service_data['deposit'] > 0,
                    'deposit_amount': service_data['deposit'],
                    'deposit_paid': False,
                    'notes': service_data['notes'],
                    'total_cost': total_cost
                }
                business_info = fetch_business_info()
                if not business_info:
                    success_message.append("Note: Unable to send confirmation - missing business info")
                else:
                    # Get customer's preferred contact method
                    preferred_method = self.form_data.customer_data.get('primary_contact_method', 'SMS')
                    
                    # Try to send via preferred method first
                    notification_sent = False
                    
                    if preferred_method == 'SMS' and self.form_data.customer_data.get('phone_number'):
                        sms_result = send_service_notification_sms(
                            customer_phone=self.form_data.customer_data['phone_number'],
                            service_details=service_details,
                            business_info=business_info,
                            notification_type="scheduled"
                        )
                        if sms_result and sms_result.success:
                            success_message.append("Confirmation SMS sent!")
                            notification_sent = True
                        else:
                            error_msg = sms_result.message if sms_result else "Unknown SMS error"
                            success_message.append(f"SMS failed ({error_msg}), trying email...")
                    
                    # If SMS failed or email is preferred, try email
                    if not notification_sent and self.form_data.customer_data.get('email_address'):
                        email_result = generate_service_scheduled_email(service_details, business_info)
                        if email_result and email_result.success:
                            success_message.append("Confirmation email sent!")
                            notification_sent = True
                        else:
                            error_msg = email_result.message if email_result else "Unknown email error"
                            success_message.append(f"Email also failed: {error_msg}")
                    
                    # If both failed or no contact info
                    if not notification_sent:
                        if preferred_method == 'Phone':
                            success_message.append("Phone confirmation preferred - please call customer")
                        else:
                            success_message.append("Note: Unable to send automatic confirmation")

            st.session_state['success_message'] = '\n'.join(success_message)
            st.session_state['show_notification'] = True
            st.session_state['page'] = 'scheduled_services'

            # Clear form data
            st.session_state.form_data = ServiceFormData.initialize()

            # Trigger rerun to navigate to scheduled services page
            st.rerun()
            return True

        except Exception as e:
            st.error(f"Error saving service: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)
            return False

    def display_customer_selector(self, matching_customers: List[str], customers_df: pd.DataFrame) -> None:
        """Example method if you want to show a dropdown of matching customers."""
        try:
            if not matching_customers or customers_df is None:
                return
            selected_customer = st.selectbox(
                "Select Customer",
                options=["Select..."] + matching_customers,
                key="customer_select"
            )
            if selected_customer and selected_customer != "Select...":
                customer_details = customers_df[
                    customers_df['FULL_NAME'] == selected_customer
                ].iloc[0]
                self.form_data.customer_data.update({
                    'customer_id': int(customer_details['CUSTOMER_ID']),
                    'first_name': customer_details.get('FIRST_NAME', ''),
                    'last_name': customer_details.get('LAST_NAME', ''),
                    'phone_number': customer_details.get('PHONE_NUMBER', ''),
                    'email_address': customer_details.get('EMAIL_ADDRESS', ''),
                    'primary_contact_method': customer_details.get('PRIMARY_CONTACT_METHOD', 'Phone'),
                    'text_flag': customer_details.get('TEXT_FLAG', False),
                    'is_commercial': False,
                    'service_address': customer_details.get('SERVICE_ADDRESS', ''),
                    'service_city': customer_details.get('SERVICE_CITY', ''),
                    'service_state': customer_details.get('SERVICE_STATE', ''),
                    'service_zip': customer_details.get('SERVICE_ZIP', ''),
                    'service_addr_sq_ft': customer_details.get('SERVICE_ADDR_SQ_FT', 0),
                    'billing_address': customer_details.get('BILLING_ADDRESS', ''),
                    'billing_city': customer_details.get('BILLING_CITY', ''),
                    'billing_state': customer_details.get('BILLING_STATE', ''),
                    'billing_zip': customer_details.get('BILLING_ZIP', '')
                })
                self.display_customer_details(customer_details)
            else:
                self.display_customer_form()
        except Exception as e:
            st.error(f"Error selecting customer: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)

    def handle_customer_search(self) -> None:
        """Process residential customer search and selection."""
        customer_name = st.text_input(
            "Search Customer",
            help="Enter customer name to search"
        )
        if customer_name:
            existing_customers_df = fetch_all_customers()
            if not existing_customers_df.empty:
                search_term = customer_name.lower()
                matching_customers = [
                    f"{row['FIRST_NAME']} {row['LAST_NAME']}"
                    for _, row in existing_customers_df.iterrows()
                    if search_term in f"{row['FIRST_NAME']} {row['LAST_NAME']}".lower() or
                       search_term in str(row['PHONE_NUMBER']).lower() or
                       (pd.notnull(row['EMAIL_ADDRESS']) and search_term in str(row['EMAIL_ADDRESS']).lower())
                ]
                if matching_customers:
                    selected_customer = st.selectbox(
                        "Select Customer",
                        options=["Select..."] + matching_customers,
                        key="customer_select"
                    )
                    if selected_customer and selected_customer != "Select...":
                        customer_details = existing_customers_df[
                            (existing_customers_df['FIRST_NAME'] + " " + existing_customers_df['LAST_NAME']) == selected_customer
                        ].iloc[0]
                        self.form_data.customer_data.update({
                            'customer_id': int(customer_details['CUSTOMER_ID']),
                            'first_name': customer_details.get('FIRST_NAME', ''),
                            'last_name': customer_details.get('LAST_NAME', ''),
                            'phone_number': customer_details.get('PHONE_NUMBER', ''),
                            'email_address': customer_details.get('EMAIL_ADDRESS', ''),
                            'primary_contact_method': customer_details.get('PRIMARY_CONTACT_METHOD', 'Phone'),
                            'text_flag': customer_details.get('TEXT_FLAG', False),
                            'is_commercial': False,
                            'service_address': customer_details.get('SERVICE_ADDRESS', ''),
                            'service_city': customer_details.get('SERVICE_CITY', ''),
                            'service_state': customer_details.get('SERVICE_STATE', ''),
                            'service_zip': customer_details.get('SERVICE_ZIP', ''),
                            'service_addr_sq_ft': customer_details.get('SERVICE_ADDR_SQ_FT', 0),
                            'billing_address': customer_details.get('BILLING_ADDRESS', ''),
                            'billing_city': customer_details.get('BILLING_CITY', ''),
                            'billing_state': customer_details.get('BILLING_STATE', ''),
                            'billing_zip': customer_details.get('BILLING_ZIP', '')
                        })
                        self.display_customer_details(customer_details)
                else:
                    st.info("No matching customers found. Please enter customer details below.")
                    self.form_data.customer_data.update({
                        'customer_id': None,
                        'first_name': customer_name.split(' ')[0] if ' ' in customer_name else customer_name,
                        'last_name': ' '.join(customer_name.split(' ')[1:]) if ' ' in customer_name else '',
                        'phone_number': '',
                        'email_address': '',
                        'primary_contact_method': 'SMS',
                        'text_flag': False,
                        'is_commercial': False,
                        'service_address': '',
                        'service_city': '',
                        'service_state': '',
                        'service_zip': '',
                        'service_addr_sq_ft': 0,
                        'billing_address': '',
                        'billing_city': '',
                        'billing_state': '',
                        'billing_zip': ''
                    })
                    self.display_customer_form()
            else:
                st.info("No customers found in system. Please enter new customer details.")
                self.form_data.customer_data.update({
                    'customer_id': None,
                    'is_commercial': False
                })
                self.display_customer_form()
        else:
            self.display_customer_form()

    def validate_customer_data(self) -> List[str]:
        """Validate customer input data (both residential and commercial)."""
        errors = []
        data = self.form_data.customer_data

        # If not commercial => Residential validation
        if not data['is_commercial']:
            # Required fields
            if not data.get('first_name'):
                errors.append("First name is required")
            if not data.get('last_name'):
                errors.append("Last name is required")

            # Phone & email checks
            if not data['phone_number'] or not validate_phone(data['phone_number']):
                errors.append("Valid phone number is required")
            if data['email_address'] and not validate_email(data['email_address']):
                errors.append("Invalid email format")

            # Service address checks
            if not data['service_address']:
                errors.append("Service street address is required")
            if not data['service_city']:
                errors.append("Service city is required")
            if not data['service_state']:
                errors.append("Service state is required")
            if not data['service_zip']:
                errors.append("Service ZIP code is required")

            # Validate service_zip length & numeric
            try:
                if data['service_zip']:
                    zip_int = int(str(data['service_zip']))
                    if len(str(zip_int)) != 5:
                        errors.append("Service ZIP code must be exactly 5 digits")
            except ValueError:
                errors.append("Service ZIP code must be a valid 5-digit number")

            # If user indicated different billing address, validate those fields too
            if data.get('different_billing'):
                if not data['billing_address']:
                    errors.append("Billing street address is required")
                if not data['billing_city']:
                    errors.append("Billing city is required")
                if not data['billing_state']:
                    errors.append("Billing state is required")
                if not data['billing_zip']:
                    errors.append("Billing ZIP code is required")

                # Validate billing_zip length & numeric
                try:
                    if data['billing_zip']:
                        zip_int = int(str(data['billing_zip']))
                        if len(str(zip_int)) != 5:
                            errors.append("Billing ZIP code must be exactly 5 digits")
                except ValueError:
                    errors.append("Billing ZIP code must be a valid 5-digit number")

        else:
            # Commercial validation
            if not data['business_name']:
                errors.append("Business name is required")
            if not data['contact_person']:
                errors.append("Contact person is required")
            if not data['phone_number'] or not validate_phone(data['phone_number']):
                errors.append("Valid phone number is required")

        return errors


    def display_service_address_form(self) -> None:
        """Display form for entering the service address details (optional method)."""
        st.markdown("#### Service Address")
        self.form_data.customer_data['service_address'] = st.text_input(
            "Service Street Address",
            value=self.form_data.customer_data['service_address'],
            key="service_street_input"
        )
        col1, col2 = st.columns(2)
        with col1:
            self.form_data.customer_data['service_city'] = st.text_input(
                "Service City",
                value=self.form_data.customer_data['service_city'],
                key="service_city_input"
            )
        with col2:
            self.form_data.customer_data['service_state'] = st.text_input(
                "Service State",
                value=self.form_data.customer_data['service_state'],
                key="service_state_input"
            )
        self.form_data.customer_data['service_zip'] = st.text_input(
            "Service ZIP Code",
            value=self.form_data.customer_data['service_zip'],
            key="service_zip_input"
        )



def new_service_page():
    """Main service scheduling page with improved UI organization."""
    try:
        initialize_session_state()

        # Top "Home" Button
        col1, col2, col3 = st.columns([1, 10, 1])
        with col1:
            if st.button("🏠 Home", key="home_button_top"):
                st.session_state.page = "home"
                st.rerun()

        st.markdown("""
            <div style='text-align: center; padding: 1rem'>
                <h2>Schedule New Service</h2>
            </div>
        """, unsafe_allow_html=True)

        # Debug Mode Toggle
        if st.secrets.get("environment") == "development":
            st.sidebar.checkbox("Debug Mode", key="debug_mode")

        # Initialize or fetch the scheduler
        try:
            if 'scheduler' not in st.session_state:
                st.session_state.scheduler = ServiceScheduler()
                debug_print("Initialized new scheduler")
            scheduler = st.session_state.scheduler
        except Exception as e:
            st.error("Error initializing service scheduler")
            debug_print(f"Scheduler initialization error: {str(e)}")
            return

        # --- Clear selected services/deposit if user toggles Residential ↔ Commercial ---
        old_customer_type = st.session_state.get("old_customer_type", None)
        customer_type = st.radio("Service For", ["Residential", "Commercial"], horizontal=True, key="customer_type")

        # If user changed type from last time, clear relevant session fields
        if old_customer_type is not None and old_customer_type != customer_type:
            st.session_state.selected_services = []
            st.session_state.deposit_amount = 0.0
            st.session_state.service_notes = ''
            st.session_state.recurrence_pattern = None
            st.session_state.is_recurring = False

        st.session_state["old_customer_type"] = customer_type
        # --- End clearing logic ---

        try:
            if customer_type == "Commercial":
                st.markdown("### Business Account")
                scheduler.handle_account_search()
            else:
                st.markdown("### Customer Information")
                search_col1, search_col2 = st.columns([2, 1])
                with search_col1:
                    customer_name = st.text_input(
                        "Search by Name, Phone, or Email",
                        help="Enter customer name, phone number, or email to search",
                        key="search_customer"
                    )
                with search_col2:
                    st.markdown("<br>", unsafe_allow_html=True)
                    new_customer = st.checkbox("New Customer", key="new_customer_checkbox")

                if customer_name and not new_customer:
                    # Searching existing customers
                    try:
                        all_customers_df = fetch_all_customers()
                        if st.session_state.get('debug_mode'):
                            st.write("Debug - Fetched customers:", len(all_customers_df))

                        if not all_customers_df.empty:
                            search_term = customer_name.lower()
                            if 'FULL_NAME' not in all_customers_df.columns:
                                all_customers_df['FULL_NAME'] = all_customers_df['FIRST_NAME'] + ' ' + all_customers_df['LAST_NAME']

                            matching_customers_df = all_customers_df[
                                all_customers_df['FULL_NAME'].str.lower().str.contains(search_term, na=False) |
                                all_customers_df['PHONE_NUMBER'].str.lower().str.contains(search_term, na=False) |
                                all_customers_df['EMAIL_ADDRESS'].str.lower().str.contains(search_term, na=False)
                            ]

                            if not matching_customers_df.empty:
                                selected_option = st.selectbox(
                                    "Select Customer",
                                    options=["Select..."] + matching_customers_df['FULL_NAME'].tolist(),
                                    key="customer_select_box"
                                )
                                if selected_option != "Select...":
                                    customer = matching_customers_df[
                                        matching_customers_df['FULL_NAME'] == selected_option
                                    ].iloc[0]
                                    # Basic Customer Info
                                    st.markdown("### Customer Details")
                                    col1, col2 = st.columns(2)
                                    with col1:
                                        scheduler.form_data.customer_data['first_name'] = st.text_input(
                                            "First Name",
                                            value=customer['FIRST_NAME'],
                                            key="edit_first_name"
                                        )
                                        scheduler.form_data.customer_data['last_name'] = st.text_input(
                                            "Last Name",
                                            value=customer['LAST_NAME'],
                                            key="edit_last_name"
                                        )
                                    with col2:
                                        scheduler.form_data.customer_data['phone_number'] = st.text_input(
                                            "Phone",
                                            value=customer['PHONE_NUMBER'],
                                            key="edit_phone"
                                        )
                                        scheduler.form_data.customer_data['email_address'] = st.text_input(
                                            "Email",
                                            value=customer['EMAIL_ADDRESS'],
                                            key="edit_email"
                                        )

                                    # Service Address
                                    st.markdown("### Service Address")
                                    scheduler.form_data.customer_data['service_address'] = st.text_input(
                                        "Street Address",
                                        value=customer['SERVICE_ADDRESS'],
                                        key="service_address_input"
                                    )
                                    service_col1, service_col2 = st.columns(2)
                                    with service_col1:
                                        scheduler.form_data.customer_data['service_city'] = st.text_input(
                                            "City",
                                            value=customer['SERVICE_CITY'],
                                            key="service_city_input"
                                        )
                                        scheduler.form_data.customer_data['service_state'] = st.text_input(
                                            "State",
                                            value=customer['SERVICE_STATE'],
                                            key="service_state_input"
                                        )
                                    with service_col2:
                                        scheduler.form_data.customer_data['service_zip'] = st.text_input(
                                            "ZIP Code",
                                            value=str(customer['SERVICE_ZIP']),
                                            key="service_zip_input"
                                        )

                                    # Different Billing?
                                    different_billing = st.checkbox(
                                        "Different Billing Address",
                                        value=False,
                                        key="different_billing_checkbox"
                                    )
                                    scheduler.form_data.customer_data['different_billing'] = different_billing
                                    if different_billing:
                                        st.markdown("### Billing Address")
                                        scheduler.form_data.customer_data['billing_address'] = st.text_input(
                                            "Street Address",
                                            value=customer.get('BILLING_ADDRESS', ''),
                                            key="billing_street"
                                        )
                                        bill_col1, bill_col2 = st.columns(2)
                                        with bill_col1:
                                            scheduler.form_data.customer_data['billing_city'] = st.text_input(
                                                "City",
                                                value=customer.get('BILLING_CITY', ''),
                                                key="billing_city"
                                            )
                                            scheduler.form_data.customer_data['billing_state'] = st.text_input(
                                                "State",
                                                value=customer.get('BILLING_STATE', ''),
                                                key="billing_state"
                                            )
                                        with bill_col2:
                                            scheduler.form_data.customer_data['billing_zip'] = st.text_input(
                                                "ZIP Code",
                                                value=str(customer.get('BILLING_ZIP', '')) if pd.notnull(customer.get('BILLING_ZIP')) else "",
                                                key="billing_zip"
                                            )
                                    else:
                                        scheduler.form_data.customer_data.update({
                                            'billing_address': scheduler.form_data.customer_data.get('service_address', ''),
                                            'billing_city': scheduler.form_data.customer_data.get('service_city', ''),
                                            'billing_state': scheduler.form_data.customer_data.get('service_state', ''),
                                            'billing_zip': scheduler.form_data.customer_data.get('service_zip', '')
                                        })

                                    # Update ID and flags
                                    scheduler.form_data.customer_data['customer_id'] = customer['CUSTOMER_ID']
                                    scheduler.form_data.customer_data['is_commercial'] = False
                            else:
                                st.info("No matching customers found. Please enter customer details below.")
                                scheduler.display_customer_form()
                        else:
                            st.info("No customers found in system. Please enter new customer details.")
                            scheduler.display_customer_form()
                    except Exception as e:
                        st.error(f"Error fetching customers: {str(e)}")
                        if st.session_state.get('debug_mode'):
                            st.exception(e)
                        scheduler.display_customer_form()
                else:
                    # If no search term or user clicked "New Customer"
                    scheduler.display_customer_form()

            # If we have at least a name or an account_id, show service selection
            show_service_section = (
                scheduler.form_data.customer_data.get("account_id") is not None or
                bool(scheduler.form_data.customer_data.get("first_name"))
            )
            if show_service_section:
                st.markdown("### Select Services")
                services_selected = scheduler.display_service_selection()
                if services_selected:
                    st.markdown("### Schedule Service")
                    schedule_selected = scheduler.process_service_scheduling()
                    if schedule_selected:
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("📅 Schedule Service", type="primary", key="schedule_service_button"):
                                try:
                                    if scheduler.save_service():
                                        st.success("Service scheduled successfully!")
                                        st.balloons()
                                        reset_session_state()
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"Error saving service: {str(e)}")
                                    if st.session_state.get('debug_mode'):
                                        st.exception(e)
                        with col2:
                            if st.button("❌ Cancel", type="secondary", key="cancel_service_button"):
                                reset_session_state()
                                st.rerun()

        except Exception as e:
            st.error(f"An error occurred while processing the form: {str(e)}")
            debug_print(f"Form processing error: {str(e)}")
            st.error(f"Error details: {type(e).__name__}: {str(e)}")
            import traceback
            st.error(f"Traceback: {traceback.format_exc()}")
            if st.session_state.get('debug_mode'):
                st.exception(e)

    except Exception as e:
        st.error("An unexpected error occurred")
        debug_print(f"Page initialization error: {str(e)}")
        if st.session_state.get('debug_mode'):
            st.exception(e)


# Run page if invoked directly
if __name__ == "__main__":
    new_service_page()
