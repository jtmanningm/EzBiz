# new_service.py
import streamlit as st
from datetime import datetime, date, time, timedelta
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
import pandas as pd
import json

from models.transaction import save_transaction
from models.customer import CustomerModel, fetch_all_customers, save_customer
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
from utils.validation import validate_phone, validate_email, validate_zip_code
from utils.email import generate_service_scheduled_email
from utils.email import generate_service_completed_email 
from pages.settings.business import fetch_business_info  # Add this import

def debug_print(msg: str) -> None:
    """Helper function for debug logging with defensive access to debug_mode."""
    if st.session_state.get('debug_mode', False):  # Default to False if not set
        print(f"DEBUG: {msg}")
        st.write(f"DEBUG: {msg}")

def initialize_session_state() -> None:
    """Initialize required session state variables"""
    if 'deposit_amount' not in st.session_state:
        st.session_state.deposit_amount = 0.0
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
        'selected_customer_id',  # Add this to clear customer selection
        # Add new customer form fields
        'new_customer_name',
        'new_customer_phone',
        'new_customer_email',
        'new_customer_street',
        'new_customer_city',
        'new_customer_state',
        'new_customer_zip',
        'customer_select',  # Clear customer selection dropdown
        'search_customer'  # Clear search field
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

    @classmethod
    def initialize(cls) -> 'ServiceFormData':
        """Initialize form data with default values"""
        # Initialize service selection in session state if not present
        if 'selected_services' not in st.session_state:
            st.session_state.selected_services = []
        if 'is_recurring' not in st.session_state:
            st.session_state.is_recurring = False
        if 'recurrence_pattern' not in st.session_state:
            st.session_state.recurrence_pattern = None
        if 'deposit_amount' not in st.session_state:
            st.session_state.deposit_amount = 0.0
        if 'service_notes' not in st.session_state:
            st.session_state.service_notes = ''

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
                'street_address': '',
                'city': '',
                'state': '',
                'zip_code': '',
                'primary_contact_method': 'Phone',
                'text_flag': False,
                'service_address': '',
                'service_city': '',
                'service_state': '',
                'service_zip': '',
                'service_address_2': None,
                'service_address_3': None,
                'service_addr_sq_ft': None,
                'comments': '',
                'member_flag': False,
                'is_commercial': False
            },
            service_selection={
                'selected_services': st.session_state.selected_services,
                'is_recurring': st.session_state.is_recurring,
                'recurrence_pattern': st.session_state.recurrence_pattern,
                'deposit_amount': st.session_state.deposit_amount,
                'notes': st.session_state.service_notes,
                'same_as_primary': True
            },
            service_schedule={
                'date': datetime.now().date(),
                'time': None
            }
        )

class ServiceScheduler:
    def __init__(self):
        if not hasattr(st.session_state, 'form_data'):
            st.session_state.form_data = ServiceFormData.initialize()
        self.form_data = st.session_state.form_data

    def display_customer_details(self, customer_details: pd.Series) -> None:
        """Display editable residential customer details"""
        with st.container():
            # Contact Information
            st.markdown("#### Contact Details")
            col1, col2 = st.columns(2)
            with col1:
                self.form_data.customer_data['first_name'] = st.text_input(
                    "First Name",
                    value=self.form_data.customer_data['first_name'],
                    key="details_first_name"
                )
                self.form_data.customer_data['phone_number'] = st.text_input(
                    "Phone Number",
                    value=self.form_data.customer_data['phone_number'],
                    key="details_phone"
                )

            with col2:
                self.form_data.customer_data['last_name'] = st.text_input(
                    "Last Name",
                    value=self.form_data.customer_data['last_name'],
                    key="details_last_name"
                )
                self.form_data.customer_data['email_address'] = st.text_input(
                    "Email",
                    value=self.form_data.customer_data['email_address'],
                    key="details_email"
                )

            # Contact Preferences
            self.form_data.customer_data['primary_contact_method'] = st.selectbox(
                "Preferred Contact Method",
                ["Phone", "Text", "Email"],
                index=["Phone", "Text", "Email"].index(
                    self.form_data.customer_data['primary_contact_method']
                ),
                key="details_contact_method"
            )

            self.form_data.customer_data['text_flag'] = st.checkbox(
                "Opt-in to Text Messages",
                value=self.form_data.customer_data['text_flag'],
                key="details_text_flag"
            )

            # Address Information
            st.markdown("#### Address Information")
            self.form_data.customer_data['street_address'] = st.text_input(
                "Street Address",
                value=self.form_data.customer_data['street_address'],
                key="details_street"
            )

            col1, col2 = st.columns(2)
            with col1:
                self.form_data.customer_data['city'] = st.text_input(
                    "City",
                    value=self.form_data.customer_data['city'],
                    key="details_city"
                )
            with col2:
                self.form_data.customer_data['state'] = st.text_input(
                    "State",
                    value=self.form_data.customer_data['state'],
                    key="details_state"
                )

            self.form_data.customer_data['zip_code'] = st.text_input(
                "ZIP Code",
                value=self.form_data.customer_data['zip_code'],
                key="details_zip"
            )

    def save_account_and_get_id(self, account_data: Dict[str, Any]) -> Optional[int]:
        """Save or update account details and return the account ID."""
        try:
            # Validate account data
            if not account_data.get('business_name'):
                st.error("Business name is required")
                return None
            if not account_data.get('contact_person'):
                st.error("Contact person is required")
                return None
            if not validate_phone(account_data.get('phone_number', '')):
                st.error("Valid phone number is required")
                return None
            if account_data.get('email_address') and not validate_email(account_data['email_address']):
                st.error("Invalid email format")
                return None

            # Prepare account data for database
            account_update = {
                'ACCOUNT_NAME': account_data['business_name'],
                'CONTACT_PERSON': account_data['contact_person'],
                'CONTACT_EMAIL': account_data['email_address'],
                'CONTACT_PHONE': account_data['phone_number'],
                'BILLING_ADDRESS': account_data['street_address'],
                'CITY': account_data['city'],
                'STATE': account_data['state'],
                'ZIP_CODE': account_data['zip_code'],
                'MODIFIED_DATE': datetime.now(),
                'MODIFIED_BY': st.session_state.get('user_id', 'SYSTEM')
            }

            # Update or insert account record
            if account_data.get('account_id'):
                # Update existing account
                query = """
                UPDATE OPERATIONAL.CARPET.ACCOUNTS
                SET 
                    ACCOUNT_NAME = ?,
                    CONTACT_PERSON = ?,
                    CONTACT_EMAIL = ?,
                    CONTACT_PHONE = ?,
                    BILLING_ADDRESS = ?,
                    CITY = ?,
                    STATE = ?,
                    ZIP_CODE = ?,
                    MODIFIED_DATE = ?,
                    MODIFIED_BY = ?
                WHERE ACCOUNT_ID = ?
                """
                params = [
                    *account_update.values(),
                    account_data['account_id']
                ]
                snowflake_conn.execute_query(query, params)
                return account_data['account_id']
            else:
                # Insert new account
                query = """
                INSERT INTO OPERATIONAL.CARPET.ACCOUNTS (
                    ACCOUNT_NAME, CONTACT_PERSON, CONTACT_EMAIL, CONTACT_PHONE,
                    BILLING_ADDRESS, CITY, STATE, ZIP_CODE,
                    CREATED_DATE, CREATED_BY, MODIFIED_DATE, MODIFIED_BY,
                    ACTIVE_FLAG
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE)
                RETURNING ACCOUNT_ID
                """
                params = [
                    *account_update.values(),
                    datetime.now(),
                    st.session_state.get('user_id', 'SYSTEM'),
                    datetime.now(),
                    st.session_state.get('user_id', 'SYSTEM')
                ]
                result = snowflake_conn.execute_query(query, params)
                return result[0]['ACCOUNT_ID'] if result else None

        except Exception as e:
            st.error(f"Error saving account: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.error(f"Debug - Error details: {str(e)}")
            return None

    def display_customer_selector(self, matching_customers: List[str], customers_df: pd.DataFrame) -> None:
        """Display customer selection dropdown and populate form with selected customer data"""
        selected_customer = st.selectbox(
            "Select Customer",
            options=["Select..."] + matching_customers,
            key="customer_select"
        )

        if selected_customer and selected_customer != "Select...":
            customer_details = customers_df[
                (customers_df['FIRST_NAME'] + " " + customers_df['LAST_NAME']) == selected_customer
            ].iloc[0]

            # Update form data with selected customer details
            self.form_data.customer_data.update({
                'customer_id': int(customer_details['CUSTOMER_ID']),
                'first_name': customer_details.get('FIRST_NAME', ''),
                'last_name': customer_details.get('LAST_NAME', ''),
                'phone_number': customer_details.get('PHONE_NUMBER', ''),
                'email_address': customer_details.get('EMAIL_ADDRESS', ''),
                'street_address': customer_details.get('STREET_ADDRESS', ''),
                'city': customer_details.get('CITY', ''),
                'state': customer_details.get('STATE', ''),
                'zip_code': customer_details.get('ZIP_CODE', ''),
                'primary_contact_method': customer_details.get('PRIMARY_CONTACT_METHOD', 'Phone'),
                'text_flag': customer_details.get('TEXT_FLAG', False),
                'is_commercial': False
            })

            # Display the customer details
            self.display_customer_details(customer_details)

    def display_account_form(self) -> None:
        """Display form for entering or editing business account details"""
        with st.container():
            # Contact Information
            st.markdown("#### Business Information")
            col1, col2 = st.columns(2)
            with col1:
                self.form_data.customer_data['business_name'] = st.text_input(
                    "Business Name",
                    value=self.form_data.customer_data.get('business_name', ''),
                    key="business_name"
                )
                self.form_data.customer_data['contact_person'] = st.text_input(
                    "Contact Person",
                    value=self.form_data.customer_data.get('contact_person', ''),
                    key="contact_person"
                )

            with col2:
                self.form_data.customer_data['phone_number'] = st.text_input(
                    "Phone Number",
                    value=self.form_data.customer_data.get('phone_number', ''),
                    key="business_phone"
                )
                self.form_data.customer_data['email_address'] = st.text_input(
                    "Email",
                    value=self.form_data.customer_data.get('email_address', ''),
                    key="business_email"
                )

            # Billing Address Information
            st.markdown("#### Billing Address")
            self.form_data.customer_data['street_address'] = st.text_input(
                "Street Address",
                value=self.form_data.customer_data.get('street_address', ''),
                key="billing_street"
            )
            col1, col2 = st.columns(2)
            with col1:
                self.form_data.customer_data['city'] = st.text_input(
                    "City",
                    value=self.form_data.customer_data.get('city', ''),
                    key="billing_city"
                )
                self.form_data.customer_data['state'] = st.text_input(
                    "State",
                    value=self.form_data.customer_data.get('state', ''),
                    key="billing_state"
                )
            with col2:
                self.form_data.customer_data['zip_code'] = st.text_input(
                    "ZIP Code",
                    value=self.form_data.customer_data.get('zip_code', ''),
                    key="billing_zip"
                )

            # Additional Business Information
            st.markdown("#### Additional Information")
            self.form_data.customer_data['comments'] = st.text_area(
                "Comments/Notes",
                value=self.form_data.customer_data.get('comments', ''),
                key="business_comments"
            )

    def display_customer_form(self) -> None:
        """Display form for entering or editing customer details"""
        with st.container():
            # Contact Information
            st.markdown("#### Contact Details")
            col1, col2 = st.columns(2)
            with col1:
                self.form_data.customer_data['first_name'] = st.text_input(
                    "First Name",
                    value=self.form_data.customer_data.get('first_name', ''),
                    key="first_name"
                )
                self.form_data.customer_data['phone_number'] = st.text_input(
                    "Phone Number",
                    value=self.form_data.customer_data.get('phone_number', ''),
                    key="phone_number"
                )

            with col2:
                self.form_data.customer_data['last_name'] = st.text_input(
                    "Last Name",
                    value=self.form_data.customer_data.get('last_name', ''),
                    key="last_name"
                )
                self.form_data.customer_data['email_address'] = st.text_input(
                    "Email",
                    value=self.form_data.customer_data.get('email_address', ''),
                    key="email_address"
                )

            # Primary Address Information
            st.markdown("#### Primary Address")
            col1, col2 = st.columns(2)
            with col1:
                self.form_data.customer_data['street_address'] = st.text_input(
                    "Street Address",
                    value=self.form_data.customer_data.get('street_address', ''),
                    key="street_address"
                )
                self.form_data.customer_data['city'] = st.text_input(
                    "City",
                    value=self.form_data.customer_data.get('city', ''),
                    key="city"
                )
            with col2:
                self.form_data.customer_data['state'] = st.text_input(
                    "State",
                    value=self.form_data.customer_data.get('state', ''),
                    key="state"
                )
                self.form_data.customer_data['zip_code'] = st.text_input(
                    "ZIP Code",
                    value=self.form_data.customer_data.get('zip_code', ''),
                    key="zip_code"
                )
            
    def display_service_address_form(self) -> None:
        """Display form for entering the service address details."""
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
        self.form_data.customer_data['service_addr_sq_ft'] = st.number_input(
            "Square Footage",
            min_value=0,
            step=100,
            value=self.form_data.customer_data.get("service_addr_sq_ft", 0),
            key="service_sq_ft"
        )

    def display_account_details(self, account: Dict[str, Any]) -> None:
        """Display editable commercial account details"""
        # Update form data with selected account details
        self.form_data.customer_data.update({
            'account_id': account['ACCOUNT_ID'],
            'business_name': account.get('ACCOUNT_NAME', ''),
            'contact_person': account.get('CONTACT_PERSON', ''),
            'email_address': account.get('CONTACT_EMAIL', ''),
            'phone_number': account.get('CONTACT_PHONE', ''),
            'street_address': account.get('BILLING_ADDRESS', ''),
            'city': account.get('CITY', ''),
            'state': account.get('STATE', ''),
            'zip_code': str(account.get('ZIP_CODE', '')),
            'is_commercial': True
        })

        # Display editable account information
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
                key="edit_phone"
            )
            self.form_data.customer_data['email_address'] = st.text_input(
                "Email",
                value=account.get('CONTACT_EMAIL', ''),
                key="edit_email"
            )

        # Primary/Billing Address
        st.markdown("### Billing Address")
        self.form_data.customer_data['street_address'] = st.text_input(
            "Street Address",
            value=account.get('BILLING_ADDRESS', ''),
            key="edit_billing_street"
        )
        col1, col2 = st.columns(2)
        with col1:
            self.form_data.customer_data['city'] = st.text_input(
                "City",
                value=account.get('CITY', ''),
                key="edit_billing_city"
            )
            self.form_data.customer_data['state'] = st.text_input(
                "State",
                value=account.get('STATE', ''),
                key="edit_billing_state"
            )
        with col2:
            self.form_data.customer_data['zip_code'] = st.text_input(
                "ZIP Code",
                value=str(account.get('ZIP_CODE', '')),
                key="edit_billing_zip"
            )

    def display_availability_selector(self, service_date: date, service_duration: int = 60) -> Optional[time]:
        """Display time slot selector with 30-minute intervals."""
        available_slots = get_available_time_slots(service_date, service_duration)
        
        if not available_slots:
            st.warning("No available time slots for the selected date")
            return None

        formatted_slots = [slot.strftime("%I:%M %p") for slot in available_slots]
        formatted_slots.insert(0, "Select time...")

        selected_time_str = st.selectbox(
            "Service Time",
            options=formatted_slots,
            help="Available time slots are shown in 30-minute intervals"
        )

        if selected_time_str != "Select time...":
            return datetime.strptime(selected_time_str, "%I:%M %p").time()
        return None

    def validate_customer_data(self) -> List[str]:
        """Validate all customer input fields"""
        errors = []
        data = self.form_data.customer_data

        if not data['is_commercial']:
            # Residential customer validation
            if not data['phone_number'] or not validate_phone(data['phone_number']):
                errors.append("Valid phone number is required")
            
            if data['email_address'] and not validate_email(data['email_address']):
                errors.append("Invalid email format")

            if not data['street_address']:
                errors.append("Street address is required")

            try:
                if data['zip_code']:
                    zip_int = int(data['zip_code'])
                    if len(str(zip_int)) != 5:
                        errors.append("ZIP code must be exactly 5 digits")
                else:
                    errors.append("ZIP code is required")
            except ValueError:
                errors.append("ZIP code must be a valid 5-digit number")

        else:
            # Commercial account validation
            if not data['business_name']:
                errors.append("Business name is required")
            if not data['contact_person']:
                errors.append("Contact person is required")
            if not data['phone_number'] or not validate_phone(data['phone_number']):
                errors.append("Valid phone number is required")

        # Service address validation
        if not self.form_data.service_selection['same_as_primary']:
            if not data['service_address']:
                errors.append("Service street address is required")
            try:
                if data['service_zip']:
                    service_zip_int = int(data['service_zip'])
                    if len(str(service_zip_int)) != 5:
                        errors.append("Service ZIP code must be exactly 5 digits")
                else:
                    errors.append("Service ZIP code is required")
            except ValueError:
                errors.append("Service ZIP code must be a valid 5-digit number")

        return errors

    # In ServiceScheduler class (new_service.py)

    def save_service(self) -> bool:
        """Save complete service booking and send confirmation email."""
        try:
            # Validate customer data first
            validation_errors = self.validate_customer_data()
            if validation_errors:
                for error in validation_errors:
                    st.error(error)
                return False

            # For new commercial accounts, redirect to account creation
            if self.form_data.customer_data['is_commercial'] and not self.form_data.customer_data.get('account_id'):
                st.session_state['new_account'] = self.form_data.customer_data
                st.session_state['return_page'] = 'new_service'
                st.session_state['page'] = 'accounts'
                st.rerun()

            # Save or update customer/account
            customer_id = self.save_customer_and_get_id(self.form_data.customer_data)
            if not customer_id:
                return False

            # Calculate total cost for the selected services
            services_df = fetch_services()
            total_cost = sum(
                float(services_df[services_df['SERVICE_NAME'] == service]['COST'].iloc[0])
                for service in self.form_data.service_selection['selected_services']
            )

            # Prepare service scheduling data
            service_data = {
                'customer_id': int(customer_id) if not self.form_data.customer_data['is_commercial'] else None,
                'account_id': int(self.form_data.customer_data['account_id']) if self.form_data.customer_data.get('account_id') else None,
                'service_name': self.form_data.service_selection['selected_services'][0],  # Primary service
                'service_date': self.form_data.service_schedule['date'],
                'service_time': self.form_data.service_schedule['time'],
                'deposit': float(self.form_data.service_selection['deposit_amount']),
                'notes': str(self.form_data.service_selection['notes']) if self.form_data.service_selection.get('notes') else None,
                'is_recurring': bool(self.form_data.service_selection['is_recurring']),
                'recurrence_pattern': str(self.form_data.service_selection['recurrence_pattern']) if self.form_data.service_selection['is_recurring'] else None
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
                recurrence_pattern=service_data['recurrence_pattern']
            )

            if not service_scheduled:
                st.error("Failed to schedule service")
                return False

            # Store success message in session state to display after rerun
            success_message = [
                "Service scheduled successfully!",
                f"Deposit Amount: {format_currency(service_data['deposit'])}",
                f"Remaining Balance: {format_currency(total_cost - service_data['deposit'])}"
            ]
            
            if service_data['is_recurring']:
                success_message.append(f"Recurring: {service_data['recurrence_pattern']}")

            # Attempt to send confirmation email if email address is available
            if self.form_data.customer_data.get('email_address'):
                service_details = {
                    'customer_name': (
                        f"{self.form_data.customer_data.get('business_name', '')}" if self.form_data.customer_data['is_commercial']
                        else f"{self.form_data.customer_data.get('first_name', '')} {self.form_data.customer_data.get('last_name', '')}"
                    ).strip(),
                    'customer_email': self.form_data.customer_data['email_address'],
                    'service_type': ', '.join(self.form_data.service_selection['selected_services']),
                    'date': self.form_data.service_schedule['date'].strftime('%Y-%m-%d'),
                    'time': self.form_data.service_schedule['time'].strftime('%I:%M %p'),
                    'deposit_required': float(self.form_data.service_selection['deposit_amount']) > 0,
                    'deposit_amount': float(self.form_data.service_selection['deposit_amount']),
                    'deposit_paid': False,  # Update based on your deposit tracking
                    'notes': self.form_data.service_selection.get('notes', ''),
                    'total_cost': total_cost
                }
                
                # Get business info for email
                business_info = fetch_business_info()
                if not business_info:
                    success_message.append("Note: Unable to send confirmation email - missing business info")
                else:
                    # Send confirmation email using the email utility
                    email_result = generate_service_scheduled_email(service_details, business_info)
                    if email_result and email_result.success:
                        success_message.append("Confirmation email sent!")
                    else:
                        error_msg = email_result.message if email_result else "Unknown error"
                        success_message.append(f"Note: Unable to send confirmation email - {error_msg}")
            
            # Store success message and notification state
            st.session_state['success_message'] = '\n'.join(success_message)
            st.session_state['show_notification'] = True
            st.session_state['page'] = 'scheduled_services'
            
            # Trigger rerun to navigate to scheduled services page
            st.rerun()

        except Exception as e:
            st.error(f"Error saving service: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.error(f"Debug - Error details: {str(e)}")
            return False

    def display_service_selection(self) -> bool:
        """Display service selection and pricing section"""
        services_df = fetch_services()
        if services_df.empty:
            st.error("No services available")
            return False

        try:
            # Initialize service costs in session state if not present
            if 'service_costs' not in st.session_state:
                st.session_state.service_costs = {}

            # Create callback for service selection
            def update_selected_services():
                selected = st.session_state.get('services_select', [])
                st.session_state.selected_services = selected
                self.form_data.service_selection['selected_services'] = selected
                
                # Update service costs
                st.session_state.service_costs = {
                    service: float(services_df.loc[services_df['SERVICE_NAME'] == service, 'COST'].iloc[0])
                    for service in selected
                }

            # Multi-select for services with session state
            selected_services = st.multiselect(
                "Select Services",
                options=services_df['SERVICE_NAME'].tolist(),
                default=st.session_state.selected_services,
                key="services_select",
                on_change=update_selected_services,
                help="You can select multiple services"
            )
            
            if selected_services:
                # Calculate total cost
                total_cost = sum(st.session_state.service_costs.values())
                st.write(f"Total Cost: ${total_cost:.2f}")

                # Recurring Service Options
                is_recurring = False
                recurrence_pattern = None
                
                col1, col2 = st.columns(2)
                with col1:
                    is_recurring = st.checkbox(
                        "Recurring Service",
                        value=st.session_state.is_recurring,
                        key="recurring_checkbox"
                    )
                    st.session_state.is_recurring = is_recurring

                if is_recurring:
                    with col2:
                        recurrence_pattern = st.selectbox(
                            "Recurrence Pattern",
                            ["Weekly", "Bi-Weekly", "Monthly"],
                            index=["Weekly", "Bi-Weekly", "Monthly"].index(
                                st.session_state.recurrence_pattern
                            ) if st.session_state.recurrence_pattern else 0,
                            key="recurrence_pattern"
                        )
                        st.session_state.recurrence_pattern = recurrence_pattern

                # Deposit Section
                st.markdown("### Deposit Information")

                # Total cost calculation should exist before this
                total_cost = sum(st.session_state.service_costs.values()) if 'service_costs' in st.session_state else 0.0

                # Ensure deposit_amount is tied to session state and updated safely
                col1, col2 = st.columns(2)
                with col1:
                    deposit_amount = st.number_input(
                        "Deposit Amount",
                        min_value=0.0,
                        max_value=total_cost,
                        value=st.session_state.get("deposit_amount", 0.0),
                        step=5.0,
                        key="deposit_amount"
                    )

                with col2:
                    remaining_balance = total_cost - st.session_state.get("deposit_amount", 0.0)
                    st.write(f"Remaining Balance: ${remaining_balance:.2f}")

                # Notes Section
                st.markdown("### Service Notes")

                # Define the text area widget and directly bind it to st.session_state.service_notes
                st.markdown("### Service Notes")

                notes = st.text_area(
                    "Additional Instructions or Requirements",
                    value=st.session_state.get("service_notes", ""),
                    key="service_notes"  # Bind directly to st.session_state
                )

                # Use the notes variable for further processing if required
                if notes:
                    st.write(f"Processed Notes: {notes.upper()}")

                # Update form data with all variables defined in this scope
                self.form_data.service_selection.update({
                    'selected_services': selected_services,
                    'is_recurring': is_recurring,
                    'recurrence_pattern': recurrence_pattern if is_recurring else None,
                    'deposit_amount': deposit_amount,
                    'notes': notes
                })

                return True

            return False

        except Exception as e:
            st.error(f"Error in service selection: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.error(f"Debug - Error details: {str(e)}")
            return False

    def process_service_scheduling(self) -> bool:
        """Handle service scheduling workflow"""
        if not st.session_state.selected_services:
            st.error("Please select at least one service")
            return False
            
        schedule_data = self.form_data.service_schedule
        
        # Initialize scheduling in session state if not present
        if 'service_date' not in st.session_state:
            st.session_state.service_date = datetime.now().date()
        if 'service_time' not in st.session_state:
            st.session_state.service_time = None

        col1, col2 = st.columns(2)
        
        # Get service date
        with col1:
            service_date = st.date_input(
                "Service Date",
                min_value=datetime.now().date(),
                value=st.session_state.service_date,
                key="service_date_input",
                on_change=lambda: setattr(st.session_state, 'service_date',
                                        st.session_state.service_date_input)
            )
            schedule_data['date'] = service_date

        # Get available times
        with col2:
            available_slots = get_available_time_slots(service_date, st.session_state.selected_services)
            
            if not available_slots:
                st.warning(f"No available time slots for {service_date.strftime('%Y-%m-%d')}.")
                return False

            # Format time slots for display
            formatted_slots = [slot.strftime("%I:%M %p") for slot in available_slots]
            
            # Time selection with session state
            selected_time_str = st.selectbox(
                "Select Time",
                options=formatted_slots,
                key="time_select_single",
                on_change=lambda: setattr(st.session_state, 'service_time',
                                        datetime.strptime(st.session_state.time_select_single, 
                                                        "%I:%M %p").time() if st.session_state.time_select_single else None)
            )

            if selected_time_str:
                # Convert selected time string to time object
                service_time = datetime.strptime(selected_time_str, "%I:%M %p").time()
                
                # Check availability with selected services
                available, message = check_service_availability(
                    service_date, 
                    service_time, 
                    st.session_state.selected_services
                )
                
                if available:
                    schedule_data['time'] = service_time
                    return True
                else:
                    st.error(message)
                    return False

        return False

    def handle_customer_search(self) -> None:
        """Process residential customer search and selection"""
        customer_name = st.text_input(
            "Search Customer",
            help="Enter customer name to search"
        )

        if customer_name:
            existing_customers_df = fetch_all_customers()
            if not existing_customers_df.empty:
                # Filter customers based on search term
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

                        # Update form data with selected customer details
                        self.form_data.customer_data.update({
                            'customer_id': int(customer_details['CUSTOMER_ID']),
                            'first_name': customer_details.get('FIRST_NAME', ''),
                            'last_name': customer_details.get('LAST_NAME', ''),
                            'phone_number': customer_details.get('PHONE_NUMBER', ''),
                            'email_address': customer_details.get('EMAIL_ADDRESS', ''),
                            'street_address': customer_details.get('STREET_ADDRESS', ''),
                            'city': customer_details.get('CITY', ''),
                            'state': customer_details.get('STATE', ''),
                            'zip_code': customer_details.get('ZIP_CODE', ''),
                            'primary_contact_method': customer_details.get('PRIMARY_CONTACT_METHOD', 'Phone'),
                            'text_flag': customer_details.get('TEXT_FLAG', False),
                            'is_commercial': False,
                            'service_address': customer_details.get('STREET_ADDRESS', ''),
                            'service_city': customer_details.get('CITY', ''),
                            'service_state': customer_details.get('STATE', ''),
                            'service_zip': customer_details.get('ZIP_CODE', '')
                        })

                        self.display_customer_details(customer_details)
                else:
                    st.info("No matching customers found. Please enter customer details below.")
                    # Reset form data to clear any previous customer data
                    self.form_data.customer_data.update({
                        'customer_id': None,
                        'first_name': customer_name.split(' ')[0] if ' ' in customer_name else customer_name,
                        'last_name': ' '.join(customer_name.split(' ')[1:]) if ' ' in customer_name else '',
                        'phone_number': '',
                        'email_address': '',
                        'street_address': '',
                        'city': '',
                        'state': '',
                        'zip_code': '',
                        'primary_contact_method': 'Phone',
                        'text_flag': False,
                        'is_commercial': False
                    })
                    self.display_customer_form()
            else:
                st.info("No customers found in system. Please enter new customer details.")
                self.form_data.customer_data.update({
                    'customer_id': None,
                    'is_commercial': False
                })
                self.display_customer_form()

    def save_customer_and_get_id(self, customer_data: Dict[str, Any]) -> Optional[int]:
        """Save or update customer details and return the customer ID."""
        try:
            # Handle service address if it's different from the primary address
            if not customer_data.get("same_as_primary"):
                customer_data["service_address"] = customer_data["service_address"]
                customer_data["service_address_2"] = customer_data.get("service_address_2")
                customer_data["service_address_3"] = customer_data.get("service_address_3")
                customer_data["service_city"] = customer_data["service_city"]
                customer_data["service_state"] = customer_data["service_state"]
                customer_data["service_zip"] = customer_data["service_zip"]
                customer_data["service_addr_sq_ft"] = customer_data.get("service_addr_sq_ft")
            else:
                # Copy primary address fields to service address if "same as primary" is selected
                customer_data["service_address"] = customer_data["street_address"]
                customer_data["service_address_2"] = None
                customer_data["service_address_3"] = None
                customer_data["service_city"] = customer_data["city"]
                customer_data["service_state"] = customer_data["state"]
                customer_data["service_zip"] = customer_data["zip_code"]
                customer_data["service_addr_sq_ft"] = None

            # Save or update customer in the database
            if customer_data.get('customer_id'):
                # Update existing customer
                customer_id = save_customer(customer_data, int(customer_data['customer_id']))
                if not customer_id:
                    st.error("Failed to update customer information")
                    return None
                return int(customer_id)  # Ensure we return an integer
            else:
                # Create a new customer
                customer_id = save_customer(customer_data)
                if customer_id:
                    return int(customer_id)  # Ensure we return an integer
                else:
                    st.error("Failed to create new customer")
                    return None

        except Exception as e:
            st.error(f"Error saving customer: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.error(f"Debug - Error details: {str(e)}")
            return None


    def handle_account_search(self) -> None:
        """Process commercial account search and selection"""
        try:
            account_search = st.text_input(
                "Search Business Account",
                help="Enter business name to search"
            )

            if account_search:
                # Sanitize the search input
                search_term = account_search.strip().lower()
                
                # Query with proper error handling
                try:
                    query = """
                    SELECT 
                        ACCOUNT_ID,
                        ACCOUNT_NAME,
                        CONTACT_PERSON,
                        CONTACT_EMAIL,
                        CONTACT_PHONE,
                        BILLING_ADDRESS,
                        CITY,
                        STATE,
                        ZIP_CODE
                    FROM OPERATIONAL.CARPET.ACCOUNTS
                    WHERE ACTIVE_FLAG = TRUE
                    AND LOWER(ACCOUNT_NAME) LIKE ?
                    ORDER BY ACCOUNT_NAME
                    """
                    
                    accounts = snowflake_conn.execute_query(
                        query, 
                        [f"%{search_term}%"]
                    )

                    if accounts:
                        account_options = {
                            account['ACCOUNT_NAME']: account 
                            for account in accounts
                        }
                        
                        selected_account = st.selectbox(
                            "Select Business",
                            options=["Select..."] + list(account_options.keys()),
                            key="business_select"
                        )

                        if selected_account != "Select...":
                            account = account_options[selected_account]
                            self.display_account_details(account)
                    else:
                        st.info("No matching accounts found")
                        
                        # Option to create new account
                        if st.button("Create New Account"):
                            self.form_data.customer_data.update({
                                'business_name': account_search,
                                'is_commercial': True
                            })
                            self.display_account_form()

                except Exception as e:
                    st.error("Error searching business accounts")
                    if st.session_state.get('debug_mode'):
                        st.error(f"Debug - Database error: {str(e)}")

        except Exception as e:
            st.error("An error occurred while processing the form")
            if st.session_state.get('debug_mode'):
                st.error(f"Debug - Form error: {str(e)}")

def new_service_page():
    """Main service scheduling page with improved UI organization"""
    try:
        # Initialize session state first
        initialize_session_state()

        st.markdown("""
            <div style='text-align: center; padding: 1rem'>
                <h2>Schedule New Service</h2>
            </div>
        """, unsafe_allow_html=True)

        # Debug mode toggle (only show in development)
        if st.secrets.get("environment") == "development":
            st.sidebar.checkbox("Debug Mode", key="debug_mode")

        # Initialize scheduler with proper error handling
        try:
            if 'scheduler' not in st.session_state:
                st.session_state.scheduler = ServiceScheduler()
                debug_print("Initialized new scheduler")
            scheduler = st.session_state.scheduler
        except Exception as e:
            st.error("Error initializing service scheduler")
            debug_print(f"Scheduler initialization error: {str(e)}")
            return

        # Customer Type Selection
        st.markdown("### Select Customer Type")
        customer_type = st.radio(
            "Service For",
            ["Residential", "Commercial"],
            horizontal=True,
            key="customer_type"
        )

        try:
            if customer_type == "Commercial":
                # Handle Commercial Customer
                st.markdown("### Business Account")
                scheduler.handle_account_search()
            else:
                # Handle Residential Customer
                try:
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
                        new_customer = st.checkbox("New Customer", key="new_customer")

                    if customer_name and not new_customer:
                        try:
                            all_customers_df = fetch_all_customers()
                            if st.session_state.get('debug_mode'):
                                st.write("Debug - Fetched customers:", len(all_customers_df))
                            
                            search_term = customer_name.lower()
                            matching_customers_df = all_customers_df[
                                all_customers_df['FULL_NAME'].str.lower().str.contains(search_term, na=False) |
                                all_customers_df['PHONE_NUMBER'].str.lower().str.contains(search_term, na=False) |
                                all_customers_df['EMAIL_ADDRESS'].str.lower().str.contains(search_term, na=False)
                            ]
                            
                            if not matching_customers_df.empty:
                                selected_option = st.selectbox(
                                    "Select Customer",
                                    options=["Select..."] + matching_customers_df['FULL_NAME'].tolist(),
                                    key="customer_select"
                                )

                                if selected_option != "Select...":
                                    try:
                                        customer_details = matching_customers_df[
                                            matching_customers_df['FULL_NAME'] == selected_option
                                        ].iloc[0]

                                        # Display editable customer details
                                        st.markdown("### Customer Details")
                                        col1, col2 = st.columns(2)
                                        with col1:
                                            scheduler.form_data.customer_data['first_name'] = st.text_input(
                                                "First Name",
                                                value=customer_details['FIRST_NAME'],
                                                key="edit_first_name"
                                            )
                                            scheduler.form_data.customer_data['phone_number'] = st.text_input(
                                                "Phone",
                                                value=customer_details['PHONE_NUMBER'],
                                                key="edit_phone"
                                            )
                                        with col2:
                                            scheduler.form_data.customer_data['last_name'] = st.text_input(
                                                "Last Name",
                                                value=customer_details['LAST_NAME'],
                                                key="edit_last_name"
                                            )
                                            scheduler.form_data.customer_data['email_address'] = st.text_input(
                                                "Email",
                                                value=customer_details['EMAIL_ADDRESS'],
                                                key="edit_email"
                                            )

                                        # Primary Address
                                        st.markdown("### Primary Address")
                                        scheduler.form_data.customer_data['street_address'] = st.text_input(
                                            "Street Address",
                                            value=customer_details['STREET_ADDRESS'],
                                            key="edit_street"
                                        )
                                        col1, col2 = st.columns(2)
                                        with col1:
                                            scheduler.form_data.customer_data['city'] = st.text_input(
                                                "City",
                                                value=customer_details['CITY'],
                                                key="edit_city"
                                            )
                                            scheduler.form_data.customer_data['state'] = st.text_input(
                                                "State",
                                                value=customer_details['STATE'],
                                                key="edit_state"
                                            )
                                        with col2:
                                            scheduler.form_data.customer_data['zip_code'] = st.text_input(
                                                "ZIP Code",
                                                value=customer_details['ZIP_CODE'],
                                                key="edit_zip"
                                            )

                                        # Update customer ID
                                        scheduler.form_data.customer_data['customer_id'] = customer_details['CUSTOMER_ID']
                                        scheduler.form_data.customer_data['is_commercial'] = False
                                    except Exception as e:
                                        st.error(f"Error displaying customer details: {str(e)}")
                                        if st.session_state.get('debug_mode'):
                                            st.exception(e)
                            else:
                                st.info("No matching customers found. Please enter customer details below.")
                                scheduler.display_customer_form()
                        except Exception as e:
                            st.error(f"Error searching customers: {str(e)}")
                            if st.session_state.get('debug_mode'):
                                st.exception(e)
                    else:
                        scheduler.display_customer_form()
                except Exception as e:
                    st.error(f"Error in residential customer section: {str(e)}")
                    if st.session_state.get('debug_mode'):
                        st.exception(e)

                # Service Address section for residential customers
                if scheduler.form_data.customer_data.get('first_name') or scheduler.form_data.customer_data.get('customer_id'):
                    try:
                        st.markdown("### Service Address")
                        same_as_primary = st.checkbox("Same as Primary Address", value=True, key="same_as_primary")
                        
                        if same_as_primary:
                            scheduler.form_data.customer_data.update({
                                'service_address': scheduler.form_data.customer_data.get('street_address', ''),
                                'service_city': scheduler.form_data.customer_data.get('city', ''),
                                'service_state': scheduler.form_data.customer_data.get('state', ''),
                                'service_zip': scheduler.form_data.customer_data.get('zip_code', '')
                            })
                            scheduler.form_data.service_selection['same_as_primary'] = True
                        else:
                            service_col1, service_col2 = st.columns(2)
                            with service_col1:
                                scheduler.form_data.customer_data['service_address'] = st.text_input(
                                    "Service Street Address",
                                    value=scheduler.form_data.customer_data.get('service_address', ''),
                                    key="service_address"
                                )
                                scheduler.form_data.customer_data['service_city'] = st.text_input(
                                    "Service City",
                                    value=scheduler.form_data.customer_data.get('service_city', ''),
                                    key="service_city"
                                )
                            with service_col2:
                                scheduler.form_data.customer_data['service_state'] = st.text_input(
                                    "Service State",
                                    value=scheduler.form_data.customer_data.get('service_state', ''),
                                    key="service_state"
                                )
                                scheduler.form_data.customer_data['service_zip'] = st.text_input(
                                    "Service ZIP",
                                    value=scheduler.form_data.customer_data.get('service_zip', ''),
                                    key="service_zip"
                                )
                            scheduler.form_data.customer_data['service_addr_sq_ft'] = st.number_input(
                                "Square Footage",
                                min_value=0,
                                step=100,
                                value=scheduler.form_data.customer_data.get('service_addr_sq_ft', 0),
                                key="service_sq_ft"
                            )
                            scheduler.form_data.service_selection['same_as_primary'] = False

                    except Exception as e:
                        st.error(f"Error in service address section: {str(e)}")
                        if st.session_state.get('debug_mode'):
                            st.exception(e)

            # Service Selection Section
            show_service_section = (
                scheduler.form_data.customer_data.get("account_id") is not None or
                bool(scheduler.form_data.customer_data.get("first_name"))
            )
            
            if show_service_section:
                st.markdown("### Select Services")
                services_selected = scheduler.display_service_selection()

                if services_selected:
                    # Schedule Section
                    st.markdown("### Schedule Service")
                    schedule_selected = scheduler.process_service_scheduling()

                    if schedule_selected:
                        # Final Buttons
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("📅 Schedule Service", type="primary"):
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
                            if st.button("❌ Cancel", type="secondary"):
                                reset_session_state()
                                st.rerun()

        except Exception as e:
            st.error("An error occurred while processing the form")
            debug_print(f"Form processing error: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)

    except Exception as e:
        st.error("An unexpected error occurred")
        debug_print(f"Page initialization error: {str(e)}")
        if st.session_state.get('debug_mode'):
            st.exception(e)

if __name__ == "__main__":
    new_service_page()
