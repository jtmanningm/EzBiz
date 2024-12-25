import streamlit as st
from datetime import datetime, date, time, timedelta
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
import pandas as pd

from models.customer import CustomerModel, fetch_all_customers, save_customer
from models.service import (
    ServiceModel,
    fetch_services,
    check_service_availability,
    save_service_schedule,
    get_available_time_slots
)
from database.connection import snowflake_conn
from utils.formatting import format_currency
from utils.validation import validate_phone, validate_email, validate_zip_code

@dataclass
class ServiceFormData:
    customer_data: Dict[str, Any]
    service_selection: Dict[str, Any]
    service_schedule: Dict[str, Any]

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
                'selected_services': [],
                'is_recurring': False,
                'recurrence_pattern': None,
                'deposit_amount': 0.0,
                'notes': '',
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

            # Service Address Section
            st.markdown("#### Service Location")
            self.form_data.service_selection['same_as_primary'] = st.checkbox(
                "Same as Primary Address",
                value=self.form_data.service_selection['same_as_primary'],
                key="details_same_address"
            )

            if not self.form_data.service_selection['same_as_primary']:
                self.display_service_address_form()
            else:
                # Copy primary address to service address
                self.form_data.customer_data.update({
                    'service_address': self.form_data.customer_data['street_address'],
                    'service_city': self.form_data.customer_data['city'],
                    'service_state': self.form_data.customer_data['state'],
                    'service_zip': self.form_data.customer_data['zip_code']
                })

    def handle_customer_search(self) -> None:
        """Process residential customer search and selection"""
        customer_name = st.text_input(
            "Search Customer",
            help="Enter customer name to search"
        )

        if customer_name:
            existing_customers_df = fetch_all_customers()
            if not existing_customers_df.empty:
                matching_customers = [
                    f"{row['FIRST_NAME']} {row['LAST_NAME']}"
                    for _, row in existing_customers_df.iterrows()
                    if customer_name.lower() in f"{row['FIRST_NAME']} {row['LAST_NAME']}".lower()
                ]

                if matching_customers:
                    self.display_customer_selector(matching_customers, existing_customers_df)
                else:
                    st.info("No matching customers found. Please enter customer details below.")
                    # Reset form data to clear any previous customer data
                    self.form_data.customer_data = ServiceFormData.initialize().customer_data
                    self.form_data.customer_data['is_commercial'] = False
                    # Split the search name into first and last name
                    names = customer_name.split(' ', 1)
                    self.form_data.customer_data['first_name'] = names[0] if names else ''
                    self.form_data.customer_data['last_name'] = names[1] if len(names) > 1 else ''
                    # Show the customer form for new customer entry
                    self.display_customer_form()
            else:
                st.info("No customers found. Please enter customer details below.")
                self.form_data.customer_data = ServiceFormData.initialize().customer_data
                self.form_data.customer_data['is_commercial'] = False
                self.display_customer_form()

    def handle_account_search(self) -> None:
        """Process commercial account search and selection"""
        account_search = st.text_input(
            "Search Business Account",
            help="Enter business name to search"
        )

        if account_search:
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
                [f"%{account_search.lower()}%"]
            )

            if accounts:
                account_options = {
                    account['ACCOUNT_NAME']: account 
                    for account in accounts
                }
                
                selected_account = st.selectbox(
                    "Select Business",
                    options=["Select..."] + list(account_options.keys())
                )

                if selected_account != "Select...":
                    account = account_options[selected_account]
                    self.display_account_details(account)
            else:
                st.info("No matching accounts found")


    def display_account_form(self) -> None:
        """Display form for entering or editing business account details"""
        with st.container():
            # Business Information
            st.markdown("#### Business Information")
            col1, col2 = st.columns(2)
            with col1:
                self.form_data.customer_data['business_name'] = st.text_input(
                    "Business Name",
                    value=self.form_data.customer_data['business_name'],
                    key="business_name"
                )
                self.form_data.customer_data['contact_person'] = st.text_input(
                    "Contact Person",
                    value=self.form_data.customer_data['contact_person'],
                    key="contact_person"
                )

            with col2:
                self.form_data.customer_data['phone_number'] = st.text_input(
                    "Phone Number",
                    value=self.form_data.customer_data['phone_number'],
                    key="business_phone"
                )
                self.form_data.customer_data['email_address'] = st.text_input(
                    "Email",
                    value=self.form_data.customer_data['email_address'],
                    key="business_email"
                )

            # Contact Preferences
            self.form_data.customer_data['primary_contact_method'] = st.selectbox(
                "Preferred Contact Method",
                ["Phone", "Email"],
                index=["Phone", "Email"].index(
                    self.form_data.customer_data['primary_contact_method']
                ) if self.form_data.customer_data['primary_contact_method'] in ["Phone", "Email"] else 0,
                key="business_contact_method"
            )

            # Address Information
            st.markdown("#### Billing Address")
            self.form_data.customer_data['street_address'] = st.text_input(
                "Street Address",
                value=self.form_data.customer_data['street_address'],
                key="business_street"
            )

            col1, col2 = st.columns(2)
            with col1:
                self.form_data.customer_data['city'] = st.text_input(
                    "City",
                    value=self.form_data.customer_data['city'],
                    key="business_city"
                )
            with col2:
                self.form_data.customer_data['state'] = st.text_input(
                    "State",
                    value=self.form_data.customer_data['state'],
                    key="business_state"
                )

            self.form_data.customer_data['zip_code'] = st.text_input(
                "ZIP Code",
                value=self.form_data.customer_data['zip_code'],
                key="business_zip"
            )

            # Service Address Section
            st.markdown("#### Service Location")
            self.form_data.service_selection['same_as_primary'] = st.checkbox(
                "Same as Billing Address",
                value=self.form_data.service_selection['same_as_primary'],
                key="business_same_address"
            )

            if not self.form_data.service_selection['same_as_primary']:
                self.display_service_address_form()
            else:
                # Copy billing address to service address
                self.form_data.customer_data.update({
                    'service_address': self.form_data.customer_data['street_address'],
                    'service_city': self.form_data.customer_data['city'],
                    'service_state': self.form_data.customer_data['state'],
                    'service_zip': self.form_data.customer_data['zip_code']
                })

    def display_account_details(self, account: Dict[str, Any]) -> None:
        """Display commercial account details"""
        # Update form data with selected account details
        self.form_data.customer_data.update({
            'account_id': account['ACCOUNT_ID'],
            'business_name': account['ACCOUNT_NAME'],
            'contact_person': account['CONTACT_PERSON'],
            'email_address': account['CONTACT_EMAIL'],
            'phone_number': account['CONTACT_PHONE'],
            'street_address': account['BILLING_ADDRESS'],
            'city': account['CITY'],
            'state': account['STATE'],
            'zip_code': str(account['ZIP_CODE']),
            'service_address': account['BILLING_ADDRESS'],
            'service_city': account['CITY'],
            'service_state': account['STATE'],
            'service_zip': str(account['ZIP_CODE']),
            'is_commercial': True
        })

        # Display account information
        st.markdown("#### Account Details")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Business:** {account['ACCOUNT_NAME']}")
            st.markdown(f"**Contact:** {account['CONTACT_PERSON']}")
        with col2:
            st.markdown(f"**Phone:** {account['CONTACT_PHONE']}")
            st.markdown(f"**Email:** {account['CONTACT_EMAIL']}")

        st.markdown("#### Service Location")
        same_location = st.checkbox(
            "Same as Billing Address",
            value=True,
            key="same_location"
        )

        if not same_location:
            self.display_service_address_form()

    def display_customer_form(self) -> None:
        """Display form for entering or editing customer details"""
        with st.container():
            # Contact Information
            st.markdown("#### Contact Details")
            col1, col2 = st.columns(2)
            with col1:
                self.form_data.customer_data['first_name'] = st.text_input(
                    "First Name",
                    value=self.form_data.customer_data['first_name'],
                    key="first_name"
                )
                self.form_data.customer_data['phone_number'] = st.text_input(
                    "Phone Number",
                    value=self.form_data.customer_data['phone_number'],
                    key="phone_number"
                )

            with col2:
                self.form_data.customer_data['last_name'] = st.text_input(
                    "Last Name",
                    value=self.form_data.customer_data['last_name'],
                    key="last_name"
                )
                self.form_data.customer_data['email_address'] = st.text_input(
                    "Email",
                    value=self.form_data.customer_data['email_address'],
                    key="email_address"
                )

           # Contact Preferences
            self.form_data.customer_data['primary_contact_method'] = st.selectbox(
                "Preferred Contact Method",
                ["Phone", "Text", "Email"],
                index=["Phone", "Text", "Email"].index(
                    self.form_data.customer_data['primary_contact_method']
                ),
                key="contact_method"
            )

            self.form_data.customer_data['text_flag'] = st.checkbox(
                "Opt-in to Text Messages",
                value=self.form_data.customer_data['text_flag'],
                key="text_flag"
            )

            # Address Information
            st.markdown("#### Address Information")
            self.form_data.customer_data['street_address'] = st.text_input(
                "Street Address",
                value=self.form_data.customer_data['street_address'],
                key="street_address"
            )

            col1, col2 = st.columns(2)
            with col1:
                self.form_data.customer_data['city'] = st.text_input(
                    "City",
                    value=self.form_data.customer_data['city'],
                    key="city"
                )
            with col2:
                self.form_data.customer_data['state'] = st.text_input(
                    "State",
                    value=self.form_data.customer_data['state'],
                    key="state"
                )

            self.form_data.customer_data['zip_code'] = st.text_input(
                "ZIP Code",
                value=self.form_data.customer_data['zip_code'],
                key="zip_code"
            )

            # Service Address Section
            st.markdown("#### Service Address")
            self.form_data.service_selection['same_as_primary'] = st.checkbox(
                "Same as Primary Address",
                value=self.form_data.service_selection['same_as_primary'],
                key="same_address_checkbox"
            )

            if not self.form_data.service_selection['same_as_primary']:
                self.display_service_address_form()
            else:
                # Copy primary address to service address
                self.form_data.customer_data.update({
                    'service_address': self.form_data.customer_data['street_address'],
                    'service_city': self.form_data.customer_data['city'],
                    'service_state': self.form_data.customer_data['state'],
                    'service_zip': self.form_data.customer_data['zip_code']
                })

    def display_service_address_form(self) -> None:
        """Display service address form fields"""
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

    def display_service_selection(self) -> bool:
        """Display service selection and pricing section"""
        services_df = fetch_services()
        if services_df.empty:
            st.error("No services available")
            return False

        try:
            selected_services = st.multiselect(
                "Select Services",
                options=services_df['SERVICE_NAME'].tolist(),
                default=self.form_data.service_selection['selected_services'],
                key="services_select"
            )
            self.form_data.service_selection['selected_services'] = selected_services

            if selected_services:
                # Calculate total cost
                total_cost = sum(
                    float(services_df.loc[services_df['SERVICE_NAME'] == service, 'COST'].iloc[0])
                    for service in selected_services
                )
                st.write(f"Total Cost: {format_currency(total_cost)}")

                # Recurring Service Options
                is_recurring = st.checkbox(
                    "Recurring Service",
                    value=self.form_data.service_selection['is_recurring'],
                    key="recurring_checkbox"
                )
                self.form_data.service_selection['is_recurring'] = is_recurring

                if is_recurring:
                    recurrence_pattern = st.selectbox(
                        "Recurrence Pattern",
                        ["Weekly", "Bi-Weekly", "Monthly"],
                        index=["Weekly", "Bi-Weekly", "Monthly"].index(
                            self.form_data.service_selection['recurrence_pattern']
                        ) if self.form_data.service_selection['recurrence_pattern'] else 0,
                        key="recurrence_pattern"
                    )
                    self.form_data.service_selection['recurrence_pattern'] = recurrence_pattern

                # Deposit Section
                deposit_amount = st.number_input(
                    "Deposit Amount",
                    min_value=0.0,
                    max_value=total_cost,
                    value=self.form_data.service_selection['deposit_amount'],
                    step=5.0,
                    key="deposit_amount"
                )
                self.form_data.service_selection['deposit_amount'] = deposit_amount
                st.write(f"Remaining Balance: {format_currency(total_cost - deposit_amount)}")

                # Notes Section
                notes = st.text_area(
                    "Service Notes",
                    value=self.form_data.service_selection['notes'],
                    key="service_notes"
                )
                self.form_data.service_selection['notes'] = notes

                return True
            return False

        except Exception as e:
            st.error(f"Error in service selection: {str(e)}")
            return False

    def process_service_scheduling(self) -> bool:
        """Handle service scheduling workflow"""
        schedule_data = self.form_data.service_schedule
        
        # Get service date
        service_date = st.date_input(
            "Service Date",
            min_value=datetime.now().date(),
            value=schedule_data['date'],
            key="service_date"
        )
        schedule_data['date'] = service_date

        # Get selected services for duration calculation
        selected_services = self.form_data.service_selection.get('selected_services', [])
        
        try:
            # Get available slots considering selected services duration
            available_slots = get_available_time_slots(service_date, selected_services)
            
            if not available_slots:
                st.warning("No available time slots for the selected date.")
                return False

            # Display time slots
            formatted_slots = [slot.strftime("%I:%M %p") for slot in available_slots]
            selected_time_str = st.selectbox(
                "Select Time",
                options=["Select..."] + formatted_slots,
                key="time_select"
            )

            if selected_time_str == "Select...":
                return False

            # Convert selected time string to time object
            service_time = datetime.strptime(selected_time_str, "%I:%M %p").time()
            
            # Check availability with selected services
            available, message = check_service_availability(service_date, service_time, selected_services)
            if not available:
                st.error(message)
                return False

            schedule_data['time'] = service_time
            return True

        except Exception as e:
            st.error(f"Error getting time slots: {str(e)}")
            return False

    def save_service(self) -> bool:
        """Save complete service booking"""
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
            customer_data = self.form_data.customer_data
            customer_id = save_customer(customer_data)
            
            if not customer_id:
                st.error("Failed to save customer information")
                return False

            # Save service schedule
            schedule_saved = save_service_schedule(
                customer_id=customer_id,
                services=self.form_data.service_selection['selected_services'],
                service_date=self.form_data.service_schedule['date'],
                service_time=self.form_data.service_schedule['time'],
                deposit_amount=self.form_data.service_selection['deposit_amount'],
                notes=self.form_data.service_selection['notes'],
                is_recurring=self.form_data.service_selection['is_recurring'],
                recurrence_pattern=self.form_data.service_selection['recurrence_pattern']
            )

            if not schedule_saved:
                st.error("Failed to save service schedule")
                return False

            return True

        except Exception as e:
            st.error(f"Error saving service: {str(e)}")
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
                matching_customers = [
                    f"{row['FIRST_NAME']} {row['LAST_NAME']}"
                    for _, row in existing_customers_df.iterrows()
                    if customer_name.lower() in f"{row['FIRST_NAME']} {row['LAST_NAME']}".lower()
                ]

                if matching_customers:
                    self.display_customer_selector(matching_customers, existing_customers_df)
                else:
                    st.info("No matching customers found")

def handle_account_search(self) -> None:
    """Process commercial account search and selection"""
    account_search = st.text_input(
        "Search Business Account",
        help="Enter business name to search"
    )

    if account_search:
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
            [f"%{account_search.lower()}%"]
        )

        if accounts:
            account_options = {
                account['ACCOUNT_NAME']: account 
                for account in accounts
            }
            
            selected_account = st.selectbox(
                "Select Business",
                options=["Select..."] + list(account_options.keys())
            )

            if selected_account != "Select...":
                account = account_options[selected_account]
                self.display_account_details(account)
        else:
            st.info("No matching accounts found")

def new_service_page():
    """Main service scheduling page"""
    st.markdown("""
        <div class="page-header">
            <h2>New Service</h2>
        </div>
    """, unsafe_allow_html=True)

    scheduler = ServiceScheduler()

    # Customer Type Selection
    st.markdown("### Select Customer Type")
    customer_type = st.radio(
        "Service For",
        ["Residential", "Commercial"],
        horizontal=True,
        key="customer_type"
    )

    if customer_type == "Residential":
        # Customer Information Section
        st.markdown("### Customer Information")
        scheduler.handle_customer_search()
    else:
        # Commercial Account Section
        st.markdown("### Business Account")
        scheduler.handle_account_search()

    # Service Selection Section
    show_service_section = False
    
    if customer_type == "Residential":
        # Show if we have an existing customer OR if we have valid new customer details
        has_customer_id = scheduler.form_data.customer_data.get('customer_id') is not None
        has_valid_details = (
            scheduler.form_data.customer_data.get('first_name') and
            scheduler.form_data.customer_data.get('last_name') and
            scheduler.form_data.customer_data.get('phone_number') and
            scheduler.form_data.customer_data.get('street_address')
        )
        show_service_section = has_customer_id or has_valid_details
    else:
        show_service_section = scheduler.form_data.customer_data.get('account_id') is not None

    if show_service_section:
        st.markdown("### Service Selection")
        if scheduler.display_service_selection():
            # Only show scheduling after services are selected
            st.markdown("### Service Schedule")
            if scheduler.process_service_scheduling():
                if st.button("Schedule Service", type="primary"):
                    if scheduler.save_service():
                        st.success("Service scheduled successfully!")
                        st.balloons()
                        # Reset form data for next booking
                        st.session_state.form_data = ServiceFormData.initialize()
                        st.session_state['page'] = 'scheduled_services'
                        st.rerun()

if __name__ == "__main__":
    new_service_page()