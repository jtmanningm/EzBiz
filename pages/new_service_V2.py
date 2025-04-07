# # new_service.py (Optimized for Mobile)
# import streamlit as st
# from datetime import datetime, date, time, timedelta
# from typing import Optional, Dict, Any, List, Tuple
# from dataclasses import dataclass
# import pandas as pd
# import json

# from utils.formatting import format_currency, add_back_navigation
# from models.transaction import save_transaction
# from models.customer import CustomerModel, fetch_all_customers, save_customer
# from models.service import (
#     ServiceModel,
#     schedule_recurring_services,
#     fetch_services,
#     check_service_availability,
#     save_service_schedule,
#     get_available_time_slots
# )
# from database.connection import snowflake_conn
# from utils.formatting import format_currency
# from utils.email import generate_service_scheduled_email, generate_service_completed_email
# from utils.validation import validate_phone, validate_email, validate_zip_code
# from pages.settings.business import fetch_business_info

# def debug_print(msg: str) -> None:
#     """Helper function for debug logging with defensive access to debug_mode."""
#     if st.session_state.get('debug_mode', False):  # Default to False if not set
#         print(f"DEBUG: {msg}")
#         st.write(f"DEBUG: {msg}")

# def initialize_session_state() -> None:
#     """Initialize required session state variables"""
#     if 'deposit_amount' not in st.session_state:
#         st.session_state.deposit_amount = 0.0
#     if 'debug_mode' not in st.session_state:
#         st.session_state['debug_mode'] = False
#     if 'selected_services' not in st.session_state:
#         st.session_state.selected_services = []
#     if 'service_costs' not in st.session_state:
#         st.session_state.service_costs = {}
#     if 'is_recurring' not in st.session_state:
#         st.session_state.is_recurring = False
#     if 'recurrence_pattern' not in st.session_state:
#         st.session_state.recurrence_pattern = None
#     if 'deposit_amount' not in st.session_state:
#         st.session_state.deposit_amount = 0.0
#     if 'service_notes' not in st.session_state:
#         st.session_state.service_notes = ''
#     if 'service_date' not in st.session_state:
#         st.session_state.service_date = datetime.now().date()
#     if 'service_time' not in st.session_state:
#         st.session_state.service_time = None
#     if 'selected_customer_id' not in st.session_state:
#         st.session_state.selected_customer_id = None

#     debug_print("Session state initialized")

# def reset_session_state() -> None:
#     """Reset all session state variables related to service scheduling"""
#     keys_to_clear = [
#         'selected_services', 
#         'is_recurring', 
#         'recurrence_pattern',
#         'deposit_amount', 
#         'service_notes', 
#         'service_date',
#         'service_time', 
#         'scheduler',
#         'service_costs',
#         'customer_type',
#         'form_data',
#         'selected_customer_id',  # Add this to clear customer selection
#         # Add new customer form fields
#         'new_customer_name',
#         'new_customer_phone',
#         'new_customer_email',
#         'new_customer_street',
#         'new_customer_city',
#         'new_customer_state',
#         'new_customer_zip',
#         'customer_select',  # Clear customer selection dropdown
#         'search_customer'  # Clear search field
#     ]
#     for key in keys_to_clear:
#         if key in st.session_state:
#             del st.session_state[key]
#     debug_print("Session state reset")
    
#     # Re-initialize session state but preserve debug_mode
#     initialize_session_state()

# @dataclass
# class ServiceFormData:
#     customer_data: Dict[str, Any]
#     service_selection: Dict[str, Any]
#     service_schedule: Dict[str, Any]

#     @classmethod
#     def initialize(cls) -> 'ServiceFormData':
#         """Initialize form data with default values"""
#         # Initialize service selection in session state if not present
#         if 'selected_services' not in st.session_state:
#             st.session_state.selected_services = []
#         if 'is_recurring' not in st.session_state:
#             st.session_state.is_recurring = False
#         if 'recurrence_pattern' not in st.session_state:
#             st.session_state.recurrence_pattern = None
#         if 'deposit_amount' not in st.session_state:
#             st.session_state.deposit_amount = 0.0
#         if 'service_notes' not in st.session_state:
#             st.session_state.service_notes = ''

#         return cls(
#             customer_data={
#                 'customer_id': None,
#                 'account_id': None,
#                 'business_name': '',
#                 'contact_person': '',
#                 'first_name': '',
#                 'last_name': '',
#                 'phone_number': '',
#                 'email_address': '',
#                 'street_address': '',
#                 'city': '',
#                 'state': '',
#                 'zip_code': '',
#                 'primary_contact_method': 'Phone',
#                 'text_flag': False,
#                 'service_address': '',
#                 'service_city': '',
#                 'service_state': '',
#                 'service_zip': '',
#                 'service_address_2': None,
#                 'service_address_3': None,
#                 'service_addr_sq_ft': None,
#                 'comments': '',
#                 'member_flag': False,
#                 'is_commercial': False
#             },
#             service_selection={
#                 'selected_services': st.session_state.selected_services,
#                 'is_recurring': st.session_state.is_recurring,
#                 'recurrence_pattern': st.session_state.recurrence_pattern,
#                 'deposit_amount': st.session_state.deposit_amount,
#                 'notes': st.session_state.service_notes,
#                 'same_as_primary': True
#             },
#             service_schedule={
#                 'date': datetime.now().date(),
#                 'time': None
#             }
#         )

# class ServiceScheduler:
#     def __init__(self):
#         if not hasattr(st.session_state, 'form_data'):
#             st.session_state.form_data = ServiceFormData.initialize()
#         self.form_data = st.session_state.form_data

#     def display_customer_form(self) -> None:
#         """Display customer form with consistent field order."""
        
#         # Initial customer type selection
#         st.markdown("### Select Service Type")
#         st.radio(
#             "Service For",
#             ["Residential", "Commercial"],
#             horizontal=True,
#             key="customer_type"
#         )

#         # Customer search section
#         st.markdown("### Customer Search")
#         with st.container():
#             st.text_input(
#                 "Search by Name, Phone, or Email",
#                 help="Enter customer name, phone number, or email to search",
#                 key="search_customer"
#             )
#             st.checkbox("New Customer", key="new_customer")

#             if st.session_state.get("search_customer") and not st.session_state.get("new_customer"):
#                 existing_customers_df = fetch_all_customers()
#                 if not existing_customers_df.empty:
#                     search_term = st.session_state.search_customer.lower()
#                     matching_customers = [
#                         f"{row['FIRST_NAME']} {row['LAST_NAME']}"
#                         for _, row in existing_customers_df.iterrows()
#                         if search_term in f"{row['FIRST_NAME']} {row['LAST_NAME']}".lower() or
#                         search_term in str(row['PHONE_NUMBER']).lower() or
#                         (pd.notnull(row['EMAIL_ADDRESS']) and search_term in str(row['EMAIL_ADDRESS']).lower())
#                     ]
                    
#                     if matching_customers:
#                         st.selectbox(
#                             "Select Customer",
#                             ["Select..."] + matching_customers,
#                             key="customer_select"
#                         )
#                     else:
#                         st.info("No matching customers found")

#         # Contact Details - Single column layout for consistent ordering
#         st.markdown("### Contact Details")
#         with st.container():
#             # Core contact info in logical order
#             self.form_data.customer_data['first_name'] = st.text_input(
#                 "First Name",
#                 value=self.form_data.customer_data.get('first_name', ''),
#                 key="first_name"
#             )
#             self.form_data.customer_data['last_name'] = st.text_input(
#                 "Last Name",
#                 value=self.form_data.customer_data.get('last_name', ''),
#                 key="last_name"
#             )
#             self.form_data.customer_data['phone_number'] = st.text_input(
#                 "Phone Number",
#                 value=self.form_data.customer_data.get('phone_number', ''),
#                 key="phone"
#             )
#             self.form_data.customer_data['email_address'] = st.text_input(
#                 "Email",
#                 value=self.form_data.customer_data.get('email_address', ''),
#                 key="email"
#             )

#             # Contact preferences
#             self.form_data.customer_data['primary_contact_method'] = st.selectbox(
#                 "Preferred Contact Method",
#                 ["Phone", "Text", "Email"],
#                 key="contact_method"
#             )
#             self.form_data.customer_data['text_flag'] = st.checkbox(
#                 "Opt-in to Text Messages",
#                 value=self.form_data.customer_data.get('text_flag', False),
#                 key="text_optin"
#             )

#         # Primary Address - Single column for consistent flow
#         st.markdown("### Primary Address")
#         with st.container():
#             self.form_data.customer_data['street_address'] = st.text_input(
#                 "Street Address",
#                 value=self.form_data.customer_data.get('street_address', ''),
#                 key="street"
#             )
#             self.form_data.customer_data['city'] = st.text_input(
#                 "City",
#                 value=self.form_data.customer_data.get('city', ''),
#                 key="city"
#             )
#             self.form_data.customer_data['state'] = st.text_input(
#                 "State",
#                 value=self.form_data.customer_data.get('state', ''),
#                 key="state"
#             )
#             self.form_data.customer_data['zip_code'] = st.text_input(
#                 "ZIP Code",
#                 value=self.form_data.customer_data.get('zip_code', ''),
#                 key="zip"
#             )

#         # Service Address - Single column with conditional display
#         st.markdown("### Service Address")
#         with st.container():
#             same_as_primary = st.checkbox(
#                 "Same as Primary Address",
#                 value=True,
#                 key="same_as_primary"
#             )

#             if not same_as_primary:
#                 self.form_data.customer_data['service_address'] = st.text_input(
#                     "Service Street Address",
#                     value=self.form_data.customer_data.get('service_address', ''),
#                     key="service_street"
#                 )
#                 self.form_data.customer_data['service_city'] = st.text_input(
#                     "Service City",
#                     value=self.form_data.customer_data.get('service_city', ''),
#                     key="service_city"
#                 )
#                 self.form_data.customer_data['service_state'] = st.text_input(
#                     "Service State",
#                     value=self.form_data.customer_data.get('service_state', ''),
#                     key="service_state"
#                 )
#                 self.form_data.customer_data['service_zip'] = st.text_input(
#                     "Service ZIP Code",
#                     value=self.form_data.customer_data.get('service_zip', ''),
#                     key="service_zip"
#                 )
#                 self.form_data.customer_data['service_addr_sq_ft'] = st.number_input(
#                     "Square Footage",
#                     min_value=0,
#                     step=100,
#                     value=self.form_data.customer_data.get('service_addr_sq_ft', 0),
#                     key="service_sq_ft"
#                 )

#     # ... (rest of the code remains the same)

# def new_service_page():
#     """Main service scheduling page with improved UI organization"""
#     try:
#         # Add back navigation at the very start
#         add_back_navigation()

#         # Initialize session state first
#         initialize_session_state()

#         st.markdown("""
#             <div style='text-align: center; padding: 1rem'>
#                 <h2>Schedule New Service</h2>
#             </div>
#         """, unsafe_allow_html=True)

#         # Debug mode toggle (only show in development)
#         if st.secrets.get("environment") == "development":
#             st.sidebar.checkbox("Debug Mode", key="debug_mode")

#         # Initialize scheduler with proper error handling
#         try:
#             if 'scheduler' not in st.session_state:
#                 st.session_state.scheduler = ServiceScheduler()
#                 debug_print("Initialized new scheduler")
#             scheduler = st.session_state.scheduler
#         except Exception as e:
#             st.error("Error initializing service scheduler")
#             debug_print(f"Scheduler initialization error: {str(e)}")
#             return

#         # Customer Type Selection
#         st.markdown("### Select Customer Type")
#         customer_type = st.radio(
#             "Service For",
#             ["Residential", "Commercial"],
#             horizontal=True,
#             key="customer_type"
#         )

#         try:
#             if customer_type == "Commercial":
#                 # Handle Commercial Customer
#                 st.markdown("### Business Account")
#                 scheduler.handle_account_search()
#             else:
#                 # Handle Residential Customer
#                 try:
#                     st.markdown("### Customer Information")
#                     search_col1, search_col2 = st.columns([2, 1])
#                     with search_col1:
#                         customer_name = st.text_input(
#                             "Search by Name, Phone, or Email",
#                             help="Enter customer name, phone number, or email to search",
#                             key="search_customer"
#                         )
#                     with search_col2:
#                         st.markdown("<br>", unsafe_allow_html=True)
#                         new_customer = st.checkbox("New Customer", key="new_customer")

#                     if customer_name and not new_customer:
#                         try:
#                             all_customers_df = fetch_all_customers()
#                             if st.session_state.get('debug_mode'):
#                                 st.write("Debug - Fetched customers:", len(all_customers_df))
                            
#                             search_term = customer_name.lower()
#                             matching_customers_df = all_customers_df[
#                                 all_customers_df['FULL_NAME'].str.lower().str.contains(search_term, na=False) |
#                                 all_customers_df['PHONE_NUMBER'].str.lower().str.contains(search_term, na=False) |
#                                 all_customers_df['EMAIL_ADDRESS'].str.lower().str.contains(search_term, na=False)
#                             ]
                            
#                             if not matching_customers_df.empty:
#                                 selected_option = st.selectbox(
#                                     "Select Customer",
#                                     options=["Select..."] + matching_customers_df['FULL_NAME'].tolist(),
#                                     key="customer_select"
#                                 )

#                                 if selected_option != "Select...":
#                                     try:
#                                         customer_details = matching_customers_df[
#                                             matching_customers_df['FULL_NAME'] == selected_option
#                                         ].iloc[0]

#                                         # Display editable customer details
#                                         st.markdown("### Customer Details")
#                                         col1, col2 = st.columns(2)
#                                         with col1:
#                                             scheduler.form_data.customer_data['first_name'] = st.text_input(
#                                                 "First Name",
#                                                 value=customer_details['FIRST_NAME'],
#                                                 key="edit_first_name"
#                                             )
#                                             scheduler.form_data.customer_data['phone_number'] = st.text_input(
#                                                 "Phone",
#                                                 value=customer_details['PHONE_NUMBER'],
#                                                 key="edit_phone"
#                                             )
#                                         with col2:
#                                             scheduler.form_data.customer_data['last_name'] = st.text_input(
#                                                 "Last Name",
#                                                 value=customer_details['LAST_NAME'],
#                                                 key="edit_last_name"
#                                             )
#                                             scheduler.form_data.customer_data['email_address'] = st.text_input(
#                                                 "Email",
#                                                 value=customer_details['EMAIL_ADDRESS'],
#                                                 key="edit_email"
#                                             )

#                                         # Primary Address
#                                         st.markdown("### Primary Address")
#                                         scheduler.form_data.customer_data['street_address'] = st.text_input(
#                                             "Street Address",
#                                             value=customer_details['STREET_ADDRESS'],
#                                             key="edit_street"
#                                         )
#                                         col1, col2 = st.columns(2)
#                                         with col1:
#                                             scheduler.form_data.customer_data['city'] = st.text_input(
#                                                 "City",
#                                                 value=customer_details['CITY'],
#                                                 key="edit_city"
#                                             )
#                                             scheduler.form_data.customer_data['state'] = st.text_input(
#                                                 "State",
#                                                 value=customer_details['STATE'],
#                                                 key="edit_state"
#                                             )
#                                         with col2:
#                                             scheduler.form_data.customer_data['zip_code'] = st.text_input(
#                                                 "ZIP Code",
#                                                 value=customer_details['ZIP_CODE'],
#                                                 key="edit_zip"
#                                             )

#                                         # Update customer ID
#                                         scheduler.form_data.customer_data['customer_id'] = customer_details['CUSTOMER_ID']
#                                         scheduler.form_data.customer_data['is_commercial'] = False
#                                     except Exception as e:
#                                         st.error(f"Error displaying customer details: {str(e)}")
#                                         if st.session_state.get('debug_mode'):
#                                             st.exception(e)
#                             else:
#                                 st.info("No matching customers found. Please enter customer details below.")
#                                 scheduler.display_customer_form()
#                         except Exception as e:
#                             st.error(f"Error searching customers: {str(e)}")
#                             if st.session_state.get('debug_mode'):
#                                 st.exception(e)
#                     else:
#                         scheduler.display_customer_form()
#                 except Exception as e:
#                     st.error(f"Error in residential customer section: {str(e)}")
#                     if st.session_state.get('debug_mode'):
#                         st.exception(e)

#                 # Service Address section for residential customers
#                 if scheduler.form_data.customer_data.get('first_name') or scheduler.form_data.customer_data.get('customer_id'):
#                     try:
#                         st.markdown("### Service Address")
#                         same_as_primary = st.checkbox("Same as Primary Address", value=True, key="same_as_primary")
                        
#                         if same_as_primary:
#                             scheduler.form_data.customer_data.update({
#                                 'service_address': scheduler.form_data.customer_data.get('street_address', ''),
#                                 'service_city': scheduler.form_data.customer_data.get('city', ''),
#                                 'service_state': scheduler.form_data.customer_data.get('state', ''),
#                                 'service_zip': scheduler.form_data.customer_data.get('zip_code', '')
#                             })
#                             scheduler.form_data.service_selection['same_as_primary'] = True
#                         else:
#                             service_col1, service_col2 = st.columns(2)
#                             with service_col1:
#                                 scheduler.form_data.customer_data['service_address'] = st.text_input(
#                                     "Service Street Address",
#                                     value=scheduler.form_data.customer_data.get('service_address', ''),
#                                     key="service_address"
#                                 )
#                                 scheduler.form_data.customer_data['service_city'] = st.text_input(
#                                     "Service City",
#                                     value=scheduler.form_data.customer_data.get('service_city', ''),
#                                     key="service_city"
#                                 )
#                             with service_col2:
#                                 scheduler.form_data.customer_data['service_state'] = st.text_input(
#                                     "Service State",
#                                     value=scheduler.form_data.customer_data.get('service_state', ''),
#                                     key="service_state"
#                                 )
#                                 scheduler.form_data.customer_data['service_zip'] = st.text_input(
#                                     "Service ZIP",
#                                     value=scheduler.form_data.customer_data.get('service_zip', ''),
#                                     key="service_zip"
#                                 )
#                             scheduler.form_data.customer_data['service_addr_sq_ft'] = st.number_input(
#                                 "Square Footage",
#                                 min_value=0,
#                                 step=100,
#                                 value=scheduler.form_data.customer_data.get('service_addr_sq_ft', 0),
#                                 key="service_sq_ft"
#                             )
#                             scheduler.form_data.service_selection['same_as_primary'] = False

#                     except Exception as e:
#                         st.error(f"Error in service address section: {str(e)}")
#                         if st.session_state.get('debug_mode'):
#                             st.exception(e)

#             # Service Selection Section
#             show_service_section = (
#                 scheduler.form_data.customer_data.get("account_id") is not None or
#                 bool(scheduler.form_data.customer_data.get("first_name"))
#             )
            
#             if show_service_section:
#                 st.markdown("### Select Services")
#                 services_selected = scheduler.display_service_selection()

#                 if services_selected:
#                     # Schedule Section
#                     st.markdown("### Schedule Service")
#                     schedule_selected = scheduler.process_service_scheduling()

#                     if schedule_selected:
#                         # Final Buttons
#                         col1, col2 = st.columns(2)
#                         with col1:
#                             if st.button("📅 Schedule Service", type="primary"):
#                                 try:
#                                     if scheduler.save_service():
#                                         st.success("Service scheduled successfully!")
#                                         st.balloons()
#                                         reset_session_state()
#                                         st.rerun()
#                                 except Exception as e:
#                                     st.error(f"Error saving service: {str(e)}")
#                                     if st.session_state.get('debug_mode'):
#                                         st.exception(e)
#                         with col2:
#                             if st.button("❌ Cancel", type="secondary"):
#                                 reset_session_state()
#                                 st.rerun()

#         except Exception as e:
#             st.error("An error occurred while processing the form")
#             debug_print(f"Form processing error: {str(e)}")
#             if st.session_state.get('debug_mode'):
#                 st.exception(e)

#     except Exception as e:
#         st.error("An unexpected error occurred")
#         debug_print(f"Page initialization error: {str(e)}")
#         if st.session_state.get('debug_mode'):
#             st.exception(e)

# if __name__ == "__main__":
#     new_service_page()