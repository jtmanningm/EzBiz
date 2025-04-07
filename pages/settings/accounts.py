import streamlit as st
from typing import Optional, Dict, Any
import pandas as pd
from datetime import datetime
from database.connection import SnowflakeConnection
from models.account import (
    save_account, validate_account_data, search_accounts,
    save_account_service_address, fetch_account
)

def accounts_settings_page():
    st.title("Accounts & Billing Settings")
    
    # Create tabs for different account functions
    tab1, tab2, tab3 = st.tabs(["Account Search", "Create Account", "Settings"])
    
    # Tab 1: Account Search
    with tab1:
        st.header("Search Accounts")
        search_term = st.text_input(
            "Search by Business Name, Contact, Email or Phone",
            help="Enter business name, contact person, email, or phone to search"
        )
        
        if search_term:
            accounts_df = search_accounts(search_term)
            if not accounts_df.empty:
                selected_account = st.selectbox(
                    "Select Account",
                    options=["Select..."] + accounts_df['ACCOUNT_DETAILS'].tolist(),
                    key="account_select"
                )
                
                if selected_account != "Select...":
                    account_details = accounts_df[
                        accounts_df['ACCOUNT_DETAILS'] == selected_account
                    ].iloc[0]
                    
                    display_account_details(account_details)
            else:
                st.info("No matching accounts found.")
                if st.button("Create New Account", key="create_new_btn"):
                    st.session_state['show_account_form'] = True
                    st.experimental_rerun()
    
    # Tab 2: Create Account
    with tab2:
        st.header("Create New Account")
        display_account_form()
    
    # Tab 3: Settings
    with tab3:
        with st.container():
            st.header("Payment Methods")
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Accepted Payment Types")
                payment_types = {
                    "cash": st.checkbox("Cash", value=True),
                    "credit_card": st.checkbox("Credit Card", value=True),
                    "check": st.checkbox("Check", value=True),
                    "venmo": st.checkbox("Venmo"),
                    "paypal": st.checkbox("PayPal")
                }
                
                st.subheader("Default Payment Method")
                default_payment = st.selectbox(
                    "Select default payment method",
                    ["Cash", "Credit Card", "Check", "Venmo", "PayPal"]
                )
            
            with col2:
                st.subheader("Credit Card Processing")
                processor = st.selectbox(
                    "Select payment processor",
                    ["Stripe", "Square", "PayPal", "Other"]
                )
                
                if processor != "Other":
                    api_key = st.text_input(
                        f"{processor} API Key",
                        type="password"
                    )
        
        with st.container():
            st.header("Pricing Settings")
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Tax Settings")
                enable_tax = st.checkbox("Enable Sales Tax")
                if enable_tax:
                    tax_rate = st.number_input(
                        "Sales Tax Rate (%)",
                        min_value=0.0,
                        max_value=20.0,
                        value=8.25,
                        step=0.25
                    )
            
            with col2:
                st.subheader("Deposit Settings")
                require_deposit = st.checkbox("Require Deposit for Services")
                if require_deposit:
                    deposit_type = st.radio(
                        "Deposit Type",
                        ["Percentage", "Fixed Amount"]
                    )
                    if deposit_type == "Percentage":
                        deposit_amount = st.number_input(
                            "Deposit Percentage",
                            min_value=0,
                            max_value=100,
                            value=25,
                            step=5
                        )
                    else:
                        deposit_amount = st.number_input(
                            "Deposit Amount ($)",
                            min_value=0,
                            value=50,
                            step=10
                        )
        
        with st.container():
            st.header("Invoice Settings")
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Invoice Numbering")
                prefix = st.text_input(
                    "Invoice Number Prefix",
                    value="INV-",
                    max_chars=5
                )
                next_number = st.number_input(
                    "Next Invoice Number",
                    min_value=1,
                    value=1001,
                    step=1
                )
            
            with col2:
                st.subheader("Payment Terms")
                default_terms = st.selectbox(
                    "Default Payment Terms",
                    ["Due on Receipt", "Net 15", "Net 30", "Net 60"]
                )
                
                late_fee = st.checkbox("Enable Late Fees")
                if late_fee:
                    fee_type = st.radio(
                        "Late Fee Type",
                        ["Percentage", "Fixed Amount"]
                    )
                    if fee_type == "Percentage":
                        fee_amount = st.number_input(
                            "Late Fee Percentage",
                            min_value=0.0,
                            max_value=100.0,
                            value=2.5,
                            step=0.5
                        )
                    else:
                        fee_amount = st.number_input(
                            "Late Fee Amount ($)",
                            min_value=0,
                            value=25,
                            step=5
                        )
        
        # Save button at the bottom
        if st.button("Save Settings", type="primary"):
            st.success("Settings saved successfully!")

def display_account_details(account: Dict[str, Any]) -> None:
    """Display editable account details."""
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
        snowflake_conn = SnowflakeConnection.get_instance()
        try:
            result = snowflake_conn.execute_query(query, [account_id])
            if result:
                service_address = result[0]
        except Exception as e:
            st.error(f"Error fetching service address: {str(e)}")
    
    # Create a form for editing account details
    with st.form(key=f"account_edit_form_{account_id}"):
        st.subheader("Account Details")
        col1, col2 = st.columns(2)
        with col1:
            business_name = st.text_input(
                "Business Name",
                value=account.get('ACCOUNT_NAME', ''),
                key=f"edit_business_name_{account_id}"
            )
            contact_person = st.text_input(
                "Contact Person",
                value=account.get('CONTACT_PERSON', ''),
                key=f"edit_contact_person_{account_id}"
            )
        with col2:
            phone = st.text_input(
                "Phone",
                value=account.get('CONTACT_PHONE', ''),
                key=f"edit_business_phone_{account_id}"
            )
            email = st.text_input(
                "Email",
                value=account.get('CONTACT_EMAIL', ''),
                key=f"edit_business_email_{account_id}"
            )
        
        st.subheader("Billing Address")
        billing_address = st.text_input(
            "Street Address",
            value=account.get('BILLING_ADDRESS', ''),
            key=f"edit_billing_address_{account_id}"
        )
        col1, col2 = st.columns(2)
        with col1:
            billing_city = st.text_input(
                "City",
                value=account.get('CITY', ''),
                key=f"edit_billing_city_{account_id}"
            )
        with col2:
            billing_state = st.text_input(
                "State",
                value=account.get('STATE', ''),
                key=f"edit_billing_state_{account_id}"
            )
        billing_zip = st.text_input(
            "ZIP Code",
            value=account.get('ZIP_CODE', ''),
            key=f"edit_billing_zip_{account_id}"
        )
        
        # Service Address Section
        st.subheader("Service Address")
        
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
            
        use_billing_as_service = st.checkbox(
            "Same as Billing Address", 
            value=use_billing_as_service,
            key=f"edit_use_billing_as_service_{account_id}",
            on_change=lambda: st.experimental_rerun()
        )
        
        # Debug to check checkbox state
        print(f"DEBUG: edit_use_billing_as_service_{account_id} checkbox value: {use_billing_as_service}")
        
        if not use_billing_as_service:
            print(f"DEBUG: Showing service address fields (account: {account_id})")
            service_address_str = st.text_input(
                "Street Address",
                value=service_address.get('STREET_ADDRESS', ''),
                key=f"edit_service_address_{account_id}"
            )
            col1, col2 = st.columns(2)
            with col1:
                service_city = st.text_input(
                    "City",
                    value=service_address.get('CITY', ''),
                    key=f"edit_service_city_{account_id}"
                )
            with col2:
                service_state = st.text_input(
                    "State",
                    value=service_address.get('STATE', ''),
                    key=f"edit_service_state_{account_id}"
                )
            service_zip = st.text_input(
                "ZIP Code",
                value=service_address.get('ZIP_CODE', ''),
                key=f"edit_service_zip_{account_id}"
            )
            service_sq_ft = st.number_input(
                "Square Footage",
                min_value=0,
                step=100,
                value=int(service_address.get('SQUARE_FOOTAGE', 0) or 0),
                key=f"edit_service_sq_ft_{account_id}"
            )
            print(f"DEBUG: Service address values (edit): {service_address_str}, {service_city}, {service_state}, {service_zip}, {service_sq_ft}")
        else:
            # If using billing address as service address
            print(f"DEBUG: Using billing address for service (account: {account_id})")
            service_address_str = billing_address
            service_city = billing_city
            service_state = billing_state
            service_zip = billing_zip
            service_sq_ft = 0
            # Display a message to inform user the billing address will be used
            st.info("Using billing address as service address")
            print(f"DEBUG: Service address (from billing): {service_address_str}, {service_city}, {service_state}, {service_zip}")
        
        submitted = st.form_submit_button("Update Account")
        
        if submitted:
            # Create account data dictionary
            account_data = {
                'account_name': business_name,
                'account_type': 'Commercial',
                'contact_person': contact_person,
                'contact_email': email,
                'contact_phone': phone,
                'billing_address': billing_address,
                'city': billing_city,
                'state': billing_state,
                'zip_code': billing_zip,
                'active_flag': True
            }
            
            # Validate the data
            validation_errors = validate_account_data(account_data)
            if validation_errors:
                for error in validation_errors:
                    st.error(error)
                return
            
            # Save the account
            saved_account_id = save_account(account_data, account_id)
            
            if saved_account_id:
                # Handle service address
                service_address_data = {
                    'service_address': service_address_str,
                    'service_city': service_city,
                    'service_state': service_state,
                    'service_zip': service_zip,
                    'service_addr_sq_ft': service_sq_ft
                }
                
                # Save service address
                service_address_id = save_account_service_address(
                    saved_account_id,
                    service_address_data,
                    is_primary=True
                )
                
                if service_address_id:
                    st.success("Account updated successfully!")
                    st.rerun()
                else:
                    st.warning("Account updated but failed to save service address.")
            else:
                st.error("Failed to update account.")

def display_account_form() -> None:
    """Display form for creating a new account."""
    with st.form("new_account_form"):
        st.subheader("Business Information")
        business_name = st.text_input("Business Name", key="new_business_name")
        contact_person = st.text_input("Contact Person", key="new_contact_person")
        col1, col2 = st.columns(2)
        with col1:
            phone = st.text_input("Phone Number", key="new_business_phone")
        with col2:
            email = st.text_input("Email", key="new_business_email")
        
        st.subheader("Billing Address")
        billing_address = st.text_input("Street Address", key="new_billing_street")
        col1, col2 = st.columns(2)
        with col1:
            billing_city = st.text_input("City", key="new_city")
        with col2:
            billing_state = st.text_input("State", key="new_state")
        billing_zip = st.text_input("ZIP Code", key="new_zip_code")
        
        st.subheader("Service Address")
        
        # Debug to show when this code is executing
        print("DEBUG: Displaying service address section")
        
        use_billing_as_service = st.checkbox(
            "Same as Billing Address", 
            value=True,
            key="use_billing_as_service",
            on_change=lambda: st.experimental_rerun()
        )
        
        # Debug to show checkbox state
        print(f"DEBUG: use_billing_as_service checkbox value: {use_billing_as_service}")
        
        if not use_billing_as_service:
            print("DEBUG: Showing service address fields (different from billing)")
            service_address = st.text_input("Street Address", key="new_service_street")
            col1, col2 = st.columns(2)
            with col1:
                service_city = st.text_input("City", key="new_service_city")
            with col2:
                service_state = st.text_input("State", key="new_service_state")
            service_zip = st.text_input("ZIP Code", key="new_service_zip")
            service_sq_ft = st.number_input(
                "Square Footage",
                min_value=0,
                step=100,
                value=0,
                key="new_service_sq_ft"
            )
            # Display the values for debugging
            print(f"DEBUG: Service address values: {service_address}, {service_city}, {service_state}, {service_zip}, {service_sq_ft}")
        else:
            print("DEBUG: Using billing address for service address")
            service_address = billing_address
            service_city = billing_city
            service_state = billing_state
            service_zip = billing_zip
            service_sq_ft = 0
            
            # Display values copied from billing
            print(f"DEBUG: Service address (copied from billing): {service_address}, {service_city}, {service_state}, {service_zip}")
        
        submitted = st.form_submit_button("Save Account")
    
    if submitted:
        print("DEBUG: Form submitted - processing account save")
        # Create account data dictionary
        account_data = {
            'account_name': business_name,
            'account_type': 'Commercial',
            'contact_person': contact_person,
            'contact_email': email,
            'contact_phone': phone,
            'billing_address': billing_address,
            'city': billing_city,
            'state': billing_state,
            'zip_code': billing_zip,
            'active_flag': True
        }
        
        print(f"DEBUG: Account data ready for validation: {account_data}")
        
        # Validate the data
        validation_errors = validate_account_data(account_data)
        if validation_errors:
            print(f"DEBUG: Validation errors: {validation_errors}")
            for error in validation_errors:
                st.error(error)
            return
        
        print("DEBUG: Validation passed, saving account")
        # Save the account
        try:
            account_id = save_account(account_data)
            print(f"DEBUG: Account saved, ID returned: {account_id}")
            
            if account_id:
                # Handle service address
                service_address_data = {
                    'service_address': service_address,
                    'service_city': service_city,
                    'service_state': service_state,
                    'service_zip': service_zip,
                    'service_addr_sq_ft': service_sq_ft
                }
                
                print(f"DEBUG: Service address data: {service_address_data}")
                
                # Save service address
                try:
                    print(f"DEBUG: Saving service address for account {account_id}")
                    service_address_id = save_account_service_address(
                        account_id,
                        service_address_data,
                        is_primary=True
                    )
                    
                    print(f"DEBUG: Service address save result: {service_address_id}")
                    
                    if service_address_id:
                        st.success(f"Account created successfully with ID: {account_id}")
                        st.rerun()
                    else:
                        st.warning("Account created but failed to save service address.")
                        print("DEBUG: Failed to save service address, returned None")
                except Exception as e:
                    st.warning(f"Account created but error saving service address: {str(e)}")
                    print(f"DEBUG: Exception saving service address: {str(e)}")
                    import traceback
                    print(f"DEBUG: Service address save traceback: {traceback.format_exc()}")
            else:
                st.error("Failed to create account. Please check your inputs and try again.")
                print("DEBUG: save_account returned None")
        except Exception as e:
            st.error(f"Error saving account: {str(e)}")
            print(f"DEBUG: Exception saving account: {str(e)}")
            import traceback
            print(f"DEBUG: Account save traceback: {traceback.format_exc()}")