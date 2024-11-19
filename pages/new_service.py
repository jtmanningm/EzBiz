import streamlit as st
from datetime import datetime, date, time
from typing import Optional
import pandas as pd

from models.customer import fetch_all_customers, save_customer
from models.service import (
    fetch_services,
    check_service_availability,
    save_service_schedule,
    get_available_time_slots
)
from utils.formatting import format_currency
from utils.validation import validate_phone, validate_email, validate_zip_code

def fetch_existing_customers() -> pd.DataFrame:
    """Fetch all existing customers from the database."""
    customers_df = fetch_all_customers()
    if customers_df.empty:
        return pd.DataFrame(columns=[
            "CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "PHONE_NUMBER", 
            "EMAIL_ADDRESS", "STREET_ADDRESS", "CITY", "STATE", "ZIP_CODE",
            "SERVICE_ADDRESS", "SERVICE_ADDRESS_2", "SERVICE_ADDRESS_3", 
            "SERVICE_ADDR_SQ_FT", "PRIMARY_CONTACT_METHOD", "TEXT_FLAG",
            "MEMBER_FLAG", "COMMENTS"
        ])
    return customers_df

def display_availability_selector(service_date: date, service_duration: int = 60) -> Optional[time]:
    """Display time slot selector with 30-minute intervals."""
    available_slots = get_available_time_slots(service_date, service_duration)
    
    if not available_slots:
        st.warning("No available time slots for selected date")
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

def new_service_page():
    """
    Complete new service scheduling page
    """
    st.markdown("""
        <div class="page-header">
            <h2>New Service</h2>
        </div>
    """, unsafe_allow_html=True)

    # Initialize session state
    if 'selected_customer_id' not in st.session_state:
        st.session_state['selected_customer_id'] = None

    # Customer Information Section
    with st.container():
        st.markdown("### Customer Information")
        customer_name = st.text_input(
            "Customer Name",
            help="Enter customer name to search or add new",
            key="new_customer_name"
        )

        # Fetch and display existing customers if name entered
        matching_customers = []
        if customer_name:
            existing_customers_df = fetch_existing_customers()
            if not existing_customers_df.empty:
                matching_customers = [
                    f"{row['FIRST_NAME']} {row['LAST_NAME']}"
                    for _, row in existing_customers_df.iterrows()
                    if customer_name.lower() in f"{row['FIRST_NAME']} {row['LAST_NAME']}".lower()
                ]
                
                if matching_customers:
                    selected_customer = st.selectbox(
                        "Select Existing Customer",
                        options=["Select..."] + matching_customers,
                        key="existing_customer_select"
                    )

                    if selected_customer and selected_customer != "Select...":
                        customer_details = existing_customers_df[
                            (existing_customers_df['FIRST_NAME'] + " " + existing_customers_df['LAST_NAME']) == selected_customer
                        ].iloc[0]
                        st.session_state['selected_customer_id'] = int(customer_details['CUSTOMER_ID'])
                        
                        # Display customer details
                        info_parts = ["Selected Customer Details:"]
                        
                        if 'PHONE_NUMBER' in customer_details and pd.notna(customer_details['PHONE_NUMBER']):
                            info_parts.append(f"Phone: {customer_details['PHONE_NUMBER']}")
                        
                        if 'EMAIL_ADDRESS' in customer_details and pd.notna(customer_details['EMAIL_ADDRESS']):
                            info_parts.append(f"Email: {customer_details['EMAIL_ADDRESS']}")
                        
                        # Primary Address
                        address_parts = []
                        if 'STREET_ADDRESS' in customer_details and pd.notna(customer_details['STREET_ADDRESS']):
                            address_parts.append(customer_details['STREET_ADDRESS'])
                        if 'CITY' in customer_details and pd.notna(customer_details['CITY']):
                            address_parts.append(customer_details['CITY'])
                        if 'STATE' in customer_details and pd.notna(customer_details['STATE']):
                            address_parts.append(customer_details['STATE'])
                        if 'ZIP_CODE' in customer_details and pd.notna(customer_details['ZIP_CODE']):
                            address_parts.append(str(customer_details['ZIP_CODE']))
                        
                        if address_parts:
                            info_parts.append(f"Address: {', '.join(address_parts)}")
                        
                        st.info("\n".join(info_parts))
                    else:
                        st.session_state['selected_customer_id'] = None

        # New customer form
        if not matching_customers or st.session_state['selected_customer_id'] is None:
            with st.container():
                col1, col2, col3 = st.columns(3)
                with col1:
                    phone = st.text_input(
                        "Phone Number",
                        key="new_customer_phone"
                    )
                with col2:
                    email = st.text_input(
                        "Email",
                        key="new_customer_email"
                    )
                with col3:
                    contact_preference = st.selectbox(
                        "Primary Contact Method",
                        ["Phone", "Text", "Email"],
                        key="new_customer_contact_pref"
                    )
                    text_flag = st.checkbox(
                        "Opt-in to Text Messages",
                        key="new_customer_text_flag"
                    )

            st.markdown("### Primary Address")
            col1, col2 = st.columns(2)
            with col1:
                street_address = st.text_input(
                    "Street Address",
                    key="new_customer_street"
                )
                city = st.text_input(
                    "City",
                    key="new_customer_city"
                )
            with col2:
                state = st.text_input(
                    "State",
                    key="new_customer_state"
                )
                zip_code = st.text_input(
                    "ZIP Code",
                    key="new_customer_zip"
                )

            # Service Address Section
            st.markdown("### Service Address")
            same_as_primary = st.checkbox("Same as Primary Address", value=True)
            
            if not same_as_primary:
                col1, col2 = st.columns(2)
                with col1:
                    service_address = st.text_input(
                        "Service Address",
                        key="new_customer_service_address"
                    )
                    service_address_2 = st.text_input(
                        "Service Address Line 2",
                        key="new_customer_service_address_2"
                    )
                    service_address_3 = st.text_input(
                        "Service Address Line 3",
                        key="new_customer_service_address_3"
                    )
                with col2:
                    service_addr_sq_ft = st.number_input(
                        "Square Footage",
                        min_value=0,
                        step=100,
                        key="service_sq_ft"
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
    services_df = fetch_services()
    if not services_df.empty:
        selected_services = st.multiselect(
            "Select Services",
            options=services_df['SERVICE_NAME'].tolist(),
            key="new_service_selection"
        )

        if selected_services:
            # Calculate total cost
            total_cost = sum(
                float(services_df[services_df['SERVICE_NAME'] == service]['COST'].iloc[0])
                for service in selected_services
            )

            st.markdown("### Service Summary")
            st.write(f"Total Cost: {format_currency(total_cost)}")

            # Recurring Service Section
            st.markdown("### Service Frequency")
            is_recurring = st.checkbox("Recurring Service", key="recurring_checkbox")
            recurrence_pattern = None
            if is_recurring:
                recurrence_pattern = st.selectbox(
                    "Recurrence Pattern",
                    ["Weekly", "Bi-Weekly", "Monthly"],
                    key="recurrence_pattern"
                )

            # Deposit Section
            deposit_amount = 0.0
            if st.checkbox("Add Deposit", key="deposit_checkbox"):
                if is_recurring:
                    st.info("For recurring services, deposit will only be applied to the first service.")
                
                deposit_amount = st.number_input(
                    "Deposit Amount",
                    min_value=0.0,
                    max_value=total_cost,
                    value=0.0,
                    step=5.0,
                    key="deposit_amount"
                )
                
                st.write(f"Remaining Balance: {format_currency(total_cost - deposit_amount)}")
                if is_recurring:
                    st.write(f"Future Service Cost: {format_currency(total_cost)}")

            # Service Notes
            service_notes = st.text_area(
                "Additional Notes",
                help="Enter any special instructions for this service",
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
                        if not customer_name or not phone:
                            st.error("Customer name and phone number are required")
                            return
                        
                        # Save new customer
                        customer_data = {
                            'first_name': customer_name.split()[0],
                            'last_name': ' '.join(customer_name.split()[1:]),
                            'phone_number': phone,
                            'email_address': email,
                            'street_address': street_address,
                            'city': city,
                            'state': state,
                            'zip_code': zip_code if zip_code else None,
                            'service_address': None if same_as_primary else service_address,
                            'service_address_2': None if same_as_primary else service_address_2,
                            'service_address_3': None if same_as_primary else service_address_3,
                            'service_addr_sq_ft': None if same_as_primary else service_addr_sq_ft,
                            'primary_contact_method': contact_preference,
                            'text_flag': text_flag,
                            'created_at': datetime.now(),
                            'last_updated_at': datetime.now(),
                            'comments': None
                        }
                        
                        customer_id = save_customer(customer_data)
                        if not customer_id:
                            st.error("Failed to save customer information")
                            return

                    # Schedule the service
                    service_scheduled = save_service_schedule(
                        customer_id=customer_id,
                        services=selected_services,
                        service_date=service_date,
                        service_time=service_time,
                        deposit_amount=deposit_amount,
                        notes=service_notes,
                        is_recurring=is_recurring,
                        recurrence_pattern=recurrence_pattern
                    )

                    if service_scheduled:
                        success_message = (
                            f"Service scheduled successfully!\n"
                            f"Deposit Amount: {format_currency(deposit_amount)}\n"
                            f"Remaining Balance: {format_currency(total_cost - deposit_amount)}"
                        )
                        if is_recurring:
                            success_message += f"\nRecurring: {recurrence_pattern}"
                        
                        st.success(success_message)
                        st.session_state['page'] = 'scheduled_services'
                        st.rerun()
                    else:
                        st.error("Failed to schedule service. Please try again.")
                except Exception as e:
                    st.error(f"Error scheduling service: {str(e)}")
    else:
        st.error("No services available. Please add services first.")

if __name__ == "__main__":
    new_service_page()