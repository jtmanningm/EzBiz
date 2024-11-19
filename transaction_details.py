# pages/transaction_details.py
import streamlit as st
from datetime import datetime, time
import pandas as pd
from database.connection import SnowflakeConnection

def fetch_existing_employees():
    """Fetch list of employees from database"""
    conn = SnowflakeConnection.get_instance()
    query = """
    SELECT 
        EMPLOYEE_ID,
        FIRST_NAME || ' ' || LAST_NAME as FULL_NAME
    FROM OPERATIONAL.CARPET.EMPLOYEE
    WHERE IS_ACTIVE = TRUE
    ORDER BY FULL_NAME
    """
    results = conn.execute_query(query)
    return pd.DataFrame(results, columns=['EMPLOYEE_ID', 'FULL_NAME'])

def get_employee_id(full_name):
    """Get employee ID from full name"""
    employees_df = fetch_existing_employees()
    employee = employees_df[employees_df['FULL_NAME'] == full_name]
    return employee.iloc[0]['EMPLOYEE_ID'] if not employee.empty else None

def save_transaction(transaction_data):
    """Save transaction to database"""
    conn = SnowflakeConnection.get_instance()
    query = """
    INSERT INTO OPERATIONAL.CARPET.SERVICE_TRANSACTION (
        CUSTOMER_ID, SERVICE_ID, AMOUNT, DISCOUNT, DEPOSIT,
        AMOUNT_RECEIVED, PYMT_MTHD_1, PYMT_MTHD_1_AMT,
        PYMT_MTHD_2, PYMT_MTHD_2_AMT, EMPLOYEE1_ID,
        EMPLOYEE2_ID, EMPLOYEE3_ID, START_TIME, END_TIME,
        TRANSACTION_DATE, TRANSACTION_TIME, COMMENTS,
        SERVICE2_ID, SERVICE3_ID
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        params = [
            transaction_data['Customer ID'], 
            transaction_data['Service ID'],
            transaction_data['Amount'],
            transaction_data['Discount'],
            transaction_data['Deposit'],
            transaction_data['Amount Received'],
            transaction_data['PYMT_MTHD_1'],
            transaction_data['PYMT_MTHD_1_AMT'],
            transaction_data['PYMT_MTHD_2'],
            transaction_data['PYMT_MTHD_2_AMT'],
            transaction_data['Employee1 ID'],
            transaction_data['Employee2 ID'],
            transaction_data['Employee3 ID'],
            transaction_data['Start Time'],
            transaction_data['End Time'],
            transaction_data['Transaction Date'],
            transaction_data['Transaction Time'],
            transaction_data['COMMENTS'],
            transaction_data['Service2 ID'],
            transaction_data['Service3 ID']
        ]
        conn.execute_query(query, params)
        return True
    except Exception as e:
        st.error(f"Error saving transaction: {str(e)}")
        return False

def transaction_details_page():
    """Handle selected service transaction details"""
    st.title("Service Details")
    
    selected_service = st.session_state.get('selected_service')
    if not selected_service:
        st.error("No service selected. Please select a service from scheduled services.")
        return

    # Create container for better mobile spacing
    with st.container():
        # Service Info
        st.markdown(f"### {selected_service['SERVICE_NAME']}")
        st.markdown(f"**Customer:** {selected_service['CUSTOMER_NAME']}")
        if selected_service.get('NOTES'):
            st.markdown(f"**Notes:** {selected_service['NOTES']}")

        # Payment Summary
        st.markdown("### Payment Summary")
        service_cost = float(selected_service.get('COST', 0))
        deposit = float(selected_service.get('DEPOSIT', 0))
        
        if st.session_state.get('_is_mobile', False):
            # Mobile view - stack vertically
            st.metric("Total Service Cost", f"${service_cost:.2f}")
            if deposit > 0:
                st.metric("Deposit Received", f"${deposit:.2f}")
            st.metric("Initial Balance", f"${service_cost:.2f}")
            st.metric("Current Balance", f"${(service_cost - deposit):.2f}")
        else:
            # Desktop view - side by side
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Service Cost", f"${service_cost:.2f}")
                if deposit > 0:
                    st.metric("Deposit Received", f"${deposit:.2f}")
            with col2:
                st.metric("Initial Balance", f"${service_cost:.2f}")
                st.metric("Current Balance", f"${(service_cost - deposit):.2f}")

        # Employee Assignment
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
        if st.session_state.get('_is_mobile', False):
            st.write(f"**Start:** {start_time.strftime('%I:%M %p')}")
            st.write(f"**End:** {end_time.strftime('%I:%M %p')}")
        else:
            col1, col2 = st.columns(2)
            with col1:
                st.write(f"**Start Time:** {start_time.strftime('%I:%M %p')}")
            with col2:
                st.write(f"**End Time:** {end_time.strftime('%I:%M %p')}")

        # Payment Details
        st.markdown("### Payment Details")
        remaining_balance = service_cost - deposit
        
        # Payment Method 1
        if st.session_state.get('_is_mobile', False):
            payment_method_1 = st.selectbox(
                "Payment Method 1",
                ["Select Method", "Cash", "Credit Card", "Debit Card", "Check", "CashApp", "Venmo", "Zelle"],
                key="payment_method_1"
            )
            payment_amount_1 = st.number_input(
                "Amount",
                min_value=0.0,
                max_value=remaining_balance,
                value=remaining_balance if payment_method_1 != "Select Method" else 0.0,
                key="payment_amount_1"
            )
        else:
            col1, col2 = st.columns(2)
            with col1:
                payment_method_1 = st.selectbox(
                    "Payment Method 1",
                    ["Select Method", "Cash", "Credit Card", "Debit Card", "Check", "CashApp", "Venmo", "Zelle"],
                    key="payment_method_1"
                )
            with col2:
                payment_amount_1 = st.number_input(
                    "Amount",
                    min_value=0.0,
                    max_value=remaining_balance,
                    value=remaining_balance if payment_method_1 != "Select Method" else 0.0,
                    key="payment_amount_1"
                )

        # Optional Payment Method 2
        use_second_payment = st.checkbox("Add Second Payment Method", key="use_second_payment")
        payment_method_2 = None
        payment_amount_2 = 0.0
        
        if use_second_payment and remaining_balance - payment_amount_1 > 0:
            if st.session_state.get('_is_mobile', False):
                payment_method_2 = st.selectbox(
                    "Payment Method 2",
                    ["Select Method", "Cash", "Credit Card", "Debit Card", "Check", "CashApp", "Venmo", "Zelle"],
                    key="payment_method_2"
                )
                payment_amount_2 = st.number_input(
                    "Amount",
                    min_value=0.0,
                    max_value=remaining_balance - payment_amount_1,
                    value=remaining_balance - payment_amount_1 if payment_method_2 != "Select Method" else 0.0,
                    key="payment_amount_2"
                )
            else:
                col1, col2 = st.columns(2)
                with col1:
                    payment_method_2 = st.selectbox(
                        "Payment Method 2",
                        ["Select Method", "Cash", "Credit Card", "Debit Card", "Check", "CashApp", "Venmo", "Zelle"],
                        key="payment_method_2"
                    )
                with col2:
                    payment_amount_2 = st.number_input(
                        "Amount",
                        min_value=0.0,
                        max_value=remaining_balance - payment_amount_1,
                        value=remaining_balance - payment_amount_1 if payment_method_2 != "Select Method" else 0.0,
                        key="payment_amount_2"
                    )

        # Final Summary
        total_new_payment = payment_amount_1 + payment_amount_2
        final_total_received = deposit + total_new_payment
        final_remaining_balance = service_cost - final_total_received

        st.markdown("### Final Summary")
        if st.session_state.get('_is_mobile', False):
            st.metric("Service Cost", f"${service_cost:.2f}")
            st.metric("Deposit Applied", f"${deposit:.2f}")
            st.metric("Additional Payments", f"${total_new_payment:.2f}")
            st.metric("Total Received", f"${final_total_received:.2f}")
            st.metric("Final Balance", f"${final_remaining_balance:.2f}")
        else:
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Service Cost", f"${service_cost:.2f}")
                st.metric("Deposit Applied", f"${deposit:.2f}")
                st.metric("Additional Payments", f"${total_new_payment:.2f}")
            with col2:
                st.metric("Total Received", f"${final_total_received:.2f}")
                st.metric("Final Balance", f"${final_remaining_balance:.2f}")

        # Notes
        st.markdown("### Notes")
        notes = st.text_area(
            "Transaction Notes",
            value=selected_service.get('NOTES', ''),
            help="Add any additional notes about the service"
        )

        # Action Buttons
        if st.session_state.get('_is_mobile', False):
            if st.button("Complete Transaction", type="primary", use_container_width=True):
                handle_transaction_completion(
                    selected_service, selected_employees, payment_method_1, payment_amount_1,
                    payment_method_2, payment_amount_2, service_cost, deposit,
                    final_total_received, start_time, end_time, notes
                )
            
            if st.button("Cancel", type="secondary", use_container_width=True):
                handle_transaction_cancel()
        else:
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Complete Transaction", type="primary", use_container_width=True):
                    handle_transaction_completion(
                        selected_service, selected_employees, payment_method_1, payment_amount_1,
                        payment_method_2, payment_amount_2, service_cost, deposit,
                        final_total_received, start_time, end_time, notes
                    )
            with col2:
                if st.button("Cancel", type="secondary", use_container_width=True):
                    handle_transaction_cancel()

def handle_transaction_completion(selected_service, selected_employees, payment_method_1, 
                               payment_amount_1, payment_method_2, payment_amount_2,
                               service_cost, deposit, final_total_received,
                               start_time, end_time, notes):
    """Handle transaction completion logic"""
    if not selected_employees:
        st.error("Please assign at least one employee")
        return

    if payment_method_1 == "Select Method" and payment_amount_1 > 0:
        st.error("Please select a payment method for Payment 1")
        return
            
    if payment_method_2 == "Select Method" and payment_amount_2 > 0:
        st.error("Please select a payment method for Payment 2")
        return

    # Prepare transaction data
    transaction_data = {
        'Customer ID': selected_service['CUSTOMER_OR_ACCOUNT_ID'],
        'Service ID': selected_service['SERVICE_ID'],
        'Amount': service_cost,
        'Discount': 0.0,
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
        'Service2 ID': None,
        'Service3 ID': None,
    }

    if save_transaction(transaction_data):
        st.success("Transaction completed successfully!")
        # Clear session state and redirect
        for key in ['service_start_time', 'selected_service']:
            st.session_state.pop(key, None)
        st.session_state['page'] = 'scheduled_services'
        st.rerun()
    else:
        st.error("Failed to complete transaction. Please try again.")

def handle_transaction_cancel():
    """Handle transaction cancellation"""
    for key in ['service_start_time', 'selected_service']:
        st.session_state.pop(key, None)
    st.session_state['page'] = 'scheduled_services'
    st.rerun()

if __name__ == "__main__":
    transaction_details_page()