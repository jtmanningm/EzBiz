from datetime import datetime
import streamlit as st

from models.service import fetch_services
from models.transaction import save_transaction, get_additional_services
from models.employee import (
    fetch_employees, 
    get_employee_by_name,
    get_employee_rate
)

from utils.email import send_service_scheduled_email, send_service_completed_email, send_completion_email
from models.pricing import (
    get_active_pricing_strategy,
    calculate_final_price
)

def transaction_details_page():
    """Handle service transaction details with dynamic pricing"""
    st.title("Service Details")
    
    # Get selected service from session state
    selected_service = st.session_state.get('selected_service')
    if not selected_service:
        st.error("No service selected. Please select a service from scheduled services.")
        return

    # Initialize session state variables
    if 'selected_employees' not in st.session_state:
        st.session_state.selected_employees = []
    if 'payment_method_1' not in st.session_state:
        st.session_state.payment_method_1 = "Select Method"
    if 'payment_amount_1' not in st.session_state:
        st.session_state.payment_amount_1 = 0.0
    if 'payment_method_2' not in st.session_state:
        st.session_state.payment_method_2 = "Select Method"
    if 'payment_amount_2' not in st.session_state:
        st.session_state.payment_amount_2 = 0.0
    if 'transaction_notes' not in st.session_state:
        st.session_state.transaction_notes = selected_service.get('NOTES', '')
    if 'material_cost' not in st.session_state:
        st.session_state.material_cost = 0.0
    if 'labor_details' not in st.session_state:
        st.session_state.labor_details = []

    with st.container():
        # Display basic service info
        st.markdown(f"### {selected_service['SERVICE_NAME']}")
        st.markdown(f"**Customer:** {selected_service['CUSTOMER_NAME']}")
        if selected_service.get('NOTES'):
            st.markdown(f"**Notes:** {selected_service['NOTES']}")

        # Additional services selection
        st.markdown("### Services")
        service2_id, service3_id, total_base_cost = get_additional_services(
            selected_service['SERVICE_ID']
        )
        total_base_cost = float(total_base_cost)

        # Get deposit info
        deposit = float(selected_service.get('DEPOSIT', 0))
        
        # Get pricing strategy
        strategy = get_active_pricing_strategy()
        if strategy:
            st.info(f"Using pricing strategy: {strategy.name}")
        
        # Employee Assignment
        st.markdown("### Employee Assignment")
        employees_df = fetch_employees()
        selected_employees = st.multiselect(
            "Assign Employees",
            options=employees_df["FULL_NAME"].tolist(),
            default=st.session_state.selected_employees,
            help="Select employees who performed the service"
        )
        st.session_state.selected_employees = selected_employees

        # Initialize pricing components
        labor_details = []
        labor_cost = 0.0
        material_cost = float(st.session_state.material_cost)

        # Collect labor details if using Cost + Labor strategy
        if strategy and strategy.type == "Cost + Labor":
            st.subheader("Labor Details")
            for employee in selected_employees:
                col1, col2 = st.columns(2)
                with col1:
                    hours = st.number_input(
                        f"Hours worked by {employee}",
                        min_value=0.0,
                        step=0.5,
                        key=f"hours_{employee}"
                    )
                with col2:
                    rate = float(get_employee_rate(employee))
                    st.write(f"Rate: ${rate:.2f}/hr")
                
                if hours > 0:
                    labor_details.append({
                        "employee": employee,
                        "hours": float(hours),
                        "rate": rate
                    })

        # Material cost input
        material_cost = st.number_input(
            "Material Cost",
            min_value=0.0,
            step=5.0,
            value=float(st.session_state.material_cost)
        )
        st.session_state.material_cost = float(material_cost)

        # Calculate initial total before adjustments
        subtotal = float(total_base_cost)
        if labor_details:
            labor_cost = sum(float(detail['hours']) * float(detail['rate']) for detail in labor_details)
            subtotal += labor_cost

        if strategy and strategy.rules.get('include_materials', True):
            subtotal += float(material_cost)

        # Price adjustment section
        st.markdown("### Price Adjustment")
        col1, col2 = st.columns(2)
        with col1:
            adjustment_type = st.radio(
                "Adjustment Type",
                ["None", "Discount", "Additional Charge"]
            )
        
        adjustment_amount = 0.0
        if adjustment_type != "None":
            with col2:
                adjustment_amount = st.number_input(
                    f"Amount to {'subtract' if adjustment_type == 'Discount' else 'add'}",
                    min_value=0.0,
                    max_value=float(subtotal) if adjustment_type == "Discount" else 1000.0,
                    step=5.0,
                    help="Enter amount in dollars"
                )
                if adjustment_type == "Discount":
                    adjustment_amount = -adjustment_amount

        # Calculate final price
        final_price = float(subtotal + adjustment_amount)
        
        # Display price breakdown
        st.markdown("### Price Breakdown")
        st.write(f"Base Services Cost: ${float(total_base_cost):.2f}")
        
        if labor_details:
            st.write(f"Labor Cost: ${float(labor_cost):.2f}")
        
        if material_cost > 0:
            st.write(f"Material Cost: ${float(material_cost):.2f}")
        
        st.write(f"Subtotal: ${float(subtotal):.2f}")
        
        if adjustment_amount != 0:
            if adjustment_amount < 0:
                st.write(f"Discount Applied: -${abs(adjustment_amount):.2f}")
            else:
                st.write(f"Additional Charge: ${adjustment_amount:.2f}")
                
        if deposit > 0:
            st.write(f"Deposit Paid: ${float(deposit):.2f}")
        
        st.markdown(f"**Final Price: ${float(final_price):.2f}**")
        
        # Payment Collection Section
        amount_due = float(final_price - deposit)
        if amount_due > 0:
            st.markdown("### Payment Collection")
            st.write(f"**Amount to Collect: ${amount_due:.2f}**")
            
            # First payment method
            payment_method_1 = st.selectbox(
                "Payment Method",
                ["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"],
                index=["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"].index(
                    st.session_state.payment_method_1
                )
            )
            st.session_state.payment_method_1 = payment_method_1
            
            payment_amount_1 = st.number_input(
                "Amount",
                min_value=0.0,
                max_value=float(amount_due),
                value=float(st.session_state.payment_amount_1 if payment_method_1 != "Select Method" else 0.0)
            )
            st.session_state.payment_amount_1 = float(payment_amount_1)

            # Handle split payment
            remaining_after_first = float(amount_due - payment_amount_1)
            payment_method_2 = None
            payment_amount_2 = 0.0
            
            if remaining_after_first > 0:
                use_split = st.checkbox("Split Payment into Two Methods")
                
                if use_split:
                    st.write(f"**Remaining to Collect: ${remaining_after_first:.2f}**")
                    payment_method_2 = st.selectbox(
                        "Second Payment Method",
                        ["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"],
                        key="method2"
                    )
                    
                    payment_amount_2 = st.number_input(
                        "Amount",
                        min_value=0.0,
                        max_value=float(remaining_after_first),
                        value=float(remaining_after_first if payment_method_2 != "Select Method" else 0.0),
                        key="amount2"
                    )

        # Notes Section
        st.markdown("### Notes")
        transaction_notes = st.text_area(
            "Transaction Notes",
            value=st.session_state.transaction_notes,
            help="Add any additional notes about the service"
        )

        # Action Buttons
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Complete Transaction", type="primary", use_container_width=True):
            if not selected_employees:
                st.error("Please assign at least one employee")
                return
                    
            if amount_due > 0 and payment_method_1 == "Select Method":
                st.error("Please select a payment method")
                return
                    
            # Prepare transaction data
            transaction_data = {
                'service_id': selected_service['SERVICE_ID'],
                'service2_id': service2_id,
                'service3_id': service3_id,
                'customer_id': selected_service['CUSTOMER_OR_ACCOUNT_ID'],
                'original_amount': float(total_base_cost),
                'final_amount': float(final_price),
                'deposit': float(deposit),
                'amount_received': float(deposit + payment_amount_1 + payment_amount_2),
                'payment_method_1': payment_method_1 if payment_method_1 != "Select Method" else None,
                'payment_amount_1': float(payment_amount_1),
                'payment_method_2': payment_method_2 if payment_method_2 and payment_method_2 != "Select Method" else None,
                'payment_amount_2': float(payment_amount_2),
                'employee1_id': get_employee_by_name(selected_employees[0]) if len(selected_employees) > 0 else None,
                'employee2_id': get_employee_by_name(selected_employees[1]) if len(selected_employees) > 1 else None,
                'employee3_id': get_employee_by_name(selected_employees[2]) if len(selected_employees) > 2 else None,
                'start_time': st.session_state.get('service_start_time', datetime.now()).time().strftime('%H:%M:%S'),
                'end_time': datetime.now().time().strftime('%H:%M:%S'),
                'transaction_date': datetime.now().date(),
                'transaction_time': datetime.now().time(),
                'completion_date': datetime.now().date(),
                'notes': transaction_notes,
                'pricing_strategy_id': strategy.strategy_id if strategy else None,
                'price_details': {
                    'base_cost': float(total_base_cost),
                    'labor_cost': float(labor_cost),
                    'material_cost': float(material_cost),
                    'adjustment_amount': float(adjustment_amount),
                    'final_price': float(final_price)
                }
            }
            
            if save_transaction(transaction_data):
                try:
                    # Attempt to send completion email
                    email_sent = send_completion_email(transaction_data, selected_service)
                    if email_sent:
                        st.success("Transaction completed and confirmation email sent!")
                    else:
                        st.success("Transaction completed successfully!")
                        st.info("Note: Confirmation email could not be sent.")
                except Exception as e:
                    st.success("Transaction completed successfully!")
                    st.warning(f"Could not send confirmation email: {str(e)}")

                # Clear session state
                keys_to_clear = [
                    'service_start_time', 'selected_service', 'selected_employees',
                    'payment_method_1', 'payment_amount_1', 'payment_method_2',
                    'payment_amount_2', 'transaction_notes', 'material_cost', 'labor_details'
                ]
                for key in keys_to_clear:
                    st.session_state.pop(key, None)
                    
                st.session_state['page'] = 'completed_services'
                st.rerun()
            else:
                st.error("Failed to complete transaction. Please try again.")
    
    with col2:
        if st.button("Cancel", type="secondary", use_container_width=True):
            # Clear session state
            keys_to_clear = [
                'service_start_time', 'selected_service', 'selected_employees',
                'payment_method_1', 'payment_amount_1', 'payment_method_2',
                'payment_amount_2', 'transaction_notes', 'material_cost', 'labor_details'
            ]
            for key in keys_to_clear:
                st.session_state.pop(key, None)
                
            st.session_state['page'] = 'scheduled_services'
            st.rerun()

# Only run if called directly
if __name__ == "__main__":
    transaction_details_page()