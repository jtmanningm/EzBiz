from datetime import datetime
import streamlit as st

from models.service import fetch_services
from models.transaction import save_transaction, get_additional_services
from models.employee import (
    fetch_employees, 
    get_employee_by_name,
    get_employee_rate
)

from models.pricing import (
    get_active_pricing_strategy,
    calculate_final_price
)

# def transaction_details_page():
#     """Handle service transaction details with dynamic pricing"""
#     st.title("Service Details")
    
#     # Get selected service from session state
#     selected_service = st.session_state.get('selected_service')
#     if not selected_service:
#         st.error("No service selected. Please select a service from scheduled services.")
#         return

#     # Initialize session state variables
#     if 'selected_employees' not in st.session_state:
#         st.session_state.selected_employees = []
#     if 'payment_method_1' not in st.session_state:
#         st.session_state.payment_method_1 = "Select Method"
#     if 'payment_amount_1' not in st.session_state:
#         st.session_state.payment_amount_1 = 0.0
#     if 'payment_method_2' not in st.session_state:
#         st.session_state.payment_method_2 = "Select Method"
#     if 'payment_amount_2' not in st.session_state:
#         st.session_state.payment_amount_2 = 0.0
#     if 'transaction_notes' not in st.session_state:
#         st.session_state.transaction_notes = selected_service.get('NOTES', '')

#     with st.container():
#         # Display basic service info
#         st.markdown(f"### {selected_service['SERVICE_NAME']}")
#         st.markdown(f"**Customer:** {selected_service['CUSTOMER_NAME']}")
#         if selected_service.get('NOTES'):
#             st.markdown(f"**Notes:** {selected_service['NOTES']}")

#         # Additional services selection
#         st.markdown("### Services")
#         service2_id, service3_id, total_base_cost = get_additional_services(
#             selected_service['SERVICE_ID']
#         )

#         # Get deposit info
#         deposit = float(selected_service.get('DEPOSIT', 0))
        
#         # Get pricing strategy
#         strategy = get_active_pricing_strategy()
#         if strategy:
#             st.info(f"Using pricing strategy: {strategy.name}")
        
#         # Employee Assignment
#         st.markdown("### Employee Assignment")
#         employees_df = fetch_employees()
#         selected_employees = st.multiselect(
#             "Assign Employees",
#             options=employees_df["FULL_NAME"].tolist(),
#             default=st.session_state.selected_employees,
#             help="Select employees who performed the service"
#         )
#         st.session_state.selected_employees = selected_employees

#         # Initialize pricing components
#         labor_details = []
#         material_cost = 0.0
        
#         # Collect required information based on strategy
#         if strategy and strategy.type == "Cost + Labor":
#             if strategy.rules.get('include_labor', True):
#                 st.subheader("Labor Details")
#                 for employee in selected_employees:
#                     col1, col2 = st.columns(2)
#                     with col1:
#                         hours = st.number_input(
#                             f"Hours worked by {employee}",
#                             min_value=0.0,
#                             step=0.5,
#                             key=f"hours_{employee}"
#                         )
#                     with col2:
#                         rate = get_employee_rate(employee)
#                         st.write(f"Rate: ${rate:.2f}/hr")
                    
#                     if hours > 0:
#                         labor_details.append({
#                             "employee": employee,
#                             "hours": hours,
#                             "rate": rate
#                         })

#                 if strategy.rules.get('include_materials', True):
#                     material_cost = st.number_input(
#                         "Material Cost",
#                         min_value=0.0,
#                         step=5.0
#                     )

#         elif strategy and strategy.type == "Fixed Price" and strategy.rules.get('allow_adjustments', True):
#             max_adjustment = float(strategy.rules.get('max_adjustment', 20.0))
#             adjustment_percentage = st.slider(
#                 "Price Adjustment (%)",
#                 min_value=-max_adjustment,
#                 max_value=max_adjustment,
#                 value=0.0,
#                 step=1.0
#             )
#             if adjustment_percentage != 0:
#                 adjustment_amount = total_base_cost * (adjustment_percentage / 100)
#                 st.write(f"Adjustment Amount: ${adjustment_amount:.2f}")
#                 total_base_cost += adjustment_amount

#         # Calculate final price
#         final_price, price_details = calculate_final_price(
#             total_base_cost,
#             strategy,
#             labor_details,
#             material_cost
#         )
        
#         # Display price breakdown
#         st.markdown("### Price Breakdown")
#         st.write(f"Services Cost: ${total_base_cost:.2f}")
        
#         if labor_details:
#             labor_cost = sum(detail['hours'] * detail['rate'] for detail in labor_details)
#             st.write(f"Labor Cost: ${labor_cost:.2f}")
#         if material_cost > 0:
#             st.write(f"Material Cost: ${material_cost:.2f}")
        
#         st.markdown(f"**Final Price: ${final_price:.2f}**")
        
#         # Payment Collection Section
#         amount_due = final_price - deposit
#         if amount_due > 0:
#             st.markdown("### Payment Collection")
#             st.write(f"**Amount to Collect: ${amount_due:.2f}**")
            
#             # First payment method
#             payment_method_1 = st.selectbox(
#                 "Payment Method",
#                 ["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"],
#                 index=["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"].index(
#                     st.session_state.payment_method_1
#                 )
#             )
#             st.session_state.payment_method_1 = payment_method_1
            
#             payment_amount_1 = st.number_input(
#                 "Amount",
#                 min_value=0.0,
#                 max_value=amount_due,
#                 value=st.session_state.payment_amount_1 if payment_method_1 != "Select Method" else 0.0
#             )
#             st.session_state.payment_amount_1 = payment_amount_1

#             # Handle split payment
#             remaining_after_first = amount_due - payment_amount_1
#             payment_method_2 = None
#             payment_amount_2 = 0.0
            
#             if remaining_after_first > 0:
#                 use_split = st.checkbox("Split Payment into Two Methods")
                
#                 if use_split:
#                     st.write(f"**Remaining to Collect: ${remaining_after_first:.2f}**")
#                     payment_method_2 = st.selectbox(
#                         "Second Payment Method",
#                         ["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"],
#                         key="method2"
#                     )
                    
#                     payment_amount_2 = st.number_input(
#                         "Amount",
#                         min_value=0.0,
#                         max_value=remaining_after_first,
#                         value=remaining_after_first if payment_method_2 != "Select Method" else 0.0,
#                         key="amount2"
#                     )

#         # Notes Section
#         st.markdown("### Notes")
#         transaction_notes = st.text_area(
#             "Transaction Notes",
#             value=st.session_state.transaction_notes,
#             help="Add any additional notes about the service"
#         )

#         # Action Buttons
#         col1, col2 = st.columns(2)
#         with col1:
#             if st.button("Complete Transaction", type="primary", use_container_width=True):
#                 if not selected_employees:
#                     st.error("Please assign at least one employee")
#                     return
                    
#                 if amount_due > 0 and payment_method_1 == "Select Method":
#                     st.error("Please select a payment method")
#                     return
                    
#                 # Prepare transaction data
#                 transaction_data = {
#                     'service_id': selected_service['SERVICE_ID'],
#                     'service2_id': service2_id,
#                     'service3_id': service3_id,
#                     'customer_id': selected_service['CUSTOMER_OR_ACCOUNT_ID'],
#                     'original_amount': total_base_cost,
#                     'final_amount': final_price,
#                     'discount': 0,
#                     'deposit': deposit,
#                     'amount_received': deposit + payment_amount_1 + payment_amount_2,
#                     'payment_method_1': payment_method_1 if payment_method_1 != "Select Method" else None,
#                     'payment_amount_1': payment_amount_1,
#                     'payment_method_2': payment_method_2 if payment_method_2 and payment_method_2 != "Select Method" else None,
#                     'payment_amount_2': payment_amount_2,
#                     'employee1_id': get_employee_by_name(selected_employees[0]) if len(selected_employees) > 0 else None,
#                     'employee2_id': get_employee_by_name(selected_employees[1]) if len(selected_employees) > 1 else None,
#                     'employee3_id': get_employee_by_name(selected_employees[2]) if len(selected_employees) > 2 else None,
#                     'start_time': st.session_state.get('service_start_time', datetime.now()).time().strftime('%H:%M:%S'),
#                     'end_time': datetime.now().time().strftime('%H:%M:%S'),
#                     'transaction_date': datetime.now().date(),
#                     'transaction_time': datetime.now().time(),
#                     'notes': transaction_notes,
#                     'pricing_strategy_id': strategy.strategy_id if strategy else None,
#                     'price_details': price_details
#                 }
                
#                 if save_transaction(transaction_data):
#                     st.success("Transaction completed successfully!")
#                     # Clear session state
#                     keys_to_clear = [
#                         'service_start_time', 'selected_service', 'selected_employees',
#                         'payment_method_1', 'payment_amount_1', 'payment_method_2',
#                         'payment_amount_2', 'transaction_notes'
#                     ]
#                     for key in keys_to_clear:
#                         st.session_state.pop(key, None)
#                     st.session_state['page'] = 'completed_services'
#                     st.rerun()
#                 else:
#                     st.error("Failed to complete transaction. Please try again.")
        
#         with col2:
#             if st.button("Cancel", type="secondary", use_container_width=True):
#                 # Clear session state
#                 keys_to_clear = [
#                     'service_start_time', 'selected_service', 'selected_employees',
#                     'payment_method_1', 'payment_amount_1', 'payment_method_2',
#                     'payment_amount_2', 'transaction_notes'
#                 ]
#                 for key in keys_to_clear:
#                     st.session_state.pop(key, None)
#                 st.session_state['page'] = 'scheduled_services'
#                 st.rerun()

# if __name__ == "__main__":
#     transaction_details_page()

# def transaction_details_page():
#     """Handle service transaction details with dynamic pricing"""
#     st.title("Service Details")
    
#     # Get selected service from session state
#     selected_service = st.session_state.get('selected_service')
#     if not selected_service:
#         st.error("No service selected. Please select a service from scheduled services.")
#         return

#     # Initialize session state variables
#     if 'selected_employees' not in st.session_state:
#         st.session_state.selected_employees = []
#     if 'payment_method_1' not in st.session_state:
#         st.session_state.payment_method_1 = "Select Method"
#     if 'payment_amount_1' not in st.session_state:
#         st.session_state.payment_amount_1 = 0.0
#     if 'payment_method_2' not in st.session_state:
#         st.session_state.payment_method_2 = "Select Method"
#     if 'payment_amount_2' not in st.session_state:
#         st.session_state.payment_amount_2 = 0.0
#     if 'transaction_notes' not in st.session_state:
#         st.session_state.transaction_notes = selected_service.get('NOTES', '')

#     with st.container():
#         # Display basic service info
#         st.markdown(f"### {selected_service['SERVICE_NAME']}")
#         st.markdown(f"**Customer:** {selected_service['CUSTOMER_NAME']}")
#         if selected_service.get('NOTES'):
#             st.markdown(f"**Notes:** {selected_service['NOTES']}")

#         # Fetch available services
#         services_df = fetch_services()
#         if services_df.empty:
#             st.error("No additional services available.")
#             return

#         # Additional services selection
#         st.markdown("### Add Additional Services")
#         service2 = st.selectbox(
#             "Select a Second Service (optional):",
#             ["None"] + services_df['SERVICE_NAME'].tolist(),
#             key="service2_select"
#         )

#         service2_id = None
#         additional_cost = 0.0
#         if service2 != "None":
#             service2_details = services_df[services_df['SERVICE_NAME'] == service2]
#             if not service2_details.empty:
#                 service2_id = int(service2_details['SERVICE_ID'].iloc[0])
#                 service2_cost = float(service2_details['COST'].iloc[0])
#                 additional_cost += service2_cost
#                 st.write(f"Added {service2} (${service2_cost:.2f})")

#         service3_id = None
#         if service2_id:
#             service3 = st.selectbox(
#                 "Select a Third Service (optional):",
#                 ["None"] + services_df['SERVICE_NAME'].tolist(),
#                 key="service3_select"
#             )
#             if service3 != "None":
#                 service3_details = services_df[services_df['SERVICE_NAME'] == service3]
#                 if not service3_details.empty:
#                     service3_id = int(service3_details['SERVICE_ID'].iloc[0])
#                     service3_cost = float(service3_details['COST'].iloc[0])
#                     additional_cost += service3_cost
#                     st.write(f"Added {service3} (${service3_cost:.2f})")

#         # Calculate total cost
#         base_cost = float(selected_service.get('COST', 0))
#         total_cost = base_cost + additional_cost
#         st.write(f"**Base Service Cost:** ${base_cost:.2f}")
#         st.write(f"**Total Transaction Cost:** ${total_cost:.2f}")
        
#         # Get deposit info
#         deposit = float(selected_service.get('DEPOSIT', 0))
        
#         # Employee Assignment
#         st.markdown("### Employee Assignment")
#         employees_df = fetch_employees()
#         selected_employees = st.multiselect(
#             "Assign Employees",
#             options=employees_df["FULL_NAME"].tolist(),
#             default=st.session_state.selected_employees,
#             help="Select employees who performed the service"
#         )
#         st.session_state.selected_employees = selected_employees

#         # Payment Collection Section
#         amount_due = total_cost - deposit
#         if amount_due > 0:
#             st.markdown("### Payment Collection")
#             st.write(f"**Amount to Collect: ${amount_due:.2f}**")
            
#             # First payment method
#             payment_method_1 = st.selectbox(
#                 "Payment Method",
#                 ["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"],
#                 index=["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"].index(
#                     st.session_state.payment_method_1
#                 )
#             )
#             st.session_state.payment_method_1 = payment_method_1
            
#             payment_amount_1 = st.number_input(
#                 "Amount",
#                 min_value=0.0,
#                 max_value=amount_due,
#                 value=st.session_state.payment_amount_1 if payment_method_1 != "Select Method" else 0.0
#             )
#             st.session_state.payment_amount_1 = payment_amount_1

#             # Handle split payment
#             remaining_after_first = amount_due - payment_amount_1
#             payment_method_2 = None
#             payment_amount_2 = 0.0
            
#             if remaining_after_first > 0:
#                 use_split = st.checkbox("Split Payment into Two Methods")
                
#                 if use_split:
#                     st.write(f"**Remaining to Collect: ${remaining_after_first:.2f}**")
#                     payment_method_2 = st.selectbox(
#                         "Second Payment Method",
#                         ["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"],
#                         key="method2"
#                     )
                    
#                     payment_amount_2 = st.number_input(
#                         "Amount",
#                         min_value=0.0,
#                         max_value=remaining_after_first,
#                         value=remaining_after_first if payment_method_2 != "Select Method" else 0.0,
#                         key="amount2"
#                     )

#         # Notes Section
#         st.markdown("### Notes")
#         transaction_notes = st.text_area(
#             "Transaction Notes",
#             value=st.session_state.transaction_notes,
#             help="Add any additional notes about the service"
#         )

#         # Action Buttons
#         col1, col2 = st.columns(2)
#         with col1:
#             if st.button("Complete Transaction", type="primary", use_container_width=True):
#                 if not selected_employees:
#                     st.error("Please assign at least one employee")
#                     return
                    
#                 if amount_due > 0 and payment_method_1 == "Select Method":
#                     st.error("Please select a payment method")
#                     return

#                 # Prepare transaction data
#                 transaction_data = {
#                     'service_id': selected_service['SERVICE_ID'],
#                     'service2_id': service2_id,
#                     'service3_id': service3_id,
#                     'customer_id': selected_service['CUSTOMER_OR_ACCOUNT_ID'],
#                     'final_amount': total_cost,
#                     'deposit': deposit,
#                     'amount_received': deposit + payment_amount_1 + payment_amount_2,
#                     'payment_method_1': payment_method_1 if payment_method_1 != "Select Method" else None,
#                     'payment_amount_1': payment_amount_1,
#                     'payment_method_2': payment_method_2 if payment_method_2 and payment_method_2 != "Select Method" else None,
#                     'payment_amount_2': payment_amount_2,
#                     'employee1_id': get_employee_by_name(selected_employees[0]) if len(selected_employees) > 0 else None,
#                     'employee2_id': get_employee_by_name(selected_employees[1]) if len(selected_employees) > 1 else None,
#                     'employee3_id': get_employee_by_name(selected_employees[2]) if len(selected_employees) > 2 else None,
#                     'start_time': st.session_state.get('service_start_time', datetime.now()).time().strftime('%H:%M:%S'),
#                     'end_time': datetime.now().time().strftime('%H:%M:%S'),
#                     'transaction_date': datetime.now().date(),
#                     'transaction_time': datetime.now().time(),
#                     'notes': transaction_notes
#                 }

#                 if save_transaction(transaction_data):
#                     st.success("Transaction completed successfully!")
#                     # Clear session state
#                     keys_to_clear = [
#                         'service_start_time', 'selected_service', 'selected_employees',
#                         'payment_method_1', 'payment_amount_1', 'payment_method_2',
#                         'payment_amount_2', 'transaction_notes'
#                     ]
#                     for key in keys_to_clear:
#                         st.session_state.pop(key, None)
#                     st.session_state['page'] = 'completed_services'
#                     st.rerun()
#                 else:
#                     st.error("Failed to save the transaction. Please try again.")

#         with col2:
#             if st.button("Cancel", type="secondary", use_container_width=True):
#                 # Clear session state
#                 keys_to_clear = [
#                     'service_start_time', 'selected_service', 'selected_employees',
#                     'payment_method_1', 'payment_amount_1', 'payment_method_2',
#                     'payment_amount_2', 'transaction_notes'
#                 ]
#                 for key in keys_to_clear:
#                     st.session_state.pop(key, None)
#                 st.session_state['page'] = 'scheduled_services'
#                 st.rerun()

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

        # Material cost input (available for all strategies)
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
            try:
                labor_cost = sum(
                    float(detail['hours']) * float(detail['rate'])
                    for detail in labor_details
                )
                subtotal += labor_cost
            except (TypeError, ValueError) as e:
                st.error(f"Error calculating labor cost: {e}")
                labor_cost = 0.0

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
                    st.success("Transaction completed successfully!")
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

if __name__ == "__main__":
    transaction_details_page()