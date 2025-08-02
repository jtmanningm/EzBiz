from datetime import datetime
import streamlit as st
import json
from typing import Dict, Any, Optional
from database.connection import SnowflakeConnection
from models.employee import fetch_employees, get_employee_by_name, get_employee_rate
from models.pricing import get_active_pricing_strategy
from utils.formatting import format_currency
from utils.email import send_completion_email
from utils.null_handling import safe_get_float, safe_get_int, safe_get_string, safe_get_bool

# Initialize database connection
snowflake_conn = SnowflakeConnection.get_instance()

def safe_get_row_value(row, column: str, default: Any = None) -> Any:
    """
    Safely get a value from a Snowflake Row object.
    
    Args:
        row: Snowflake Row object
        column: Column name to access
        default: Default value if column doesn't exist or is None
    
    Returns:
        Column value or default
    """
    try:
        value = getattr(row, column)
        return value if value is not None else default
    except AttributeError:
        return default

def transaction_details_page():
    """Display and handle service transaction details"""
    st.title("Service Details")
    
    # Get selected service from session state
    selected_service = st.session_state.get('selected_service')
    if not selected_service:
        st.error("No service selected. Please select a service from scheduled services.")
        return

    # Try different ways to get the transaction ID
    transaction_id = None
    transaction_id = (
        safe_get_int(selected_service.get('TRANSACTION_ID')) or 
        safe_get_int(selected_service.get('ID'))
    )
    
    if not transaction_id:
        st.error("Could not determine transaction ID. Please try selecting the service again.")
        return

    # Get transaction details
    query = """
    SELECT 
        t.ID,
        t.SERVICE_NAME,
        t.SERVICE_ID,
        t.SERVICE2_ID,
        t.SERVICE3_ID,
        t.BASE_SERVICE_COST,
        t.MATERIAL_COST,
        t.TOTAL_LABOR_COST,
        t.COMMENTS,
        t.STATUS,
        t.PRICING_STRATEGY,
        t.DEPOSIT,
        t.DEPOSIT_PAID,
        t.START_TIME,
        t.MARKUP_PERCENTAGE,
        t.PRICE_ADJUSTMENTS_JSON,
        COALESCE(c.FIRST_NAME || ' ' || c.LAST_NAME, a.ACCOUNT_NAME) as CUSTOMER_NAME,
        c.EMAIL_ADDRESS as CUSTOMER_EMAIL,  -- Added customer email
        s1.SERVICE_NAME as PRIMARY_SERVICE_NAME,
        s1.SERVICE_DURATION as PRIMARY_DURATION,
        s2.SERVICE_NAME as SERVICE2_NAME,
        s2.COST as SERVICE2_COST,
        s3.SERVICE_NAME as SERVICE3_NAME,
        s3.COST as SERVICE3_COST,
        sa.STREET_ADDRESS as SERVICE_ADDRESS,  -- Added service address
        sa.CITY as SERVICE_CITY,
        sa.STATE as SERVICE_STATE,
        sa.ZIP_CODE as SERVICE_ZIP
    FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION t
    LEFT JOIN OPERATIONAL.CARPET.CUSTOMER c ON t.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN OPERATIONAL.CARPET.ACCOUNTS a ON t.ACCOUNT_ID = a.ACCOUNT_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES s1 ON t.SERVICE_ID = s1.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES s2 ON t.SERVICE2_ID = s2.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES s3 ON t.SERVICE3_ID = s3.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICE_ADDRESSES sa ON t.CUSTOMER_ID = sa.CUSTOMER_ID
    WHERE t.ID = ?
    """
    
    result = snowflake_conn.execute_query(query, [transaction_id])
    if not result:
        st.error("Could not load transaction details.")
        return

    transaction = result[0]
    
    # Initialize key variables
    payment_method_2 = None
    payment_amount_2 = 0.0
    labor_details = []
    labor_cost = 0.0
    material_cost = 0.0

    # Display basic service info
    st.markdown(f"### {safe_get_row_value(transaction, 'SERVICE_NAME')}")
    st.markdown(f"**Customer:** {safe_get_row_value(transaction, 'CUSTOMER_NAME')}")
    
    # Display service address if available
    address_parts = [
        safe_get_row_value(transaction, 'SERVICE_ADDRESS'),
        safe_get_row_value(transaction, 'SERVICE_CITY'),
        safe_get_row_value(transaction, 'SERVICE_STATE'),
        str(safe_get_row_value(transaction, 'SERVICE_ZIP'))
    ]
    address = ', '.join(filter(None, address_parts))
    if address:
        st.markdown(f"**Service Address:** {address}")
    
    notes = safe_get_row_value(transaction, 'COMMENTS')
    if notes:
        st.markdown(f"**Notes:** {notes}")

    # Get active pricing strategy
    strategy = get_active_pricing_strategy()
    if strategy:
        st.info(f"Using pricing strategy: {strategy.name}")

    # Display base cost information
    st.markdown("### Services")
    base_cost = safe_get_float(safe_get_row_value(transaction, 'BASE_SERVICE_COST', 0))
    
    # If base cost is 0, try using AMOUNT field as fallback
    if base_cost == 0:
        base_cost = safe_get_float(safe_get_row_value(transaction, 'AMOUNT', 0))
    primary_service_name = safe_get_row_value(transaction, 'PRIMARY_SERVICE_NAME', None)
    
    # If no primary service name from join, try getting from SERVICE_NAME field
    if not primary_service_name:
        primary_service_name = safe_get_row_value(transaction, 'SERVICE_NAME', 'Unknown Service')
    
    # Debug information if in debug mode
    if st.session_state.get('debug_mode'):
        st.write("Debug - Transaction fields:")
        st.json({k: str(v) for k, v in transaction.items() if 'SERVICE' in k.upper()})
        st.write("Debug - Selected service from session:")
        if 'selected_service' in st.session_state:
            st.json({k: str(v) for k, v in st.session_state.selected_service.items()})
        st.write(f"Debug - Using transaction_id: {transaction_id}")
    
    st.write(f"Primary Service: {primary_service_name} - ${base_cost:.2f}")
    
    total_cost = base_cost
    
    # Show additional services if present
    service2_name = safe_get_row_value(transaction, 'SERVICE2_NAME')
    if service2_name:
        service2_cost = safe_get_float(safe_get_row_value(transaction, 'SERVICE2_COST', 0))
        st.write(f"Additional Service 1: {service2_name} - ${service2_cost:.2f}")
        total_cost += service2_cost
        
    service3_name = safe_get_row_value(transaction, 'SERVICE3_NAME')
    if service3_name:
        service3_cost = safe_get_float(safe_get_row_value(transaction, 'SERVICE3_COST', 0))
        st.write(f"Additional Service 2: {service3_name} - ${service3_cost:.2f}")
        total_cost += service3_cost

    # Employee Assignment
    st.markdown("### Employee Assignment")
    employees_df = fetch_employees()
    selected_employees = st.multiselect(
        "Assign Employees",
        options=employees_df["FULL_NAME"].tolist(),
        default=st.session_state.get('selected_employees', []),
        help="Select employees who performed the service"
    )
    st.session_state.selected_employees = selected_employees

    # Labor and material costs - only show if using Cost + Labor strategy
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
                labor_cost += hours * rate

        # Material cost input - only show for Cost + Labor strategy
        material_cost = st.number_input(
            "Material Cost",
            min_value=0.0,
            step=5.0,
            value=safe_get_float(safe_get_row_value(transaction, 'MATERIAL_COST', 0))
        )

    # Calculate subtotal
    subtotal = total_cost
    if strategy and strategy.type == "Cost + Labor":
        subtotal += labor_cost + material_cost

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
    final_price = subtotal + adjustment_amount
    
    # Display price breakdown
    st.markdown("### Price Breakdown")
    st.write(f"Base Services Cost: ${float(total_cost):.2f}")
    
    if strategy and strategy.type == "Cost + Labor":
        if labor_cost > 0:
            st.write(f"Labor Cost: ${labor_cost:.2f}")
        if material_cost > 0:
            st.write(f"Material Cost: ${material_cost:.2f}")
    
    st.write(f"Subtotal: ${subtotal:.2f}")
    
    if adjustment_amount != 0:
        if adjustment_amount < 0:
            st.write(f"Discount Applied: -${abs(adjustment_amount):.2f}")
        else:
            st.write(f"Additional Charge: ${adjustment_amount:.2f}")
            
    deposit = safe_get_float(safe_get_row_value(transaction, 'DEPOSIT', 0))
    if deposit > 0:
        st.write(f"Deposit Paid: ${deposit:.2f}")
    
    st.markdown(f"**Final Price: ${final_price:.2f}**")
    
    # Payment Collection Section
    amount_due = final_price - deposit
    payment_amount_1 = 0.0
    payment_method_1 = "Select Method"
    
    if amount_due > 0:
        st.markdown("### Payment Collection")
        st.write(f"**Amount to Collect: ${amount_due:.2f}**")
        
        payment_method_1 = st.selectbox(
            "Payment Method",
            ["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"],
            index=["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"].index(
                st.session_state.get('payment_method_1', "Select Method")
            )
        )
        st.session_state.payment_method_1 = payment_method_1
        
        payment_amount_1 = st.number_input(
            "Amount",
            min_value=0.0,
            max_value=amount_due,
            value=float(st.session_state.get('payment_amount_1', 0.0))
        )
        st.session_state.payment_amount_1 = payment_amount_1

        # Handle split payment
        remaining_after_first = amount_due - payment_amount_1
        if remaining_after_first > 0:
            use_split = st.checkbox("Split Payment into Two Methods")
            
            if use_split:
                st.write(f"**Remaining to Collect: ${remaining_after_first:.2f}**")
                payment_method_2 = st.selectbox(
                    "Second Payment Method",
                    ["Select Method", "Cash", "Credit Card", "Check", "Digital Payment"]
                )
                
                payment_amount_2 = st.number_input(
                    "Amount",
                    min_value=0.0,
                    max_value=remaining_after_first,
                    value=remaining_after_first if payment_method_2 != "Select Method" else 0.0
                )

    # Notes Section
    st.markdown("### Notes")
    transaction_notes = st.text_area(
        "Transaction Notes",
        value=safe_get_row_value(transaction, 'COMMENTS', ''),
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
            
            # Prepare price adjustments JSON
            price_adjustments = {
                'base_cost': float(total_cost),
                'labor_cost': float(labor_cost),
                'material_cost': float(material_cost),
                'adjustment_amount': float(adjustment_amount),
                'final_price': float(final_price)
            }
                    
            # Update transaction
            update_query = """
            UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
            SET 
                STATUS = 'COMPLETED',
                COMPLETION_DATE = CURRENT_DATE(),
                AMOUNT = ?,
                PYMT_MTHD_1 = ?,
                PYMT_MTHD_1_AMT = ?,
                PYMT_MTHD_2 = ?,
                PYMT_MTHD_2_AMT = ?,
                EMPLOYEE1_ID = ?,
                EMPLOYEE2_ID = ?,
                EMPLOYEE3_ID = ?,
                END_TIME = ?,
                COMMENTS = ?,
                TOTAL_LABOR_COST = ?,
                MATERIAL_COST = ?,
                PRICE_ADJUSTMENTS_JSON = ?,
                LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
            WHERE ID = ?
            """
            
            try:
                # Prepare email data
                email_data = {
                    'customer_name': safe_get_row_value(transaction, 'CUSTOMER_NAME'),
                    'customer_email': safe_get_row_value(transaction, 'CUSTOMER_EMAIL'),
                    'service_name': safe_get_row_value(transaction, 'SERVICE_NAME'),
                    'service_date': selected_service['SERVICE_DATE'],
                    'final_amount': final_price,
                    'payment_method': payment_method_1,
                    'payment_amount': payment_amount_1,
                    'second_payment_method': payment_method_2 if payment_method_2 and payment_method_2 != "Select Method" else None,
                    'second_payment_amount': payment_amount_2,
                    'service_address': address,
                    'notes': transaction_notes,
                    'employees': selected_employees
                }
                
                params = [
                    final_price,
                    payment_method_1 if payment_method_1 != "Select Method" else None,
                    payment_amount_1,
                    payment_method_2 if payment_method_2 and payment_method_2 != "Select Method" else None,
                    payment_amount_2,
                    get_employee_by_name(selected_employees[0]) if len(selected_employees) > 0 else None,
                    get_employee_by_name(selected_employees[1]) if len(selected_employees) > 1 else None,
                    get_employee_by_name(selected_employees[2]) if len(selected_employees) > 2 else None,
                    datetime.now().time().strftime('%H:%M:%S'),
                    transaction_notes,
                    labor_cost,
                    material_cost,
                    json.dumps(price_adjustments),
                    transaction_id
                ]
                
                snowflake_conn.execute_query(update_query, params)
                
                # Send completion email
                try:
                    email_sent = send_completion_email(email_data, selected_service)
                    if email_sent:
                        st.success("Transaction completed and confirmation email sent!")
                    else:
                        st.success("Transaction completed successfully!")
                        st.warning("Note: Confirmation email could not be sent.")
                except Exception as e:
                    print(f"Error sending email: {str(e)}")
                    st.success("Transaction completed successfully!")
                    st.warning("Unable to send confirmation email, but service was scheduled successfully.")
                
                # Clear session state
                keys_to_clear = [
                    'service_start_time', 'selected_service', 'selected_employees',
                    'payment_method_1', 'payment_amount_1', 'payment_method_2',
                    'payment_amount_2', 'transaction_notes'
                ]
                for key in keys_to_clear:
                    st.session_state.pop(key, None)
                
                st.session_state['page'] = 'completed_services'
                st.rerun()
                
            except Exception as e:
                print(f"Error completing transaction: {str(e)}")
                st.error(f"Failed to complete transaction: {str(e)}")
                
    with col2:
        if st.button("Cancel", type="secondary", use_container_width=True):
            # Clear session state
            keys_to_clear = [
                'service_start_time', 
                'selected_service', 
                'selected_employees',
                'payment_method_1', 
                'payment_amount_1', 
                'payment_method_2',
                'payment_amount_2', 
                'transaction_notes'
            ]
            
            for key in keys_to_clear:
                if key in st.session_state:
                    st.session_state.pop(key, None)
                
            st.session_state['page'] = 'scheduled_services'
            st.rerun()

if __name__ == "__main__":
    transaction_details_page()