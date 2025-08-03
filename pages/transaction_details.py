"""
NEW Transaction Details Page - Built from scratch
Displays complete service transaction information with accurate pricing
"""

import streamlit as st
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from database.connection import SnowflakeConnection
from utils.formatting import format_currency, format_date, format_time
from utils.null_handling import safe_get_float, safe_get_int, safe_get_string, safe_get_bool

def get_transaction_details(transaction_id: int) -> Optional[Dict[str, Any]]:
    """Get complete transaction details from database"""
    conn = SnowflakeConnection.get_instance()
    
    query = """
    SELECT 
        -- Transaction core data
        t.ID as TRANSACTION_ID,
        t.SERVICE_NAME as PRIMARY_SERVICE_NAME,
        t.SERVICE_ID as PRIMARY_SERVICE_ID,
        t.SERVICE2_ID,
        t.SERVICE3_ID,
        t.BASE_SERVICE_COST,
        t.AMOUNT as TOTAL_AMOUNT,
        t.STATUS,
        t.COMMENTS,
        t.SERVICE_DATE,
        t.START_TIME,
        t.END_TIME,
        t.DEPOSIT,
        t.DEPOSIT_PAID,
        t.MATERIAL_COST,
        t.TOTAL_LABOR_COST,
        t.PRICING_STRATEGY,
        t.MARKUP_PERCENTAGE,
        t.PRICE_ADJUSTMENTS_JSON,
        t.IS_RECURRING,
        t.RECURRENCE_PATTERN,
        t.CREATED_DATE,
        
        -- Customer information
        t.CUSTOMER_ID,
        c.FIRST_NAME as CUSTOMER_FIRST_NAME,
        c.LAST_NAME as CUSTOMER_LAST_NAME,
        c.EMAIL_ADDRESS as CUSTOMER_EMAIL,
        c.PHONE_NUMBER as CUSTOMER_PHONE,
        
        -- Account information (if applicable)
        t.ACCOUNT_ID,
        a.ACCOUNT_NAME,
        
        -- Primary service details from SERVICES table
        s1.SERVICE_NAME as PRIMARY_SERVICE_TABLE_NAME,
        s1.COST as PRIMARY_SERVICE_TABLE_COST,
        s1.SERVICE_DURATION as PRIMARY_SERVICE_DURATION,
        s1.SERVICE_CATEGORY as PRIMARY_SERVICE_CATEGORY,
        
        -- Additional service 2 details
        s2.SERVICE_NAME as SERVICE2_NAME,
        s2.COST as SERVICE2_COST,
        s2.SERVICE_DURATION as SERVICE2_DURATION,
        s2.SERVICE_CATEGORY as SERVICE2_CATEGORY,
        
        -- Additional service 3 details
        s3.SERVICE_NAME as SERVICE3_NAME,
        s3.COST as SERVICE3_COST,
        s3.SERVICE_DURATION as SERVICE3_DURATION,
        s3.SERVICE_CATEGORY as SERVICE3_CATEGORY,
        
        -- Service address
        sa.STREET_ADDRESS,
        sa.CITY,
        sa.STATE,
        sa.ZIP_CODE,
        sa.SQUARE_FOOTAGE
        
    FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION t
    LEFT JOIN OPERATIONAL.CARPET.CUSTOMER c ON t.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN OPERATIONAL.CARPET.ACCOUNTS a ON t.ACCOUNT_ID = a.ACCOUNT_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES s1 ON t.SERVICE_ID = s1.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES s2 ON t.SERVICE2_ID = s2.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES s3 ON t.SERVICE3_ID = s3.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICE_ADDRESSES sa ON 
        (t.CUSTOMER_ID = sa.CUSTOMER_ID OR t.ACCOUNT_ID = sa.ACCOUNT_ID)
        AND sa.IS_PRIMARY_SERVICE = TRUE
    WHERE t.ID = ?
    """
    
    try:
        result = conn.execute_query(query, [transaction_id])
        if result:
            return dict(result[0])
        return None
    except Exception as e:
        st.error(f"Error loading transaction details: {str(e)}")
        return None

def display_transaction_header(transaction: Dict[str, Any]) -> None:
    """Display transaction header with key information"""
    
    # Customer/Account name
    if transaction.get('CUSTOMER_FIRST_NAME'):
        customer_name = f"{transaction['CUSTOMER_FIRST_NAME']} {transaction['CUSTOMER_LAST_NAME']}"
    elif transaction.get('ACCOUNT_NAME'):
        customer_name = transaction['ACCOUNT_NAME']
    else:
        customer_name = "Unknown Customer"
    
    # Primary service name (prefer transaction data, fallback to service table)
    service_name = transaction.get('PRIMARY_SERVICE_NAME') or transaction.get('PRIMARY_SERVICE_TABLE_NAME') or "Unknown Service"
    
    # Status badge
    status = transaction.get('STATUS', 'UNKNOWN')
    status_colors = {
        'SCHEDULED': '🟡',
        'IN_PROGRESS': '🔵', 
        'COMPLETED': '🟢',
        'CANCELLED': '🔴'
    }
    status_icon = status_colors.get(status, '⚪')
    
    st.markdown(f"## {status_icon} {service_name}")
    st.markdown(f"**Customer:** {customer_name}")
    st.markdown(f"**Status:** {status}")
    
    # Service address
    address_parts = [
        transaction.get('STREET_ADDRESS'),
        transaction.get('CITY'),
        transaction.get('STATE'),
        str(transaction.get('ZIP_CODE')) if transaction.get('ZIP_CODE') else None
    ]
    address = ', '.join(filter(None, address_parts))
    if address:
        st.markdown(f"**Service Address:** {address}")
    
    # Service date and time
    service_date = transaction.get('SERVICE_DATE')
    start_time = transaction.get('START_TIME')
    if service_date:
        st.markdown(f"**Service Date:** {format_date(service_date)}")
    if start_time:
        st.markdown(f"**Service Time:** {format_time(start_time)}")
    
    # Comments
    comments = transaction.get('COMMENTS')
    if comments:
        st.markdown(f"**Notes:** {comments}")

def display_service_breakdown(transaction: Dict[str, Any]) -> float:
    """Display detailed service breakdown and return total cost"""
    
    st.markdown("### 🛠️ Service Breakdown")
    
    total_cost = 0.0
    
    # Primary Service
    primary_service_name = transaction.get('PRIMARY_SERVICE_NAME') or transaction.get('PRIMARY_SERVICE_TABLE_NAME') or "Unknown Service"
    
    # Use BASE_SERVICE_COST from transaction as primary source
    primary_cost = safe_get_float(transaction.get('BASE_SERVICE_COST', 0))
    
    # If no BASE_SERVICE_COST, fallback to service table cost
    if primary_cost <= 0:
        primary_cost = safe_get_float(transaction.get('PRIMARY_SERVICE_TABLE_COST', 0))
    
    with st.container():
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown(f"**Primary Service:** {primary_service_name}")
            category = transaction.get('PRIMARY_SERVICE_CATEGORY')
            if category:
                st.markdown(f"*Category: {category}*")
        with col2:
            st.markdown(f"**${primary_cost:.2f}**")
    
    total_cost += primary_cost
    
    # Additional Service 2
    if transaction.get('SERVICE2_ID') and transaction.get('SERVICE2_NAME'):
        service2_cost = safe_get_float(transaction.get('SERVICE2_COST', 0))
        
        with st.container():
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.markdown(f"**Additional Service 1:** {transaction['SERVICE2_NAME']}")
                category = transaction.get('SERVICE2_CATEGORY')
                if category:
                    st.markdown(f"*Category: {category}*")
            with col2:
                st.markdown(f"**${service2_cost:.2f}**")
            with col3:
                if st.button("❌ Remove", key="remove_service2", help="Remove this service"):
                    if remove_additional_service(transaction['TRANSACTION_ID'], 'SERVICE2_ID'):
                        st.rerun()
        
        total_cost += service2_cost
    
    # Additional Service 3
    if transaction.get('SERVICE3_ID') and transaction.get('SERVICE3_NAME'):
        service3_cost = safe_get_float(transaction.get('SERVICE3_COST', 0))
        
        with st.container():
            col1, col2, col3 = st.columns([3, 1, 1])
            with col1:
                st.markdown(f"**Additional Service 2:** {transaction['SERVICE3_NAME']}")
                category = transaction.get('SERVICE3_CATEGORY')
                if category:
                    st.markdown(f"*Category: {category}*")
            with col2:
                st.markdown(f"**${service3_cost:.2f}**")
            with col3:
                if st.button("❌ Remove", key="remove_service3", help="Remove this service"):
                    if remove_additional_service(transaction['TRANSACTION_ID'], 'SERVICE3_ID'):
                        st.rerun()
        
        total_cost += service3_cost
    
    # Cost Summary
    st.markdown("---")
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("**Subtotal (Services):**")
    with col2:
        st.markdown(f"**${total_cost:.2f}**")
    
    # Material costs if any
    material_cost = safe_get_float(transaction.get('MATERIAL_COST', 0))
    if material_cost > 0:
        with st.columns([3, 1]) as cols:
            cols[0].markdown("**Materials:**")
            cols[1].markdown(f"**${material_cost:.2f}**")
        total_cost += material_cost
    
    # Labor costs if separate
    labor_cost = safe_get_float(transaction.get('TOTAL_LABOR_COST', 0))
    if labor_cost > 0 and labor_cost != total_cost:
        with st.columns([3, 1]) as cols:
            cols[0].markdown("**Labor:**")
            cols[1].markdown(f"**${labor_cost:.2f}**")
    
    # Final total
    final_amount = safe_get_float(transaction.get('TOTAL_AMOUNT', total_cost))
    
    st.markdown("---")
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("### **Total Amount:**")
    with col2:
        st.markdown(f"### **${final_amount:.2f}**")
    
    return final_amount

def display_payment_information(transaction: Dict[str, Any]) -> None:
    """Display payment and deposit information"""
    
    st.markdown("### 💳 Payment Information")
    
    total_amount = safe_get_float(transaction.get('TOTAL_AMOUNT', 0))
    deposit = safe_get_float(transaction.get('DEPOSIT', 0))
    deposit_paid = safe_get_bool(transaction.get('DEPOSIT_PAID', False))
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown(f"**Total Amount:** ${total_amount:.2f}")
        if deposit > 0:
            st.markdown(f"**Deposit Required:** ${deposit:.2f}")
            status = "✅ Paid" if deposit_paid else "❌ Pending"
            st.markdown(f"**Deposit Status:** {status}")
        
        balance = total_amount - (deposit if deposit_paid else 0)
        if balance > 0:
            st.markdown(f"**Remaining Balance:** ${balance:.2f}")
    
    with col2:
        # Payment actions
        if not deposit_paid and deposit > 0:
            if st.button("Mark Deposit as Paid", type="primary"):
                if mark_deposit_paid(transaction['TRANSACTION_ID']):
                    st.success("Deposit marked as paid!")
                    st.rerun()

def display_employee_assignment(transaction: Dict[str, Any]) -> None:
    """Display employee assignment section"""
    
    st.markdown("### 👷 Employee Assignment")
    
    # Get available employees
    conn = SnowflakeConnection.get_instance()
    employees_query = """
    SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, JOB_TITLE
    FROM OPERATIONAL.CARPET.EMPLOYEE
    WHERE ACTIVE_STATUS = TRUE
    ORDER BY FIRST_NAME, LAST_NAME
    """
    
    try:
        employees = conn.execute_query(employees_query)
        if employees:
            employee_options = {f"{emp['FIRST_NAME']} {emp['LAST_NAME']} ({emp['JOB_TITLE']})": emp['EMPLOYEE_ID'] 
                              for emp in employees}
            
            selected_employee = st.selectbox(
                "Assign Employee",
                options=["None"] + list(employee_options.keys()),
                help="Select an employee to assign to this service"
            )
            
            if selected_employee != "None":
                st.button("Assign Employee", type="secondary")
        else:
            st.info("No employees available for assignment")
    except Exception as e:
        st.error(f"Error loading employees: {str(e)}")

def display_service_actions(transaction: Dict[str, Any]) -> None:
    """Display service action buttons based on status"""
    
    st.markdown("### ⚡ Service Actions")
    
    status = transaction.get('STATUS', '')
    transaction_id = transaction['TRANSACTION_ID']
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if status == 'SCHEDULED':
            if st.button("🚀 Start Service", type="primary", use_container_width=True):
                if update_service_status(transaction_id, 'IN_PROGRESS'):
                    st.success("Service started!")
                    st.rerun()
    
    with col2:
        if status == 'IN_PROGRESS':
            if st.button("✅ Complete Service", type="primary", use_container_width=True):
                if update_service_status(transaction_id, 'COMPLETED'):
                    st.success("Service completed!")
                    st.rerun()
    
    with col3:
        if status in ['SCHEDULED', 'IN_PROGRESS']:
            if st.button("❌ Cancel Service", type="secondary", use_container_width=True):
                if st.session_state.get('confirm_cancel'):
                    if update_service_status(transaction_id, 'CANCELLED'):
                        st.success("Service cancelled!")
                        st.rerun()
                else:
                    st.session_state.confirm_cancel = True
                    st.warning("Click again to confirm cancellation")
                    st.rerun()
    
    with col4:
        if st.button("📧 Send Update", type="secondary", use_container_width=True):
            send_customer_update(transaction)

def display_debug_information(transaction: Dict[str, Any]) -> None:
    """Display debug information if debug mode is enabled"""
    
    if st.session_state.get('debug_mode', False):
        st.markdown("### 🐛 Debug Information")
        
        with st.expander("Raw Transaction Data"):
            st.json({k: str(v) for k, v in transaction.items()})
        
        with st.expander("Pricing Analysis"):
            st.write("BASE_SERVICE_COST:", transaction.get('BASE_SERVICE_COST'))
            st.write("TOTAL_AMOUNT:", transaction.get('TOTAL_AMOUNT'))
            st.write("PRIMARY_SERVICE_TABLE_COST:", transaction.get('PRIMARY_SERVICE_TABLE_COST'))
            st.write("SERVICE2_COST:", transaction.get('SERVICE2_COST'))
            st.write("SERVICE3_COST:", transaction.get('SERVICE3_COST'))

# Helper functions
def remove_additional_service(transaction_id: int, service_field: str) -> bool:
    """Remove an additional service from the transaction"""
    conn = SnowflakeConnection.get_instance()
    
    query = f"""
    UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
    SET {service_field} = NULL,
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE ID = ?
    """
    
    try:
        conn.execute_query(query, [transaction_id])
        return True
    except Exception as e:
        st.error(f"Error removing service: {str(e)}")
        return False

def mark_deposit_paid(transaction_id: int) -> bool:
    """Mark deposit as paid"""
    conn = SnowflakeConnection.get_instance()
    
    query = """
    UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
    SET DEPOSIT_PAID = TRUE,
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE ID = ?
    """
    
    try:
        conn.execute_query(query, [transaction_id])
        return True
    except Exception as e:
        st.error(f"Error updating deposit status: {str(e)}")
        return False

def update_service_status(transaction_id: int, new_status: str) -> bool:
    """Update service status"""
    conn = SnowflakeConnection.get_instance()
    
    query = """
    UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
    SET STATUS = ?,
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE ID = ?
    """
    
    try:
        conn.execute_query(query, [new_status, transaction_id])
        return True
    except Exception as e:
        st.error(f"Error updating service status: {str(e)}")
        return False

def send_customer_update(transaction: Dict[str, Any]) -> None:
    """Send customer update (placeholder)"""
    st.info("Customer update functionality would be implemented here")

def transaction_details_page():
    """Main transaction details page"""
    
    st.title("📋 Service Transaction Details")
    
    # Get transaction ID from session state
    selected_service = st.session_state.get('selected_service')
    if not selected_service:
        st.error("No service selected. Please select a service from scheduled services.")
        if st.button("← Back to Scheduled Services"):
            st.session_state.page = 'scheduled'
            st.rerun()
        return
    
    transaction_id = safe_get_int(selected_service.get('TRANSACTION_ID'))
    if not transaction_id:
        st.error("Could not determine transaction ID. Please try selecting the service again.")
        if st.button("← Back to Scheduled Services"):
            st.session_state.page = 'scheduled'
            st.rerun()
        return
    
    # Load transaction details
    transaction = get_transaction_details(transaction_id)
    if not transaction:
        st.error("Could not load transaction details.")
        if st.button("← Back to Scheduled Services"):
            st.session_state.page = 'scheduled'
            st.rerun()
        return
    
    # Navigation
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("← Back", use_container_width=True):
            st.session_state.page = 'scheduled'
            st.rerun()
    
    # Display all sections
    display_transaction_header(transaction)
    
    st.markdown("---")
    total_cost = display_service_breakdown(transaction)
    
    st.markdown("---")
    display_payment_information(transaction)
    
    st.markdown("---")
    display_employee_assignment(transaction)
    
    st.markdown("---")
    display_service_actions(transaction)
    
    # Debug information (if enabled)
    display_debug_information(transaction)

if __name__ == "__main__":
    new_transaction_details_page()