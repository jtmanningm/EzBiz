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
    """Display detailed service breakdown with editing capabilities"""
    
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("### 🛠️ Service Breakdown")
    with col2:
        if st.button("➕ Add Service", type="secondary", use_container_width=True):
            st.session_state.show_add_service = True
    
    # Show add service dialog if requested
    if st.session_state.get('show_add_service', False):
        display_add_service_dialog(transaction)
    
    total_cost = 0.0
    
    # Primary Service with price editing
    primary_service_name = transaction.get('PRIMARY_SERVICE_NAME') or transaction.get('PRIMARY_SERVICE_TABLE_NAME') or "Unknown Service"
    primary_cost = safe_get_float(transaction.get('BASE_SERVICE_COST', 0))
    
    if primary_cost <= 0:
        primary_cost = safe_get_float(transaction.get('PRIMARY_SERVICE_TABLE_COST', 0))
    
    st.markdown("#### Primary Service")
    with st.container():
        col1, col2, col3, col4 = st.columns([2, 1, 1, 0.5])
        with col1:
            st.markdown(f"**{primary_service_name}**")
            category = transaction.get('PRIMARY_SERVICE_CATEGORY')
            if category:
                st.markdown(f"*{category}*")
        
        with col2:
            # Editable price for primary service
            new_primary_cost = st.number_input(
                "Price",
                value=float(primary_cost),
                min_value=0.0,
                step=0.01,
                key="primary_cost_edit",
                label_visibility="collapsed"
            )
            if new_primary_cost != primary_cost:
                if st.button("💾", key="save_primary_cost", help="Save price change"):
                    if update_service_cost(transaction['TRANSACTION_ID'], 'BASE_SERVICE_COST', new_primary_cost):
                        st.success("Price updated!")
                        st.rerun()
        
        with col3:
            # Employee assignment for primary service
            if st.button("👷 Assign", key="assign_primary", help="Assign employees", use_container_width=True):
                st.session_state.show_employee_assign = f"primary_{transaction['PRIMARY_SERVICE_ID']}"
        
        with col4:
            st.markdown("🔒", help="Primary service cannot be removed")
    
    total_cost += new_primary_cost if 'new_primary_cost' in locals() and new_primary_cost != primary_cost else primary_cost
    
    # Additional Service 2
    if transaction.get('SERVICE2_ID') and transaction.get('SERVICE2_NAME'):
        service2_cost = safe_get_float(transaction.get('SERVICE2_COST', 0))
        
        st.markdown("#### Additional Service 1")
        with st.container():
            col1, col2, col3, col4 = st.columns([2, 1, 1, 0.5])
            with col1:
                st.markdown(f"**{transaction['SERVICE2_NAME']}**")
                category = transaction.get('SERVICE2_CATEGORY')
                if category:
                    st.markdown(f"*{category}*")
            
            with col2:
                # Editable price for service 2
                new_service2_cost = st.number_input(
                    "Price",
                    value=float(service2_cost),
                    min_value=0.0,
                    step=0.01,
                    key="service2_cost_edit",
                    label_visibility="collapsed"
                )
                if new_service2_cost != service2_cost:
                    if st.button("💾", key="save_service2_cost", help="Save price change"):
                        if update_additional_service_cost(transaction['TRANSACTION_ID'], 'SERVICE2_ID', new_service2_cost):
                            st.success("Price updated!")
                            st.rerun()
            
            with col3:
                # Employee assignment for service 2
                if st.button("👷 Assign", key="assign_service2", help="Assign employees", use_container_width=True):
                    st.session_state.show_employee_assign = f"service2_{transaction['SERVICE2_ID']}"
            
            with col4:
                if st.button("❌", key="remove_service2", help="Remove this service"):
                    if st.session_state.get('confirm_remove_service2'):
                        if remove_additional_service(transaction['TRANSACTION_ID'], 'SERVICE2_ID'):
                            st.success("Service removed!")
                            st.rerun()
                    else:
                        st.session_state.confirm_remove_service2 = True
                        st.warning("Click again to confirm")
                        st.rerun()
        
        total_cost += new_service2_cost if 'new_service2_cost' in locals() and new_service2_cost != service2_cost else service2_cost
    
    # Additional Service 3
    if transaction.get('SERVICE3_ID') and transaction.get('SERVICE3_NAME'):
        service3_cost = safe_get_float(transaction.get('SERVICE3_COST', 0))
        
        st.markdown("#### Additional Service 2")
        with st.container():
            col1, col2, col3, col4 = st.columns([2, 1, 1, 0.5])
            with col1:
                st.markdown(f"**{transaction['SERVICE3_NAME']}**")
                category = transaction.get('SERVICE3_CATEGORY')
                if category:
                    st.markdown(f"*{category}*")
            
            with col2:
                # Editable price for service 3
                new_service3_cost = st.number_input(
                    "Price",
                    value=float(service3_cost),
                    min_value=0.0,
                    step=0.01,
                    key="service3_cost_edit",
                    label_visibility="collapsed"
                )
                if new_service3_cost != service3_cost:
                    if st.button("💾", key="save_service3_cost", help="Save price change"):
                        if update_additional_service_cost(transaction['TRANSACTION_ID'], 'SERVICE3_ID', new_service3_cost):
                            st.success("Price updated!")
                            st.rerun()
            
            with col3:
                # Employee assignment for service 3
                if st.button("👷 Assign", key="assign_service3", help="Assign employees", use_container_width=True):
                    st.session_state.show_employee_assign = f"service3_{transaction['SERVICE3_ID']}"
            
            with col4:
                if st.button("❌", key="remove_service3", help="Remove this service"):
                    if st.session_state.get('confirm_remove_service3'):
                        if remove_additional_service(transaction['TRANSACTION_ID'], 'SERVICE3_ID'):
                            st.success("Service removed!")
                            st.rerun()
                    else:
                        st.session_state.confirm_remove_service3 = True
                        st.warning("Click again to confirm")
                        st.rerun()
        
        total_cost += new_service3_cost if 'new_service3_cost' in locals() and new_service3_cost != service3_cost else service3_cost
    
    # Show employee assignment dialog if requested
    if st.session_state.get('show_employee_assign'):
        display_employee_assignment_dialog(transaction)
    
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
    """Display employee assignment overview section"""
    
    st.markdown("### 👷 Employee Assignments")
    
    # Get all current assignments for this transaction
    conn = SnowflakeConnection.get_instance()
    assignments_query = """
    SELECT 
        sa.ASSIGNMENT_ID,
        sa.NOTES,
        sa.ASSIGNMENT_STATUS,
        e.FIRST_NAME,
        e.LAST_NAME,
        e.JOB_TITLE
    FROM OPERATIONAL.CARPET.SERVICE_ASSIGNMENTS sa
    JOIN OPERATIONAL.CARPET.EMPLOYEE e ON sa.EMPLOYEE_ID = e.EMPLOYEE_ID
    WHERE sa.TRANSACTION_ID = ?
    ORDER BY e.FIRST_NAME, e.LAST_NAME
    """
    
    try:
        assignments = conn.execute_query(assignments_query, [transaction['TRANSACTION_ID']])
        
        if assignments:
            st.markdown("**Current Assignments:**")
            
            # Display all assignments for this transaction
            for assignment in assignments:
                col1, col2 = st.columns([3, 1])
                with col1:
                    employee_name = f"{assignment['FIRST_NAME']} {assignment['LAST_NAME']}"
                    st.markdown(f"  • {employee_name} ({assignment['JOB_TITLE']})")
                    if assignment.get('NOTES'):
                        st.markdown(f"    *Notes: {assignment['NOTES']}*")
                with col2:
                    if st.button("Remove", key=f"remove_assignment_{assignment['ASSIGNMENT_ID']}", 
                               type="secondary", use_container_width=True):
                        if remove_employee_assignment(assignment['ASSIGNMENT_ID']):
                            st.success("Assignment removed!")
                            st.rerun()
        else:
            st.info("No employees currently assigned to this transaction")
    except Exception as e:
        st.error(f"Error loading employee assignments: {str(e)}")

def display_service_actions(transaction: Dict[str, Any]) -> None:
    """Display service action buttons based on status"""
    
    st.markdown("### ⚡ Service Actions")
    
    status = transaction.get('STATUS', '')
    transaction_id = transaction['TRANSACTION_ID']
    
    # Use 5 columns to fit the reset option
    col1, col2, col3, col4, col5 = st.columns(5)
    
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
                    st.success("Service completed! Redirecting to home page...")
                    # Clear the selected service from session state
                    if 'selected_service' in st.session_state:
                        del st.session_state.selected_service
                    # Route to home page
                    st.session_state.page = 'home'
                    st.rerun()
    
    with col3:
        if status in ['SCHEDULED', 'IN_PROGRESS']:
            if st.button("❌ Cancel Service", type="secondary", use_container_width=True):
                if st.session_state.get('confirm_cancel'):
                    if update_service_status(transaction_id, 'CANCELLED'):
                        st.success("Service cancelled! Redirecting to home page...")
                        # Clear the selected service from session state
                        if 'selected_service' in st.session_state:
                            del st.session_state.selected_service
                        # Clear the confirmation flag
                        if 'confirm_cancel' in st.session_state:
                            del st.session_state.confirm_cancel
                        # Route to home page
                        st.session_state.page = 'home'
                        st.rerun()
                else:
                    st.session_state.confirm_cancel = True
                    st.warning("Click again to confirm cancellation")
                    st.rerun()
    
    with col4:
        # Reset service option - available for completed, cancelled, or in-progress services
        if status in ['COMPLETED', 'CANCELLED', 'IN_PROGRESS']:
            if st.button("🔄 Reset Service", type="secondary", use_container_width=True):
                if st.session_state.get('confirm_reset'):
                    if reset_service_status(transaction_id):
                        st.success("Service reset to scheduled status!")
                        # Clear confirmation flag
                        if 'confirm_reset' in st.session_state:
                            del st.session_state.confirm_reset
                        st.rerun()
                else:
                    st.session_state.confirm_reset = True
                    st.warning("Click again to confirm reset to SCHEDULED")
                    st.rerun()
    
    with col5:
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

# Service Management Functions
def display_add_service_dialog(transaction: Dict[str, Any]) -> None:
    """Display dialog for adding a new service"""
    
    st.markdown("### ➕ Add New Service")
    
    with st.container():
        # Get available services
        conn = SnowflakeConnection.get_instance()
        services_query = """
        SELECT SERVICE_ID, SERVICE_NAME, COST, SERVICE_CATEGORY, SERVICE_DURATION
        FROM OPERATIONAL.CARPET.SERVICES
        WHERE ACTIVE_STATUS = TRUE
        ORDER BY SERVICE_CATEGORY, SERVICE_NAME
        """
        
        try:
            services = conn.execute_query(services_query)
            if services:
                # Create service options grouped by category
                service_options = {}
                for service in services:
                    category = service['SERVICE_CATEGORY'] or 'Other'
                    if category not in service_options:
                        service_options[category] = []
                    service_options[category].append({
                        'id': service['SERVICE_ID'],
                        'name': service['SERVICE_NAME'],
                        'cost': service['COST'],
                        'duration': service['SERVICE_DURATION']
                    })
                
                col1, col2 = st.columns(2)
                
                with col1:
                    # Category selection
                    selected_category = st.selectbox(
                        "Service Category",
                        options=list(service_options.keys()),
                        key="add_service_category"
                    )
                
                with col2:
                    # Service selection within category
                    if selected_category and selected_category in service_options:
                        service_names = [f"{s['name']} (${s['cost']:.2f})" for s in service_options[selected_category]]
                        selected_service_idx = st.selectbox(
                            "Select Service",
                            options=range(len(service_names)),
                            format_func=lambda x: service_names[x],
                            key="add_service_selection"
                        )
                        
                        if selected_service_idx is not None:
                            selected_service = service_options[selected_category][selected_service_idx]
                            
                            # Price adjustment
                            adjusted_price = st.number_input(
                                "Service Price",
                                value=float(selected_service['cost']),
                                min_value=0.0,
                                step=0.01,
                                key="add_service_price"
                            )
                            
                            # Add service buttons
                            col_add, col_cancel = st.columns(2)
                            with col_add:
                                if st.button("✅ Add Service", type="primary", use_container_width=True):
                                    if add_service_to_transaction(
                                        transaction['TRANSACTION_ID'],
                                        selected_service['id'],
                                        adjusted_price
                                    ):
                                        st.success(f"Added {selected_service['name']}!")
                                        st.session_state.show_add_service = False
                                        st.rerun()
                            
                            with col_cancel:
                                if st.button("❌ Cancel", use_container_width=True):
                                    st.session_state.show_add_service = False
                                    st.rerun()
            else:
                st.error("No services available")
                
        except Exception as e:
            st.error(f"Error loading services: {str(e)}")

def display_employee_assignment_dialog(transaction: Dict[str, Any]) -> None:
    """Display dialog for assigning employees to services"""
    
    service_key = st.session_state.get('show_employee_assign', '')
    if not service_key:
        return
    
    # Parse service key to get service info
    if service_key.startswith('primary_'):
        service_id = service_key.replace('primary_', '')
        service_name = transaction.get('PRIMARY_SERVICE_NAME', 'Primary Service')
    elif service_key.startswith('service2_'):
        service_id = service_key.replace('service2_', '')
        service_name = transaction.get('SERVICE2_NAME', 'Additional Service 1')
    elif service_key.startswith('service3_'):
        service_id = service_key.replace('service3_', '')
        service_name = transaction.get('SERVICE3_NAME', 'Additional Service 2')
    else:
        return
    
    st.markdown(f"### 👷 Assign Employees to {service_name}")
    
    # Get available employees
    conn = SnowflakeConnection.get_instance()
    employees_query = """
    SELECT EMPLOYEE_ID, FIRST_NAME, LAST_NAME, JOB_TITLE, HOURLY_RATE
    FROM OPERATIONAL.CARPET.EMPLOYEE
    WHERE ACTIVE_STATUS = TRUE
    ORDER BY FIRST_NAME, LAST_NAME
    """
    
    # Get currently assigned employees
    assignments_query = """
    SELECT e.EMPLOYEE_ID, e.FIRST_NAME, e.LAST_NAME, e.JOB_TITLE,
           sa.ASSIGNMENT_ID, sa.NOTES
    FROM OPERATIONAL.CARPET.SERVICE_ASSIGNMENTS sa
    JOIN OPERATIONAL.CARPET.EMPLOYEE e ON sa.EMPLOYEE_ID = e.EMPLOYEE_ID
    WHERE sa.TRANSACTION_ID = ?
    """
    
    try:
        employees = conn.execute_query(employees_query)
        current_assignments = conn.execute_query(assignments_query, [transaction['TRANSACTION_ID']])
        
        if employees:
            assigned_employee_ids = [a['EMPLOYEE_ID'] for a in current_assignments] if current_assignments else []
            
            # Current assignments
            if current_assignments:
                st.markdown("**Currently Assigned:**")
                for assignment in current_assignments:
                    col1, col2, col3 = st.columns([2, 1, 0.5])
                    with col1:
                        st.markdown(f"• {assignment['FIRST_NAME']} {assignment['LAST_NAME']} ({assignment['JOB_TITLE']})")
                    with col2:
                        notes = assignment.get('NOTES', '')
                        if notes:
                            st.markdown(f"*{notes}*")
                    with col3:
                        if st.button("🗑️", key=f"remove_assign_{assignment['ASSIGNMENT_ID']}", help="Remove assignment"):
                            if remove_employee_assignment(assignment['ASSIGNMENT_ID']):
                                st.success("Assignment removed!")
                                st.rerun()
                
                st.markdown("---")
            
            # Add new assignment
            st.markdown("**Add New Assignment:**")
            available_employees = [emp for emp in employees if emp['EMPLOYEE_ID'] not in assigned_employee_ids]
            
            if available_employees:
                col1, col2 = st.columns(2)
                
                with col1:
                    employee_options = {f"{emp['FIRST_NAME']} {emp['LAST_NAME']} ({emp['JOB_TITLE']})": emp for emp in available_employees}
                    selected_employee_name = st.selectbox(
                        "Select Employee",
                        options=list(employee_options.keys()),
                        key="assign_employee_select"
                    )
                
                with col2:
                    if selected_employee_name:
                        selected_employee = employee_options[selected_employee_name]
                        hourly_rate = st.number_input(
                            "Hourly Rate Override",
                            value=float(selected_employee.get('HOURLY_RATE', 25.0)),
                            min_value=0.0,
                            step=0.25,
                            key="assign_hourly_rate",
                            help="Leave as default or override for this service"
                        )
                
                # Assignment buttons
                col_assign, col_close = st.columns(2)
                with col_assign:
                    if st.button("✅ Assign Employee", type="primary", use_container_width=True):
                        if selected_employee_name:
                            if assign_employee_to_service(
                                transaction['TRANSACTION_ID'],
                                service_id,
                                selected_employee['EMPLOYEE_ID'],
                                hourly_rate
                            ):
                                st.success(f"Assigned {selected_employee_name}!")
                                st.rerun()
                
                with col_close:
                    if st.button("❌ Close", use_container_width=True):
                        st.session_state.show_employee_assign = None
                        st.rerun()
            else:
                st.info("All available employees are already assigned to this service")
                if st.button("❌ Close", use_container_width=True):
                    st.session_state.show_employee_assign = None
                    st.rerun()
        else:
            st.error("No employees available")
            
    except Exception as e:
        st.error(f"Error loading employee data: {str(e)}")

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

def add_service_to_transaction(transaction_id: int, service_id: int, service_cost: float) -> bool:
    """Add a new service to the transaction"""
    conn = SnowflakeConnection.get_instance()
    
    # First check which service slot is available
    check_query = """
    SELECT SERVICE2_ID, SERVICE3_ID
    FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION
    WHERE ID = ?
    """
    
    try:
        result = conn.execute_query(check_query, [transaction_id])
        if not result:
            st.error("Transaction not found")
            return False
        
        transaction_data = result[0]
        
        # Determine which slot to use
        if not transaction_data['SERVICE2_ID']:
            service_field = 'SERVICE2_ID'
        elif not transaction_data['SERVICE3_ID']:
            service_field = 'SERVICE3_ID'
        else:
            st.error("Cannot add more services - maximum of 3 services per transaction")
            return False
        
        # Add the service
        update_query = f"""
        UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
        SET {service_field} = ?,
            LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
        WHERE ID = ?
        """
        
        conn.execute_query(update_query, [service_id, transaction_id])
        
        # Update the service cost in the appropriate cost field
        cost_field = 'SERVICE2_COST' if service_field == 'SERVICE2_ID' else 'SERVICE3_COST'
        cost_query = f"""
        UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
        SET {cost_field} = (
            SELECT COST FROM OPERATIONAL.CARPET.SERVICES WHERE SERVICE_ID = ?
        ),
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
        WHERE ID = ?
        """
        
        conn.execute_query(cost_query, [service_id, transaction_id])
        
        # Recalculate total amount
        recalculate_transaction_total(transaction_id)
        
        return True
        
    except Exception as e:
        st.error(f"Error adding service: {str(e)}")
        return False

def update_service_cost(transaction_id: int, cost_field: str, new_cost: float) -> bool:
    """Update the cost of a service in the transaction"""
    conn = SnowflakeConnection.get_instance()
    
    query = f"""
    UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
    SET {cost_field} = ?,
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE ID = ?
    """
    
    try:
        conn.execute_query(query, [new_cost, transaction_id])
        recalculate_transaction_total(transaction_id)
        return True
    except Exception as e:
        st.error(f"Error updating service cost: {str(e)}")
        return False

def update_additional_service_cost(transaction_id: int, service_field: str, new_cost: float) -> bool:
    """Update the cost of an additional service"""
    cost_field = 'SERVICE2_COST' if service_field == 'SERVICE2_ID' else 'SERVICE3_COST'
    return update_service_cost(transaction_id, cost_field, new_cost)

def recalculate_transaction_total(transaction_id: int) -> bool:
    """Recalculate the total amount for a transaction"""
    conn = SnowflakeConnection.get_instance()
    
    query = """
    UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
    SET AMOUNT = COALESCE(BASE_SERVICE_COST, 0) + 
                 COALESCE(SERVICE2_COST, 0) + 
                 COALESCE(SERVICE3_COST, 0) + 
                 COALESCE(MATERIAL_COST, 0),
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE ID = ?
    """
    
    try:
        conn.execute_query(query, [transaction_id])
        return True
    except Exception as e:
        st.error(f"Error recalculating total: {str(e)}")
        return False

def assign_employee_to_service(transaction_id: int, service_id: int, employee_id: int, hourly_rate: float) -> bool:
    """Assign an employee to a specific service in a transaction"""
    conn = SnowflakeConnection.get_instance()
    
    # Note: service_id and hourly_rate parameters are kept for API compatibility
    # but not stored in the current table structure
    query = """
    INSERT INTO OPERATIONAL.CARPET.SERVICE_ASSIGNMENTS (
        TRANSACTION_ID, EMPLOYEE_ID, ASSIGNMENT_STATUS, NOTES
    )
    VALUES (?, ?, 'ASSIGNED', ?)
    """
    
    notes = f"Service ID: {service_id}, Hourly Rate: ${hourly_rate:.2f}"
    
    try:
        conn.execute_query(query, [transaction_id, employee_id, notes])
        return True
    except Exception as e:
        st.error(f"Error assigning employee: {str(e)}")
        return False

def remove_employee_assignment(assignment_id: int) -> bool:
    """Remove an employee assignment"""
    conn = SnowflakeConnection.get_instance()
    
    query = """
    DELETE FROM OPERATIONAL.CARPET.SERVICE_ASSIGNMENTS
    WHERE ASSIGNMENT_ID = ?
    """
    
    try:
        conn.execute_query(query, [assignment_id])
        return True
    except Exception as e:
        st.error(f"Error removing assignment: {str(e)}")
        return False

def update_service_status(transaction_id: int, new_status: str) -> bool:
    """Update the status of a service transaction"""
    conn = SnowflakeConnection.get_instance()
    
    query = """
    UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
    SET STATUS = ?,
        MODIFIED_AT = CURRENT_TIMESTAMP()
    WHERE ID = ?
    """
    
    try:
        conn.execute_query(query, [new_status, transaction_id])
        return True
    except Exception as e:
        st.error(f"Error updating service status: {str(e)}")
        return False

def reset_service_status(transaction_id: int) -> bool:
    """Reset service status back to SCHEDULED and clear completion data"""
    conn = SnowflakeConnection.get_instance()
    
    # Reset status to SCHEDULED and clear any completion timestamps or data
    query = """
    UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
    SET STATUS = 'SCHEDULED',
        MODIFIED_AT = CURRENT_TIMESTAMP()
    WHERE ID = ?
    """
    
    try:
        conn.execute_query(query, [transaction_id])
        
        # Also remove any employee assignments that were specific to the completed service
        # (Optional - you might want to keep assignments for rescheduled services)
        # Uncomment the following if you want to clear assignments on reset:
        """
        assignment_query = '''
        DELETE FROM OPERATIONAL.CARPET.SERVICE_ASSIGNMENTS
        WHERE TRANSACTION_ID = ?
        '''
        conn.execute_query(assignment_query, [transaction_id])
        """
        
        return True
    except Exception as e:
        st.error(f"Error resetting service status: {str(e)}")
        return False

def display_employee_assignment(transaction: Dict[str, Any]) -> None:
    """Display employee assignments for this transaction"""
    
    st.markdown("### 👷 Employee Assignments")
    
    transaction_id = transaction['TRANSACTION_ID']
    
    try:
        # Get employee assignments for this transaction
        conn = SnowflakeConnection.get_instance()
        query = """
        SELECT 
            sa.ASSIGNMENT_ID,
            sa.TRANSACTION_ID,
            sa.EMPLOYEE_ID,
            sa.ASSIGNMENT_DATE,
            sa.ASSIGNMENT_STATUS,
            sa.NOTES,
            e.FIRST_NAME,
            e.LAST_NAME,
            e.EMAIL,
            e.PHONE_NUMBER
        FROM OPERATIONAL.CARPET.SERVICE_ASSIGNMENTS sa
        JOIN OPERATIONAL.CARPET.EMPLOYEE e ON sa.EMPLOYEE_ID = e.EMPLOYEE_ID
        WHERE sa.TRANSACTION_ID = ?
        ORDER BY sa.ASSIGNMENT_DATE DESC
        """
        
        assignments = conn.execute_query(query, [transaction_id])
        
        if assignments:
            st.markdown(f"**{len(assignments)} employee(s) assigned to this transaction:**")
            
            for assignment in assignments:
                with st.container():
                    col1, col2, col3 = st.columns([3, 2, 1])
                    
                    with col1:
                        st.markdown(f"**{assignment['FIRST_NAME']} {assignment['LAST_NAME']}**")
                        st.markdown(f"📧 {assignment['EMAIL']}")
                        if assignment['PHONE_NUMBER']:
                            st.markdown(f"📞 {assignment['PHONE_NUMBER']}")
                        
                    with col2:
                        st.markdown(f"**Status:** {assignment['ASSIGNMENT_STATUS']}")
                        st.markdown(f"**Assigned:** {assignment['ASSIGNMENT_DATE'].strftime('%m/%d/%Y %I:%M %p')}")
                        if assignment['NOTES']:
                            st.markdown(f"**Notes:** {assignment['NOTES']}")
                    
                    with col3:
                        if st.button("Remove", key=f"remove_assignment_{assignment['ASSIGNMENT_ID']}", 
                                   type="secondary", use_container_width=True):
                            if remove_employee_assignment(assignment['ASSIGNMENT_ID']):
                                st.success("Assignment removed!")
                                st.rerun()
                    
                    st.markdown("---")
        else:
            st.info("No employees currently assigned to this transaction")
            
    except Exception as e:
        st.error(f"Error loading employee assignments: {str(e)}")
        
    # Add assignment button
    if st.button("➕ Add Employee Assignment", type="primary"):
        st.session_state.show_employee_assign = f"transaction_{transaction_id}"
    
    # Show assignment dialog if requested
    if st.session_state.get('show_employee_assign') == f"transaction_{transaction_id}":
        display_employee_assignment_dialog(transaction_id)

def display_employee_assignment_dialog(transaction_id: int) -> None:
    """Display dialog for assigning employees to a transaction"""
    
    st.markdown("### ➕ Assign Employee to Transaction")
    
    try:
        conn = SnowflakeConnection.get_instance()
        
        # Get available employees (not already assigned to this transaction)
        employee_query = """
        SELECT 
            e.EMPLOYEE_ID,
            e.FIRST_NAME,
            e.LAST_NAME,
            e.EMAIL
        FROM OPERATIONAL.CARPET.EMPLOYEE e
        WHERE e.EMPLOYEE_ID NOT IN (
            SELECT sa.EMPLOYEE_ID 
            FROM OPERATIONAL.CARPET.SERVICE_ASSIGNMENTS sa 
            WHERE sa.TRANSACTION_ID = ?
        )
        ORDER BY e.FIRST_NAME, e.LAST_NAME
        """
        
        available_employees = conn.execute_query(employee_query, [transaction_id])
        
        if available_employees:
            # Create employee options
            employee_options = {
                f"{emp['FIRST_NAME']} {emp['LAST_NAME']} ({emp['EMAIL']})": emp['EMPLOYEE_ID']
                for emp in available_employees
            }
            
            with st.form("assign_employee_form"):
                selected_employee_name = st.selectbox(
                    "Select Employee:",
                    options=list(employee_options.keys()),
                    index=0
                )
                
                assignment_notes = st.text_area(
                    "Assignment Notes (optional):",
                    placeholder="Any specific notes about this assignment..."
                )
                
                # Assignment buttons
                col_assign, col_close = st.columns(2)
                with col_assign:
                    if st.form_submit_button("✅ Assign Employee", type="primary", use_container_width=True):
                        if selected_employee_name:
                            employee_id = employee_options[selected_employee_name]
                            if assign_employee_to_transaction(transaction_id, employee_id, assignment_notes):
                                st.success(f"Employee {selected_employee_name} assigned successfully!")
                                st.session_state.show_employee_assign = None
                                st.rerun()
                
                with col_close:
                    if st.form_submit_button("❌ Cancel", use_container_width=True):
                        st.session_state.show_employee_assign = None
                        st.rerun()
        else:
            st.info("All available employees are already assigned to this transaction")
            if st.button("❌ Close", use_container_width=True):
                st.session_state.show_employee_assign = None
                st.rerun()
                
    except Exception as e:
        st.error(f"Error loading employees: {str(e)}")

def assign_employee_to_transaction(transaction_id: int, employee_id: int, notes: str = "") -> bool:
    """Assign an employee to a transaction"""
    conn = SnowflakeConnection.get_instance()
    
    query = """
    INSERT INTO OPERATIONAL.CARPET.SERVICE_ASSIGNMENTS (
        TRANSACTION_ID, EMPLOYEE_ID, ASSIGNMENT_STATUS, NOTES
    ) VALUES (?, ?, 'ASSIGNED', ?)
    """
    
    try:
        conn.execute_query(query, [transaction_id, employee_id, notes])
        return True
    except Exception as e:
        st.error(f"Error assigning employee: {str(e)}")
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