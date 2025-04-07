# schedule_service.py
import streamlit as st
from datetime import datetime, timedelta
from database.connection import snowflake_conn
from utils.auth.middleware import require_customer_auth
from utils.auth.auth_utils import check_rate_limit
from utils.formatting import format_currency
from typing import Any, Dict, List, Optional

class QueryDebugger:
    @staticmethod
    def debug_print(title: str, data: Any) -> None:
        """Print debug information if debug mode is enabled"""
        if st.session_state.get('debug_mode', False):
            st.write(f"🔍 Debug: {title}")
            if isinstance(data, (dict, list)):
                st.json(data)
            else:
                st.code(str(data))

    @staticmethod
    def debug_query(query: str, params: List[Any]) -> None:
        """Debug database query and parameters"""
        if st.session_state.get('debug_mode', False):
            with st.expander("🔍 Query Debug", expanded=True):
                st.markdown("### Query")
                st.code(query, language="sql")
                st.markdown("### Parameters")
                for i, param in enumerate(params):
                    st.write(f"Param {i}:", 
                            f"Value: {param}",
                            f"Type: {type(param).__name__}")

@require_customer_auth
def schedule_service_page():
    """Customer portal service scheduling page"""
    debug = QueryDebugger()
    st.title("Schedule a Service")
    
    # Get client info for rate limiting
    client_ip = st.session_state.get('client_ip', 'unknown')
    user_agent = st.session_state.get('user_agent', 'unknown')
    
    debug.debug_print("Session Info", {
        "client_ip": client_ip,
        "user_agent": user_agent,
        "portal_user_id": st.session_state.get('portal_user_id'),
        "customer_id": st.session_state.get('customer_id')
    })
    
    # Check rate limits
    rate_check, message = check_rate_limit(
        client_ip, 
        'BOOKING_ATTEMPT',
        st.session_state.portal_user_id
    )
    if not rate_check:
        st.error(message)
        debug.debug_print("Rate Limit Exceeded", {
            "client_ip": client_ip,
            "attempt_type": 'BOOKING_ATTEMPT',
            "message": message
        })
        return
    
    try:
        # Fetch available services
        services_query = """
        SELECT 
            SERVICE_ID,
            SERVICE_NAME,
            SERVICE_CATEGORY,
            SERVICE_DESCRIPTION,
            COST,
            SERVICE_DURATION,
            DEPOSIT_REQUIRED,
            DEPOSIT_AMOUNT
        FROM OPERATIONAL.CARPET.SERVICES
        WHERE ACTIVE_STATUS = TRUE
        AND CUSTOMER_BOOKABLE = TRUE
        ORDER BY SERVICE_CATEGORY, SERVICE_NAME
        """
        
        debug.debug_query(services_query, [])
        services = snowflake_conn.execute_query(services_query)
        
        if not services or len(services) == 0:
            st.error("No services available for booking at this time")
            debug.debug_print("Services Query Result", "No services found")
            return
        
        # Convert service rows to dictionaries for easier access
        services = [row.as_dict() for row in services]
        debug.debug_print("Available Services", services)
        
        # Service Selection
        st.subheader("Select Service")
        selected_service = None
        
        # Group services by category
        service_categories = {}
        for service in services:
            category = service['SERVICE_CATEGORY']
            if category not in service_categories:
                service_categories[category] = []
            service_categories[category].append(service)
        
        for category, category_services in service_categories.items():
            st.write(f"**{category}**")
            cols = st.columns(len(category_services))
            for idx, service in enumerate(category_services):
                with cols[idx]:
                    if st.button(
                        f"{service['SERVICE_NAME']}\n"
                        f"{format_currency(float(service['COST']))}",
                        key=f"service_{service['SERVICE_ID']}",
                        use_container_width=True
                    ):
                        selected_service = service
                        debug.debug_print("Selected Service", service)
        
        if selected_service:
            st.write("---")
            st.write(f"**Selected:** {selected_service['SERVICE_NAME']}")
            st.write(selected_service['SERVICE_DESCRIPTION'])
            st.write(f"Cost: {format_currency(float(selected_service['COST']))}")
            
            if selected_service['DEPOSIT_REQUIRED']:
                st.info(f"This service requires a deposit of {format_currency(float(selected_service['DEPOSIT_AMOUNT']))}")
            
            # Date Selection
            st.subheader("Select Date")
            min_date = datetime.now().date()
            max_date = min_date + timedelta(days=180)  # 6 months in advance
            selected_date = st.date_input(
                "Service Date",
                min_value=min_date,
                max_value=max_date,
                value=min_date
            )
            
            if selected_date:
                debug.debug_print("Selected Date", str(selected_date))
                
                # Check availability for the selected date
                availability_query = """
                SELECT 
                    START_TIME,
                    SERVICE_DURATION
                FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION
                WHERE SERVICE_DATE = ?
                AND STATUS IN ('SCHEDULED', 'IN_PROGRESS')
                ORDER BY START_TIME
                """
                
                debug.debug_query(availability_query, [selected_date])
                bookings = snowflake_conn.execute_query(availability_query, [selected_date])
                debug.debug_print("Existing Bookings", bookings)
                
                # Calculate available time slots
                business_start = datetime.strptime("08:00", "%H:%M").time()
                business_end = datetime.strptime("17:00", "%H:%M").time()
                service_duration = int(selected_service['SERVICE_DURATION'])
                
                available_slots = []
                current_slot = datetime.combine(selected_date, business_start)
                end_time = datetime.combine(selected_date, business_end)
                
                while current_slot + timedelta(minutes=service_duration) <= end_time:
                    slot_available = True
                    slot_end = current_slot + timedelta(minutes=service_duration)
                    
                    # Check if slot conflicts with existing bookings
                    if bookings:
                        for booking in bookings:
                            booking_start = datetime.combine(
                                selected_date,
                                booking['START_TIME']
                            )
                            booking_end = booking_start + timedelta(
                                minutes=int(booking['SERVICE_DURATION'])
                            )
                            
                            if (current_slot < booking_end and 
                                slot_end > booking_start):
                                slot_available = False
                                break
                    
                    if slot_available:
                        available_slots.append(current_slot.time())
                    
                    current_slot += timedelta(minutes=30)
                
                debug.debug_print("Available Time Slots", 
                                [slot.strftime("%I:%M %p") for slot in available_slots])
                
                # Time Selection
                if available_slots:
                    st.subheader("Select Time")
                    time_columns = st.columns(4)
                    selected_time = None
                    
                    for idx, time_slot in enumerate(available_slots):
                        with time_columns[idx % 4]:
                            if st.button(
                                time_slot.strftime("%I:%M %p"),
                                key=f"time_{time_slot.strftime('%H%M')}",
                                use_container_width=True
                            ):
                                selected_time = time_slot
                                debug.debug_print("Selected Time", str(selected_time))
                    
                    if selected_time:
                        st.write("---")
                        # Recurring Options
                        is_recurring = st.checkbox("Make this a recurring service")
                        recurrence_pattern = None
                        
                        if is_recurring:
                            recurrence_pattern = st.selectbox(
                                "How often?",
                                ["Weekly", "Bi-Weekly", "Monthly"]
                            )
                            st.info(
                                "Recurring services will be scheduled for 6 months. "
                                "You can cancel individual appointments if needed."
                            )
                            debug.debug_print("Recurring Settings", {
                                "is_recurring": is_recurring,
                                "pattern": recurrence_pattern
                            })
                        
                        # Notes
                        notes = st.text_area("Additional Notes", placeholder="Optional")
                        
                        # Confirmation
                        st.subheader("Review and Confirm")
                        st.write("**Service Details**")
                        st.write(f"Service: {selected_service['SERVICE_NAME']}")
                        st.write(f"Date: {selected_date.strftime('%B %d, %Y')}")
                        st.write(f"Time: {selected_time.strftime('%I:%M %p')}")
                        st.write(f"Cost: {format_currency(float(selected_service['COST']))}")
                        
                        if selected_service['DEPOSIT_REQUIRED']:
                            st.write(f"Deposit Required: {format_currency(float(selected_service['DEPOSIT_AMOUNT']))}")
                        
                        if is_recurring:
                            st.write(f"Recurring: {recurrence_pattern}")
                        
                        if st.button("Confirm Booking", type="primary", use_container_width=True):
                            try:
                                # Save the service booking
                                booking_query = """
                                INSERT INTO OPERATIONAL.CARPET.SERVICE_TRANSACTION (
                                    CUSTOMER_ID,
                                    SERVICE_ID,
                                    SERVICE_NAME,
                                    SERVICE_DATE,
                                    START_TIME,
                                    AMOUNT,
                                    STATUS,
                                    DEPOSIT,
                                    DEPOSIT_PAID,
                                    IS_RECURRING,
                                    RECURRENCE_PATTERN,
                                    COMMENTS,
                                    TRANSACTION_DATE,
                                    CREATED_DATE,
                                    BASE_SERVICE_COST
                                )
                                VALUES (
                                    ?, ?, ?, ?, ?, ?, 'PENDING',
                                    ?, FALSE,
                                    ?, ?, ?,
                                    CURRENT_DATE(),
                                    CURRENT_TIMESTAMP(),
                                    ?
                                )
                                """
                                
                                # Prepare parameters with explicit type casting
                                booking_params = [
                                    int(st.session_state.customer_id),
                                    int(selected_service['SERVICE_ID']),
                                    str(selected_service['SERVICE_NAME']),
                                    selected_date,
                                    selected_time,
                                    float(selected_service['COST']),
                                    float(selected_service['DEPOSIT_AMOUNT']) if selected_service['DEPOSIT_REQUIRED'] else 0.0,
                                    bool(is_recurring),
                                    str(recurrence_pattern) if is_recurring else None,
                                    str(notes) if notes else None,
                                    float(selected_service['COST'])
                                ]

                                debug.debug_query(booking_query, booking_params)
                                
                                snowflake_conn.execute_query(booking_query, booking_params)
                                
                                if is_recurring:
                                    debug.debug_print("Scheduling Recurring Services", {
                                        "pattern": recurrence_pattern,
                                        "start_date": str(selected_date)
                                    })
                                    
                                    # Schedule recurring services with same type casting
                                    current_date = selected_date
                                    for _ in range(24):  # 6 months of weekly services
                                        if recurrence_pattern == "Weekly":
                                            current_date += timedelta(days=7)
                                        elif recurrence_pattern == "Bi-Weekly":
                                            current_date += timedelta(days=14)
                                        elif recurrence_pattern == "Monthly":
                                            # Add a month
                                            year = current_date.year
                                            month = current_date.month + 1
                                            if month > 12:
                                                year += 1
                                                month = 1
                                            current_date = current_date.replace(
                                                year=year, month=month
                                            )
                                        
                                        if (current_date - selected_date).days > 180:
                                            break
                                            
                                        recurring_params = [
                                            int(st.session_state.customer_id),
                                            int(selected_service['SERVICE_ID']),
                                            str(selected_service['SERVICE_NAME']),
                                            current_date,
                                            selected_time,
                                            float(selected_service['COST']),
                                            float(selected_service['DEPOSIT_AMOUNT']) if selected_service['DEPOSIT_REQUIRED'] else 0.0,
                                            bool(is_recurring),
                                            str(recurrence_pattern),
                                            str(notes) if notes else None,
                                            float(selected_service['COST'])
                                        ]
                                        
                                        debug.debug_query(booking_query, recurring_params)
                                        snowflake_conn.execute_query(booking_query, recurring_params)
                                
                                st.success("Service scheduled successfully!")
                                st.balloons()
                                
                                if selected_service['DEPOSIT_REQUIRED']:
                                    st.warning(
                                        "Please note that your appointment requires a deposit. "
                                        "Our team will contact you for deposit payment."
                                    )
                                
                                # Return to portal home
                                if st.button("Return to Portal"):
                                    st.session_state.page = 'portal_home'
                                    st.rerun()
                                    
                            except Exception as e:
                                st.error("Error scheduling service")
                                debug.debug_print("Booking Error", {
                                    "error_type": type(e).__name__,
                                    "error_message": str(e),
                                    "customer_id": st.session_state.customer_id,
                                    "service": selected_service,
                                    "date": str(selected_date),
                                    "time": str(selected_time)
                                })
                else:
                    st.warning("No available time slots for the selected date")
                    st.write("Please select another date")
                    debug.debug_print("No Available Slots", {
                        "date": str(selected_date),
                        "service_duration": service_duration
                    })
    
    except Exception as e:
        st.error("Error loading services")
        debug.debug_print("Service Load Error", {
            "error_type": type(e).__name__,
            "error_message": str(e),
            "stack_trace": getattr(e, '__traceback__', None)
        })
        if st.session_state.get('debug_mode'):
            st.exception(e)
        return
        
    # Add cancel/back buttons at bottom
    st.markdown("---")
    col1, col2 = st.columns(2)
    with col1:
        if st.button("← Back", use_container_width=True):
            debug.debug_print("Navigation", "Back button clicked")
            st.session_state.page = 'portal_home'
            st.rerun()
            
    with col2:
        if st.button("Cancel", type="secondary", use_container_width=True):
            debug.debug_print("Navigation", "Cancel button clicked")
            st.session_state.page = 'portal_home'
            st.rerun()