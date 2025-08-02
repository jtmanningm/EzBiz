# pages/portal/services/upcoming.py
import streamlit as st
from datetime import datetime, timedelta
from utils.auth.middleware import require_customer_auth
from database.connection import snowflake_conn

@require_customer_auth
def upcoming_services_page():
    """Upcoming services management page"""
    st.title("Upcoming Services")
    
    try:
        # Fetch upcoming services
        query = """
        SELECT 
            ID as TRANSACTION_ID,
            SERVICE_NAME,
            SERVICE_DATE,
            START_TIME,
            AMOUNT,
            DEPOSIT,
            DEPOSIT_PAID,
            IS_RECURRING,
            RECURRENCE_PATTERN,
            COMMENTS,
            STATUS
        FROM SERVICE_TRANSACTION
        WHERE CUSTOMER_ID = ?
        AND SERVICE_DATE >= CURRENT_DATE()
        AND STATUS IN ('SCHEDULED', 'CANCELLED')
        ORDER BY SERVICE_DATE, START_TIME
        """
                
        services = snowflake_conn.execute_query(query, [st.session_state.customer_id])
        
        if not services:
            st.info("No upcoming services scheduled")
            
            if st.button("Schedule a Service"):
                st.session_state.page = 'book_service'
                st.rerun()
            return
            
        # Organize by date
        current_date = None
        
        for service in services:
            # Display date header if date changes
            service_date = service['SERVICE_DATE']
            if current_date != service_date:
                current_date = service_date
                st.header(service_date.strftime('%B %d, %Y'))
            
            # Service card
            with st.container():
                # Service info
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.subheader(service['SERVICE_NAME'])
                    st.write(f"🕒 {service['START_TIME'].strftime('%I:%M %p')}")
                    if service['IS_RECURRING']:
                        st.write(f"🔄 {service['RECURRENCE_PATTERN']} Service")
                    if service['COMMENTS']:
                        st.write(f"📝 {service['COMMENTS']}")
                        
                with col2:
                    if service['DEPOSIT'] > 0:
                        deposit_status = "✅" if service['DEPOSIT_PAID'] else "❌"
                        st.write(
                            f"Deposit: ${float(service['DEPOSIT']):.2f} "
                            f"{deposit_status}"
                        )
                
                # Actions - different buttons based on status
                if service.get('STATUS') == 'CANCELLED':
                    # For cancelled services, show restart button
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("❌ **Service Cancelled**")
                    with col2:
                        # Restart button
                        if st.button(
                            "🔄 Restart Service", 
                            key=f"restart_{service['TRANSACTION_ID']}",
                            type="primary",
                            use_container_width=True
                        ):
                            # Restart the service
                            try:
                                update_query = """
                                UPDATE SERVICE_TRANSACTION
                                SET STATUS = 'SCHEDULED',
                                    COMMENTS = COALESCE(COMMENTS || ' | ', '') || 'Service restarted by customer on ' || CURRENT_DATE(),
                                    LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
                                WHERE ID = ?
                                """
                                snowflake_conn.execute_query(update_query, [service['TRANSACTION_ID']])
                                st.success("Service restarted successfully!")
                                st.rerun()
                            except Exception as e:
                                st.error(f"Error restarting service: {str(e)}")
                else:
                    # For scheduled services, show normal buttons
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        # Reschedule button
                        if st.button(
                            "Reschedule", 
                            key=f"reschedule_{service['TRANSACTION_ID']}",
                            type="secondary",
                            use_container_width=True
                        ):
                            st.session_state.reschedule_service = service
                            st.session_state.show_reschedule = True
                            st.rerun()
                            
                    with col2:
                        # Modify button (for notes)
                        if st.button(
                            "Modify Notes", 
                            key=f"modify_{service['TRANSACTION_ID']}",
                            type="secondary",
                            use_container_width=True
                        ):
                            st.session_state.modify_service = service
                            st.session_state.show_modify = True
                            st.rerun()
                            
                    with col3:
                        # Cancel button
                        if st.button(
                            "Cancel", 
                            key=f"cancel_{service['TRANSACTION_ID']}",
                            type="secondary",
                            use_container_width=True
                        ):
                            st.session_state.cancel_service = service
                            st.session_state.show_cancel = True
                            st.rerun()
                        
                st.markdown("---")
        
        # Handle reschedule modal
        if st.session_state.get('show_reschedule'):
            service = st.session_state.reschedule_service
            st.sidebar.title("Reschedule Service")
            
            # Date selection
            min_date = datetime.now().date()
            max_date = min_date + timedelta(days=180)
            new_date = st.sidebar.date_input(
                "New Date",
                min_value=min_date,
                max_value=max_date,
                value=service['SERVICE_DATE']
            )
            
            # Time selection
            if new_date:
                # Get available times
                times_query = """
                SELECT 
                    START_TIME,
                    SERVICE_DURATION
                FROM SERVICE_TRANSACTION
                WHERE SERVICE_DATE = ?
                AND STATUS = 'SCHEDULED'
                ORDER BY START_TIME
                """
                
                booked_times = snowflake_conn.execute_query(
                    times_query,
                    [new_date]
                )
                
                # Generate available slots
                business_start = datetime.strptime("08:00", "%H:%M").time()
                business_end = datetime.strptime("17:00", "%H:%M").time()
                slot_duration = 30  # minutes
                
                current_slot = datetime.combine(new_date, business_start)
                end_time = datetime.combine(new_date, business_end)
                
                available_slots = []
                while current_slot <= end_time:
                    slot_available = True
                    for booking in booked_times:
                        booking_start = datetime.combine(
                            new_date,
                            booking['START_TIME']
                        )
                        booking_end = booking_start + timedelta(
                            minutes=booking['SERVICE_DURATION']
                        )
                        
                        if (current_slot >= booking_start and 
                            current_slot < booking_end):
                            slot_available = False
                            break
                            
                    if slot_available:
                        available_slots.append(current_slot.time())
                        
                    current_slot += timedelta(minutes=slot_duration)
                
                if available_slots:
                    time_options = [t.strftime("%I:%M %p") for t in available_slots]
                    new_time = st.sidebar.selectbox(
                        "New Time",
                        options=time_options
                    )
                    
                    if st.sidebar.button("Confirm Reschedule", type="primary"):
                        try:
                            # Update service
                            update_query = """
                            UPDATE SERVICE_TRANSACTION
                            SET 
                                SERVICE_DATE = ?,
                                START_TIME = ?,
                                LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
                            WHERE ID = ?
                            """
                            
                            snowflake_conn.execute_query(update_query, [
                                new_date,
                                datetime.strptime(new_time, "%I:%M %p").time(),
                                service['TRANSACTION_ID']
                            ])
                            
                            st.session_state.pop('show_reschedule', None)
                            st.session_state.pop('reschedule_service', None)
                            st.success("Service rescheduled successfully!")
                            st.rerun()
                            
                        except Exception as e:
                            st.error("Error rescheduling service")
                            print(f"Reschedule error: {str(e)}")
                else:
                    st.sidebar.warning("No available times for selected date")
                    
            if st.sidebar.button("Cancel"):
                st.session_state.pop('show_reschedule', None)
                st.session_state.pop('reschedule_service', None)
                st.rerun()
        
        # Handle modify modal
        if st.session_state.get('show_modify'):
            service = st.session_state.modify_service
            st.sidebar.title("Modify Service Notes")
            
            new_notes = st.sidebar.text_area(
                "Service Notes",
                value=service['COMMENTS'] if service['COMMENTS'] else ""
            )
            
            if st.sidebar.button("Save Changes", type="primary"):
                try:
                    # Update notes
                    update_query = """
                    UPDATE SERVICE_TRANSACTION
                    SET 
                        COMMENTS = ?,
                        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
                    WHERE ID = ?
                    """
                    
                    snowflake_conn.execute_query(update_query, [
                        new_notes,
                        service['TRANSACTION_ID']
                    ])
                    
                    st.session_state.pop('show_modify', None)
                    st.session_state.pop('modify_service', None)
                    st.success("Service notes updated!")
                    st.rerun()
                    
                except Exception as e:
                    st.error("Error updating service notes")
                    print(f"Note update error: {str(e)}")
                    
            if st.sidebar.button("Cancel"):
                st.session_state.pop('show_modify', None)
                st.session_state.pop('modify_service', None)
                st.rerun()
        
        # Handle cancel modal
        if st.session_state.get('show_cancel'):
            service = st.session_state.cancel_service
            st.sidebar.title("Cancel Service")
            
            st.sidebar.warning(
                "Are you sure you want to cancel this service?\n\n"
                f"**{service['SERVICE_NAME']}**\n"
                f"Date: {service['SERVICE_DATE'].strftime('%B %d, %Y')}\n"
                f"Time: {service['START_TIME'].strftime('%I:%M %p')}"
            )
            
            cancel_notes = st.sidebar.text_area(
                "Cancellation Reason (Optional)"
            )
            
            col1, col2 = st.sidebar.columns(2)
            with col1:
                if st.button("Yes, Cancel", type="primary", use_container_width=True):
                    try:
                        # Update service status
                        update_query = """
                        UPDATE SERVICE_TRANSACTION
                        SET 
                            STATUS = 'CANCELLED',
                            COMMENTS = ?,
                            LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
                        WHERE ID = ?
                        """
                        
                        snowflake_conn.execute_query(update_query, [
                            cancel_notes,
                            service['TRANSACTION_ID']
                        ])
                        
                        st.session_state.pop('show_cancel', None)
                        st.session_state.pop('cancel_service', None)
                        st.success("Service cancelled successfully!")
                        st.rerun()
                        
                    except Exception as e:
                        st.error("Error cancelling service")
                        print(f"Cancel error: {str(e)}")
                        
            with col2:
                if st.button("No, Keep", use_container_width=True):
                    st.session_state.pop('show_cancel', None)
                    st.session_state.pop('cancel_service', None)
                    st.rerun()
                    
    except Exception as e:
        st.error("Error loading upcoming services")
        print(f"Upcoming services error: {str(e)}")

if __name__ == "__main__":
    upcoming_services_page()