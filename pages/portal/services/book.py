import streamlit as st
from datetime import datetime, timedelta, time
from typing import Tuple
from utils.auth.middleware import require_customer_auth
from utils.auth.auth_utils import check_rate_limit
from database.connection import snowflake_conn
from pages.settings.business import fetch_business_info
from models.service import schedule_recurring_services
from models.service import get_available_time_slots


def get_client_info() -> str:
    """Get client IP from session state or set default."""
    if 'client_ip' not in st.session_state:
        st.session_state.client_ip = 'unknown'
    return st.session_state.client_ip

def fetch_service_addresses(customer_id: int):
    """Fetch service addresses for the logged in customer"""
    query = """
    SELECT 
        ADDRESS_ID,
        STREET_ADDRESS,
        CITY,
        STATE,
        ZIP_CODE,
        SQUARE_FOOTAGE,
        IS_PRIMARY_SERVICE
    FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES
    WHERE CUSTOMER_ID = ?
    ORDER BY IS_PRIMARY_SERVICE DESC, ADDRESS_ID
    """
    return snowflake_conn.execute_query(query, [customer_id])


def get_business_hours(date: datetime.date) -> Tuple[time, time]:
    """
    Get business hours for the given date from settings.
    Returns (opening_time, closing_time).
    """
    business_info = fetch_business_info()
    is_weekend = date.weekday() >= 5
    start_str = (
        business_info.get('WEEKEND_OPERATING_HOURS_START', '09:00')
        if is_weekend
        else business_info.get('OPERATING_HOURS_START', '08:00')
    )
    end_str = (
        business_info.get('WEEKEND_OPERATING_HOURS_END', '14:00')
        if is_weekend
        else business_info.get('OPERATING_HOURS_END', '17:00')
    )
    try:
        opening_time = datetime.strptime(start_str, '%H:%M').time()
        closing_time = datetime.strptime(end_str, '%H:%M').time()
    except (ValueError, TypeError):
        opening_time = time(8, 0) if not is_weekend else time(9, 0)
        closing_time = time(17, 0) if not is_weekend else time(14, 0)
    return opening_time, closing_time


def handle_recurring_bookings(service, base_date, time_slot, address_id, customer_id, pattern, notes):
    """Handle recurring bookings for a service."""
    booking_query = """
    INSERT INTO OPERATIONAL.CARPET.SERVICE_TRANSACTION (
        SERVICE_ID,
        CUSTOMER_ID,
        ADDRESS_ID,
        PYMT_MTHD_1,
        TRANSACTION_DATE,
        TRANSACTION_TIME,
        AMOUNT,
        DEPOSIT,
        START_TIME,
        END_TIME,
        STATUS,
        SERVICE_DATE,
        IS_RECURRING,
        RECURRENCE_PATTERN,
        COMMENTS,
        SERVICE_NAME
    ) VALUES (
        :1,                -- SERVICE_ID
        :2,                -- CUSTOMER_ID
        :3,                -- ADDRESS_ID
        NULL,              -- PYMT_MTHD_1
        :4,                -- TRANSACTION_DATE
        :5,                -- TRANSACTION_TIME
        :6,                -- AMOUNT
        0,                 -- DEPOSIT
        :7,                -- START_TIME
        :8,                -- END_TIME
        'SCHEDULED',       -- STATUS
        :9,                -- SERVICE_DATE
        TRUE,              -- IS_RECURRING
        :10,               -- RECURRENCE_PATTERN
        :11,               -- COMMENTS
        :12                -- SERVICE_NAME
    )
    """
    
    current_date = base_date
    # Use dictionary-style access instead of .get() method
    service_duration = service['SERVICE_DURATION'] if 'SERVICE_DURATION' in service else 60
    end_time = (datetime.combine(base_date, time_slot) + timedelta(minutes=service_duration)).time()
    
    for _ in range(24):  # 6 months max
        if pattern == "Weekly":
            current_date += timedelta(days=7)
        elif pattern == "Bi-Weekly":
            current_date += timedelta(days=14)
        elif pattern == "Monthly":
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=1)
            else:
                current_date = current_date.replace(month=current_date.month + 1)
        
        if (current_date - base_date).days > 180:
            break
            
        if current_date.weekday() == 6:  # Skip Sundays
            continue
        
        snowflake_conn.execute_query(booking_query, [
        service['SERVICE_ID'],           # SERVICE_ID
        customer_id,                     # CUSTOMER_ID
        address_id,                      # ADDRESS_ID
        current_date,                    # TRANSACTION_DATE
        time_slot,                       # TRANSACTION_TIME
        float(service['COST']),          # AMOUNT
        time_slot,                       # START_TIME
        end_time,                        # END_TIME
        current_date,                    # SERVICE_DATE
        pattern,                         # RECURRENCE_PATTERN
        notes,                           # COMMENTS
        service['SERVICE_NAME'],         # SERVICE_NAME
        float(service['COST'])           # BASE_SERVICE_COST
    ])

def get_additional_services(service_id):
    """
    Fetch primary and additional services with their costs.
    Calculate the total cost, including up to two additional services.
    """
    query = """
    SELECT 
        s1.SERVICE_ID AS PRIMARY_SERVICE_ID,
        s1.COST AS PRIMARY_COST,
        s2.SERVICE_ID AS SERVICE2_ID,
        s2.COST AS SERVICE2_COST,
        s3.SERVICE_ID AS SERVICE3_ID,
        s3.COST AS SERVICE3_COST
    FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION t
    JOIN OPERATIONAL.CARPET.SERVICES s1 ON t.SERVICE_ID = s1.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES s2 ON t.SERVICE2_ID = s2.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES s3 ON t.SERVICE3_ID = s3.SERVICE_ID
    WHERE t.ID = :1
    """
    
    try:
        result = snowflake_conn.execute_query(query, [service_id])
    except Exception as e:
        st.error(f"Error fetching service details: {e}")
        return None, None, 0.0

    # Check if query returned data
    if not result or not result[0]:
        st.warning(f"No service details found for transaction ID: {service_id}")
        return None, None, 0.0

    # Extract data from the result
    row = result[0]
    primary_cost = float(row.get('PRIMARY_COST') or 0.0)
    service2_id = row.get('SERVICE2_ID')
    service2_cost = float(row.get('SERVICE2_COST') or 0.0)
    service3_id = row.get('SERVICE3_ID')
    service3_cost = float(row.get('SERVICE3_COST') or 0.0)

    # Calculate total cost
    total_cost = primary_cost
    if service2_id:
        total_cost += service2_cost
    if service3_id:
        total_cost += service3_cost

    return service2_id, service3_id, total_cost

def clear_booking_session():
    """Clear all booking-related session state"""
    booking_keys = [
        'booking_step',
        'selected_service',
        'selected_date',
        'selected_time',
        'selected_address_id',
        'is_recurring',
        'recurrence_pattern',
        'booking_notes'
    ]
    for key in booking_keys:
        if key in st.session_state:
            del st.session_state[key]

@require_customer_auth
def book_service_page():
    """Service booking page with service address selection"""
    st.title("Schedule a Service")
    
    # Initialize booking state
    if 'booking_step' not in st.session_state:
        st.session_state.booking_step = 1
    if 'selected_service' not in st.session_state:
        st.session_state.selected_service = None
    if 'selected_date' not in st.session_state:
        st.session_state.selected_date = None
    if 'selected_time' not in st.session_state:
        st.session_state.selected_time = None
    if 'selected_address_id' not in st.session_state:
        st.session_state.selected_address_id = None

    # Step 1: Service Address Selection
    if st.session_state.booking_step == 1:
        st.subheader("Select Service Location")
        addresses = fetch_service_addresses(st.session_state.customer_id)
        
        if not addresses:
            st.error("No service addresses found. Please contact support to add a service location.")
            if st.button("Return to Portal Home"):
                st.session_state.page = 'portal_home'
                st.rerun()
            return
            
        for address in addresses:
            with st.container():
                cols = st.columns([3, 1])
                with cols[0]:
                    address_text = f"{address['STREET_ADDRESS']}, {address['CITY']}, {address['STATE']} {address['ZIP_CODE']}"
                    if address['IS_PRIMARY_SERVICE']:
                        address_text += " (Primary)"
                    st.write(f"**{address_text}**")
                    if address['SQUARE_FOOTAGE']:
                        st.write(f"Square Footage: {address['SQUARE_FOOTAGE']}")
                with cols[1]:
                    if st.button("Select", key=f"addr_{address['ADDRESS_ID']}"):
                        st.session_state.selected_address_id = address['ADDRESS_ID']
                        st.session_state.booking_step = 2
                        st.rerun()
                st.markdown("---")

        if st.button("Cancel Booking", use_container_width=True):
            clear_booking_session()
            st.session_state.page = 'portal_home'
            st.rerun()

    # Step 2: Service Selection
    elif st.session_state.booking_step == 2:
        st.subheader("Select Service")
        services_query = """
        SELECT 
            SERVICE_ID,
            SERVICE_NAME,
            SERVICE_DESCRIPTION,
            SERVICE_CATEGORY,
            COST,
            SERVICE_DURATION
        FROM OPERATIONAL.CARPET.SERVICES
        WHERE ACTIVE_STATUS = TRUE
        AND CUSTOMER_BOOKABLE = TRUE
        ORDER BY SERVICE_CATEGORY, SERVICE_NAME
        """
        services = snowflake_conn.execute_query(services_query)
        if not services:
            st.error("No services available for booking")
            return
            
        for service in services:
            with st.container():
                cols = st.columns([3, 1, 1])
                with cols[0]:
                    st.write(f"**{service['SERVICE_NAME']}**")
                    st.write(f"Category: {service['SERVICE_CATEGORY']}")
                    if service['SERVICE_DESCRIPTION']:
                        st.write(service['SERVICE_DESCRIPTION'])
                with cols[1]:
                    st.write(f"Cost: ${float(service['COST']):.2f}")
                    st.write(f"Duration: {service['SERVICE_DURATION']} min")
                with cols[2]:
                    if st.button("Select", key=f"select_{service['SERVICE_ID']}"):
                        st.session_state.selected_service = service
                        st.session_state.booking_step = 3
                        st.rerun()
                st.markdown("---")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("← Back", use_container_width=True):
                st.session_state.booking_step = 1
                st.rerun()
        with col2:
            if st.button("Cancel", use_container_width=True):
                clear_booking_session()
                st.session_state.page = 'portal_home'
                st.rerun()

    # Step 3: Date Selection
    elif st.session_state.booking_step == 3:
        st.subheader("Select Date and Time")
        
        col1, col2 = st.columns(2)
        with col1:
            min_date = datetime.now().date()
            max_date = min_date + timedelta(days=180)
            
            selected_date = st.date_input(
                "Service Date",
                min_value=min_date,
                max_value=max_date,
                value=st.session_state.selected_date or min_date,
                help="Select a date within the next 6 months"
            )
            
            # if selected_date and selected_date.weekday() == 6:
            #     st.error("We are closed on Sundays. Please select another date.")
            #     return
        
        with col2:
            if selected_date:
                times = get_available_time_slots(selected_date)
                
                if not times:
                    st.warning("No available times for selected date")
                else:
                    selected_time = st.selectbox(
                        "Select Time",
                        options=times,
                        format_func=lambda x: x.strftime("%I:%M %p")
                    )
                    
                    if selected_time:
                        st.session_state.selected_date = selected_date
                        st.session_state.selected_time = selected_time
        
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            if st.button("← Back", use_container_width=True):
                st.session_state.booking_step = 2
                st.rerun()
        with col2:
            if st.button("Cancel", use_container_width=True):
                clear_booking_session()
                st.session_state.page = 'portal_home'
                st.rerun()
        with col3:
            if selected_date and 'selected_time' in st.session_state:
                if st.button("Continue →", type="primary", use_container_width=True):
                    st.session_state.booking_step = 4
                    st.rerun()

    # Step 4: Recurring Options & Notes
    elif st.session_state.booking_step == 4:
        st.subheader("Additional Options")
        
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
        
        booking_notes = st.text_area(
            "Additional Notes",
            help="Add any special instructions or requirements"
        )
        
        st.session_state.is_recurring = is_recurring
        st.session_state.recurrence_pattern = recurrence_pattern
        st.session_state.booking_notes = booking_notes
        
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            if st.button("← Back", use_container_width=True):
                st.session_state.booking_step = 3
                st.rerun()
        with col2:
            if st.button("Cancel", use_container_width=True):
                clear_booking_session()
                st.session_state.page = 'portal_home'
                st.rerun()
        with col3:
            if st.button("Continue →", type="primary", use_container_width=True):
                st.session_state.booking_step = 5
                st.rerun()

    # Step 5: Confirmation
    elif st.session_state.booking_step == 5:
        st.subheader("Confirm Booking")
        
        service = st.session_state.selected_service
        
        # Get selected address details
        addresses = fetch_service_addresses(st.session_state.customer_id)
        selected_address = next(
            (addr for addr in addresses if addr['ADDRESS_ID'] == st.session_state.selected_address_id),
            None
        )
        
        # Display booking details
        st.write("### Service Details")
        st.write("**Service Location:**")
        st.write(f"{selected_address['STREET_ADDRESS']}")
        st.write(f"{selected_address['CITY']}, {selected_address['STATE']} {selected_address['ZIP_CODE']}")
        
        st.write(f"**Service:** {service['SERVICE_NAME']}")
        st.write(f"**Category:** {service['SERVICE_CATEGORY']}")
        st.write(f"**Date:** {st.session_state.selected_date.strftime('%B %d, %Y')}")
        st.write(f"**Time:** {st.session_state.selected_time.strftime('%I:%M %p')}")
        st.write(f"**Duration:** {service['SERVICE_DURATION']} minutes")
        st.write(f"**Cost:** ${float(service['COST']):.2f}")
            
        if st.session_state.is_recurring:
            st.write(f"**Recurring:** {st.session_state.recurrence_pattern}")
            
        if st.session_state.booking_notes:
            st.write("**Notes:**")
            st.write(st.session_state.booking_notes)
        
        # Navigation buttons
        col1, col2, col3 = st.columns([1, 1, 1])
        with col1:
            if st.button("← Back", use_container_width=True):
                st.session_state.booking_step = 4
                st.rerun()
        with col2:
            if st.button("Cancel", use_container_width=True):
                clear_booking_session()
                st.session_state.page = 'portal_home'
                st.rerun()
        with col3:
            if st.button("Confirm Booking", type="primary", use_container_width=True):
                try:
                    # Calculate end time based on service duration
                    service_duration = service['SERVICE_DURATION'] if 'SERVICE_DURATION' in service else 60
                    end_time = (datetime.combine(st.session_state.selected_date, 
                                              st.session_state.selected_time) + 
                              timedelta(minutes=service_duration)).time()
                    
                    # Save booking
                    booking_query = """
                    INSERT INTO OPERATIONAL.CARPET.SERVICE_TRANSACTION (
                        SERVICE_ID,
                        CUSTOMER_ID,
                        ADDRESS_ID,
                        PYMT_MTHD_1,
                        TRANSACTION_DATE,
                        TRANSACTION_TIME,
                        AMOUNT,
                        DEPOSIT,
                        START_TIME,
                        END_TIME,
                        STATUS,
                        SERVICE_DATE,
                        IS_RECURRING,
                        RECURRENCE_PATTERN,
                        COMMENTS,
                        SERVICE_NAME,
                        BASE_SERVICE_COST  -- Add this column
                    ) VALUES (
                        :1,                -- SERVICE_ID
                        :2,                -- CUSTOMER_ID
                        :3,                -- ADDRESS_ID
                        NULL,              -- PYMT_MTHD_1
                        :4,                -- TRANSACTION_DATE
                        :5,                -- TRANSACTION_TIME
                        :6,                -- AMOUNT
                        0,                 -- DEPOSIT
                        :7,                -- START_TIME
                        :8,                -- END_TIME
                        'SCHEDULED',       -- STATUS
                        :9,                -- SERVICE_DATE
                        :10,               -- IS_RECURRING
                        :11,               -- RECURRENCE_PATTERN
                        :12,               -- COMMENTS
                        :13,               -- SERVICE_NAME
                        :6                 -- BASE_SERVICE_COST (same as AMOUNT)
                    )
                    """
                    
                    # Execute booking query
                    snowflake_conn.execute_query(booking_query, [
                        service['SERVICE_ID'],                   # SERVICE_ID
                        st.session_state.customer_id,           # CUSTOMER_ID
                        st.session_state.selected_address_id,   # ADDRESS_ID
                        st.session_state.selected_date,         # TRANSACTION_DATE
                        st.session_state.selected_time,         # TRANSACTION_TIME
                        float(service['COST']),                 # AMOUNT
                        st.session_state.selected_time,         # START_TIME
                        end_time,                               # END_TIME
                        st.session_state.selected_date,         # SERVICE_DATE
                        st.session_state.is_recurring,          # IS_RECURRING
                        st.session_state.recurrence_pattern,    # RECURRENCE_PATTERN
                        st.session_state.booking_notes,         # COMMENTS
                        service['SERVICE_NAME']                 # SERVICE_NAME
                    ])
                    
                    # Handle recurring bookings if needed
                    if st.session_state.is_recurring:
                        handle_recurring_bookings(
                            service=service,
                            base_date=st.session_state.selected_date,
                            time_slot=st.session_state.selected_time,
                            address_id=st.session_state.selected_address_id,
                            customer_id=st.session_state.customer_id,
                            pattern=st.session_state.recurrence_pattern,
                            notes=st.session_state.booking_notes
                        )
                    
                    st.success("Service scheduled successfully!")
                    # st.balloons()
                    
                    # Clear booking state
                    clear_booking_session()
                    
                    # Return to portal home
                    st.session_state.page = 'portal_home'
                    st.rerun()
                    
                except Exception as e:
                    st.error("Error scheduling service")
                    print(f"Booking error: {str(e)}")
                    if st.session_state.get('debug_mode'):
                        st.exception(e)


