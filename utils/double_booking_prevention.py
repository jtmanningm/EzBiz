# utils/double_booking_prevention.py
"""
Centralized double booking prevention system for Ez Biz application.
Ensures no scheduling conflicts across business and customer portals.
"""

import streamlit as st
from datetime import datetime, date, time, timedelta
from typing import List, Tuple, Optional, Dict, Any
from database.connection import SnowflakeConnection
from utils.business.info import fetch_business_info

# Initialize database connection
snowflake_conn = SnowflakeConnection.get_instance()

def debug_print(msg: str) -> None:
    """Helper function for debug logging with defensive access to debug_mode."""
    if st.session_state.get('debug_mode', False):
        print(f"DEBUG: {msg}")
        st.write(f"DEBUG: {msg}")

class BookingConflict:
    """Represents a booking conflict with detailed information."""
    
    def __init__(self, 
                 conflict_time: time, 
                 conflict_date: date,
                 existing_service: str,
                 existing_customer: str,
                 conflict_duration: int,
                 transaction_id: int):
        self.conflict_time = conflict_time
        self.conflict_date = conflict_date
        self.existing_service = existing_service
        self.existing_customer = existing_customer
        self.conflict_duration = conflict_duration
        self.transaction_id = transaction_id
    
    def get_conflict_message(self) -> str:
        """Get human-readable conflict message."""
        return (f"Time slot conflicts with existing service '{self.existing_service}' "
                f"for {self.existing_customer} at {self.conflict_time.strftime('%I:%M %p')} "
                f"(Duration: {self.conflict_duration} minutes)")

def get_business_hours_for_date(service_date: date) -> Tuple[time, time]:
    """
    Get business hours for a specific date from database.
    
    Args:
        service_date: Date to check business hours for
    
    Returns:
        Tuple of (start_time, end_time) for business hours
    """
    try:
        business_hours_query = """
        SELECT 
            OPERATING_HOURS_START,
            OPERATING_HOURS_END,
            WEEKEND_OPERATING_HOURS_START,
            WEEKEND_OPERATING_HOURS_END
        FROM OPERATIONAL.CARPET.BUSINESS_INFO
        WHERE ACTIVE_STATUS = TRUE
        ORDER BY MODIFIED_DATE DESC
        LIMIT 1
        """
        
        business_hours_result = snowflake_conn.execute_query(business_hours_query)
        
        if business_hours_result:
            business_info = business_hours_result[0]
            # Check if it's weekend (Saturday = 5, Sunday = 6)
            is_weekend = service_date.weekday() >= 5
            
            # Get appropriate hours based on weekday/weekend
            if is_weekend:
                start_time_str = business_info.get('WEEKEND_OPERATING_HOURS_START')
                end_time_str = business_info.get('WEEKEND_OPERATING_HOURS_END')
            else:
                start_time_str = business_info.get('OPERATING_HOURS_START')
                end_time_str = business_info.get('OPERATING_HOURS_END')
            
            # Parse time strings, fallback to defaults if parsing fails
            try:
                business_start = time.fromisoformat(str(start_time_str)) if start_time_str else time(8, 0)
                business_end = time.fromisoformat(str(end_time_str)) if end_time_str else time(17, 0)
            except (ValueError, TypeError):
                business_start = time(8, 0)  # Default 8 AM
                business_end = time(17, 0)   # Default 5 PM
        else:
            # Fallback to defaults if no business hours found
            business_start = time(8, 0)  # Default 8 AM
            business_end = time(17, 0)   # Default 5 PM
            
        return business_start, business_end
        
    except Exception as e:
        debug_print(f"Error fetching business hours: {str(e)}")
        # Fallback to defaults
        return time(8, 0), time(17, 0)

def get_service_duration(service_names: List[str]) -> int:
    """
    Calculate total duration for multiple services.
    
    Args:
        service_names: List of service names
    
    Returns:
        Total duration in minutes
    """
    if not service_names:
        return 60  # Default duration
    
    try:
        placeholders = ','.join(['?' for _ in service_names])
        services_query = f"""
        SELECT SUM(COALESCE(SERVICE_DURATION, 60)) as TOTAL_DURATION
        FROM OPERATIONAL.CARPET.SERVICES
        WHERE SERVICE_NAME IN ({placeholders})
        """
        
        duration_result = snowflake_conn.execute_query(services_query, service_names)
        if duration_result and duration_result[0]['TOTAL_DURATION']:
            return int(duration_result[0]['TOTAL_DURATION'])
        
        return len(service_names) * 60  # Fallback: 60 minutes per service
        
    except Exception as e:
        debug_print(f"Error calculating service duration: {str(e)}")
        return len(service_names) * 60  # Fallback: 60 minutes per service

def get_existing_bookings(service_date: date) -> List[Dict[str, Any]]:
    """
    Get all existing bookings for a specific date.
    
    Args:
        service_date: Date to check bookings for
    
    Returns:
        List of booking dictionaries with scheduling details
    """
    try:
        bookings_query = """
        SELECT 
            ST.ID as TRANSACTION_ID,
            ST.START_TIME,
            ST.END_TIME,
            ST.SERVICE_NAME,
            COALESCE(S.SERVICE_DURATION, 60) as SERVICE_DURATION,
            COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
            ST.STATUS,
            CASE 
                WHEN C.CUSTOMER_ID IS NOT NULL THEN 'Residential'
                WHEN A.ACCOUNT_ID IS NOT NULL THEN 'Commercial'
                ELSE 'Unknown'
            END AS CUSTOMER_TYPE
        FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION ST
        LEFT JOIN OPERATIONAL.CARPET.SERVICES S ON ST.SERVICE_ID = S.SERVICE_ID
        LEFT JOIN OPERATIONAL.CARPET.CUSTOMER C ON ST.CUSTOMER_ID = C.CUSTOMER_ID
        LEFT JOIN OPERATIONAL.CARPET.ACCOUNTS A ON ST.ACCOUNT_ID = A.ACCOUNT_ID
        WHERE ST.SERVICE_DATE = ?
        AND ST.STATUS IN ('SCHEDULED', 'IN_PROGRESS')
        ORDER BY ST.START_TIME
        """
        
        bookings = snowflake_conn.execute_query(bookings_query, [service_date.strftime('%Y-%m-%d')])
        return bookings or []
        
    except Exception as e:
        debug_print(f"Error fetching existing bookings: {str(e)}")
        st.error(f"Error checking existing bookings: {str(e)}")
        return []

def check_time_overlap(
    requested_start: datetime,
    requested_end: datetime,
    existing_start: datetime,
    existing_end: datetime,
    buffer_minutes: int = 15
) -> bool:
    """
    Check if two time periods overlap with optional buffer time.
    
    Args:
        requested_start: Start time of requested booking
        requested_end: End time of requested booking
        existing_start: Start time of existing booking
        existing_end: End time of existing booking
        buffer_minutes: Buffer time in minutes between bookings
    
    Returns:
        True if there's an overlap, False otherwise
    """
    # Add buffer time to existing booking
    buffered_start = existing_start - timedelta(minutes=buffer_minutes)
    buffered_end = existing_end + timedelta(minutes=buffer_minutes)
    
    # Check for overlap: bookings overlap if requested start is before existing end
    # and requested end is after existing start
    return requested_start < buffered_end and requested_end > buffered_start

def validate_business_hours(
    service_date: date,
    service_time: time,
    service_duration: int
) -> Tuple[bool, Optional[str]]:
    """
    Validate that the service time is within business hours.
    
    Args:
        service_date: Date of the service
        service_time: Start time of the service
        service_duration: Duration of service in minutes
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    try:
        business_start, business_end = get_business_hours_for_date(service_date)
        
        # Calculate service end time
        service_start_datetime = datetime.combine(service_date, service_time)
        service_end_datetime = service_start_datetime + timedelta(minutes=service_duration)
        service_end_time = service_end_datetime.time()
        
        # Check if service starts before business hours
        if service_time < business_start:
            return False, f"Service cannot start before business hours ({business_start.strftime('%I:%M %p')})"
        
        # Check if service ends after business hours
        if service_end_time > business_end:
            return False, f"Service would end after business hours ({business_end.strftime('%I:%M %p')}). Please select an earlier time or reduce service duration."
        
        return True, None
        
    except Exception as e:
        debug_print(f"Error validating business hours: {str(e)}")
        return False, f"Error validating business hours: {str(e)}"

def check_for_booking_conflicts(
    service_date: date,
    service_time: time,
    service_names: List[str],
    exclude_transaction_id: Optional[int] = None
) -> Tuple[bool, Optional[str], List[BookingConflict]]:
    """
    Comprehensive check for booking conflicts.
    
    Args:
        service_date: Date of the requested service
        service_time: Start time of the requested service
        service_names: List of service names being scheduled
        exclude_transaction_id: Transaction ID to exclude from conflict checking (for rescheduling)
    
    Returns:
        Tuple of (is_available, error_message, list_of_conflicts)
    """
    try:
        # Calculate total service duration
        total_duration = get_service_duration(service_names)
        
        # Validate business hours first
        business_valid, business_error = validate_business_hours(service_date, service_time, total_duration)
        if not business_valid:
            return False, business_error, []
        
        # Calculate requested booking time range
        requested_start = datetime.combine(service_date, service_time)
        requested_end = requested_start + timedelta(minutes=total_duration)
        
        # Get existing bookings for the date
        existing_bookings = get_existing_bookings(service_date)
        
        conflicts = []
        
        for booking in existing_bookings:
            # Skip if this is the same transaction (for rescheduling)
            if exclude_transaction_id and booking['TRANSACTION_ID'] == exclude_transaction_id:
                continue
            
            # Skip if booking doesn't have valid time data
            if not booking['START_TIME']:
                continue
            
            # Handle different time formats from database
            booking_start_time = booking['START_TIME']
            if isinstance(booking_start_time, str):
                try:
                    hour, minute, second = map(int, booking_start_time.split(':'))
                    booking_start_time = time(hour, minute, second)
                except ValueError:
                    continue  # Skip invalid time format
            elif isinstance(booking_start_time, datetime):
                booking_start_time = booking_start_time.time()
            
            # Calculate booking end time
            booking_duration = int(booking['SERVICE_DURATION'] or 60)
            booking_start = datetime.combine(service_date, booking_start_time)
            booking_end = booking_start + timedelta(minutes=booking_duration)
            
            # Check for time overlap with 15-minute buffer
            if check_time_overlap(requested_start, requested_end, booking_start, booking_end, buffer_minutes=15):
                conflict = BookingConflict(
                    conflict_time=booking_start_time,
                    conflict_date=service_date,
                    existing_service=booking['SERVICE_NAME'],
                    existing_customer=booking['CUSTOMER_NAME'] or 'Unknown Customer',
                    conflict_duration=booking_duration,
                    transaction_id=booking['TRANSACTION_ID']
                )
                conflicts.append(conflict)
        
        if conflicts:
            # Generate detailed error message
            primary_conflict = conflicts[0]
            error_message = primary_conflict.get_conflict_message()
            
            if len(conflicts) > 1:
                error_message += f" (and {len(conflicts) - 1} other conflict{'s' if len(conflicts) > 2 else ''})"
            
            return False, error_message, conflicts
        
        return True, None, []
        
    except Exception as e:
        debug_print(f"Error checking booking conflicts: {str(e)}")
        error_msg = f"Error checking for booking conflicts: {str(e)}"
        st.error(error_msg)
        return False, error_msg, []

def get_available_time_slots_enhanced(
    service_date: date,
    service_names: List[str],
    slot_duration_minutes: int = 30
) -> List[time]:
    """
    Enhanced version of get_available_time_slots with comprehensive conflict checking.
    
    Args:
        service_date: Date to check availability for
        service_names: List of service names to calculate total duration
        slot_duration_minutes: Duration between available slots in minutes
    
    Returns:
        List of available time slots
    """
    try:
        # Get business hours
        business_start, business_end = get_business_hours_for_date(service_date)
        
        # Calculate total service duration
        total_duration = get_service_duration(service_names)
        
        # Generate time slots
        current_slot = datetime.combine(service_date, business_start)
        end_time = datetime.combine(service_date, business_end)
        
        available_slots = []
        
        # Generate slots that can fit the entire service within business hours
        while current_slot + timedelta(minutes=total_duration) <= end_time:
            slot_time = current_slot.time()
            
            # Check if this slot is available (no conflicts)
            is_available, _, _ = check_for_booking_conflicts(
                service_date=service_date,
                service_time=slot_time,
                service_names=service_names
            )
            
            if is_available:
                available_slots.append(slot_time)
            
            # Move to next slot
            current_slot += timedelta(minutes=slot_duration_minutes)
        
        return available_slots
        
    except Exception as e:
        debug_print(f"Error generating available time slots: {str(e)}")
        st.error(f"Error generating available time slots: {str(e)}")
        return []

def validate_recurring_service_availability(
    base_date: date,
    service_time: time,
    service_names: List[str],
    recurrence_pattern: str,
    max_occurrences: int = 24
) -> Tuple[bool, List[str], List[date]]:
    """
    Validate availability for recurring services.
    
    Args:
        base_date: Starting date for recurring service
        service_time: Time for the service
        service_names: List of service names
        recurrence_pattern: "Weekly", "Bi-Weekly", or "Monthly"
        max_occurrences: Maximum number of recurring services to check
    
    Returns:
        Tuple of (all_available, list_of_conflict_messages, list_of_conflict_dates)
    """
    try:
        conflict_messages = []
        conflict_dates = []
        current_date = base_date
        
        for occurrence in range(max_occurrences):
            # Calculate next occurrence date
            if occurrence > 0:  # Skip first occurrence as it's the base_date
                if recurrence_pattern == "Weekly":
                    current_date += timedelta(days=7)
                elif recurrence_pattern == "Bi-Weekly":
                    current_date += timedelta(days=14)
                elif recurrence_pattern == "Monthly":
                    # Handle month increment
                    year = current_date.year
                    month = current_date.month + 1
                    if month > 12:
                        year += 1
                        month = 1
                    try:
                        current_date = current_date.replace(year=year, month=month)
                    except ValueError:
                        # Handle cases like Jan 31 -> Feb 31 (doesn't exist)
                        if month + 1 > 12:
                            next_month = current_date.replace(year=year + 1, month=1, day=1)
                        else:
                            next_month = current_date.replace(year=year, month=month + 1, day=1)
                        current_date = next_month - timedelta(days=1)
            
            # Stop if we've gone beyond 6 months (180 days)
            if (current_date - base_date).days > 180:
                break
            
            # Check availability for this occurrence
            is_available, error_message, conflicts = check_for_booking_conflicts(
                service_date=current_date,
                service_time=service_time,
                service_names=service_names
            )
            
            if not is_available:
                conflict_messages.append(f"{current_date.strftime('%B %d, %Y')}: {error_message}")
                conflict_dates.append(current_date)
        
        all_available = len(conflict_messages) == 0
        return all_available, conflict_messages, conflict_dates
        
    except Exception as e:
        debug_print(f"Error validating recurring service availability: {str(e)}")
        return False, [f"Error validating recurring availability: {str(e)}"], []

# Backward compatibility functions that use the enhanced system
def check_service_availability(
    service_date: date,
    service_time: time,
    selected_services: List[str] = None
) -> Tuple[bool, Optional[str]]:
    """
    Backward compatible version of service availability checking.
    
    Args:
        service_date: Date of the service
        service_time: Start time of the service
        selected_services: List of selected service names
    
    Returns:
        Tuple of (is_available, error_message)
    """
    if not selected_services:
        selected_services = ["Standard Service"]  # Default service
    
    is_available, error_message, _ = check_for_booking_conflicts(
        service_date=service_date,
        service_time=service_time,
        service_names=selected_services
    )
    
    return is_available, error_message

def get_available_time_slots(
    selected_date: date,
    selected_services: List[str] = None
) -> List[time]:
    """
    Backward compatible version of get_available_time_slots.
    
    Args:
        selected_date: Date to get available slots for
        selected_services: List of selected service names
    
    Returns:
        List of available time slots
    """
    if not selected_services:
        selected_services = ["Standard Service"]  # Default service
    
    return get_available_time_slots_enhanced(
        service_date=selected_date,
        service_names=selected_services
    )

__all__ = [
    'BookingConflict',
    'check_for_booking_conflicts',
    'get_available_time_slots_enhanced',
    'validate_recurring_service_availability',
    'check_service_availability',
    'get_available_time_slots',
    'validate_business_hours',
    'get_business_hours_for_date',
    'get_service_duration'
]