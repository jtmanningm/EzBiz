from database.connection import SnowflakeConnection
from typing import Optional, Dict, Any, List, Union, Tuple
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
import streamlit as st
import pandas as pd
import json

# Initialize database connection
snowflake_conn = SnowflakeConnection.get_instance()

@dataclass
class ServiceModel:
    service_id: Optional[int] = None
    customer_id: int = 0
    service_name: str = ""
    service_date: datetime = None
    service_time: str = ""
    is_recurring: bool = False
    recurrence_pattern: Optional[str] = None
    notes: Optional[str] = None
    deposit: Optional[float] = 0.0
    deposit_paid: bool = False
    status: str = "SCHEDULED"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "customer_id": self.customer_id,
            "service_name": self.service_name,
            "service_date": self.service_date,
            "service_time": self.service_time,
            "is_recurring": self.is_recurring,
            "recurrence_pattern": self.recurrence_pattern,
            "notes": self.notes,
            "deposit": self.deposit,
            "deposit_paid": self.deposit_paid,
            "status": self.status
        }

@st.cache_data
def fetch_services() -> pd.DataFrame:
    """Fetch all active services from the SERVICES table."""
    query = """
    SELECT 
        SERVICE_ID,
        SERVICE_NAME,
        SERVICE_CATEGORY,
        SERVICE_DESCRIPTION,
        COST,
        ACTIVE_STATUS
    FROM OPERATIONAL.CARPET.SERVICES
    WHERE ACTIVE_STATUS = TRUE
    ORDER BY SERVICE_CATEGORY, SERVICE_NAME
    """
    try:
        results = snowflake_conn.execute_query(query)
        return pd.DataFrame(results) if results else pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching services: {str(e)}")
        return pd.DataFrame()

def fetch_customer_services(customer_id: int) -> pd.DataFrame:
    """Fetch all services for a customer"""
    query = """
    SELECT 
        ST.ID as SERVICE_ID,
        ST.SERVICE_NAME,
        ST.SERVICE_DATE,
        ST.START_TIME as SERVICE_TIME,
        ST.IS_RECURRING,
        ST.RECURRENCE_PATTERN,
        ST.COMMENTS as NOTES,
        ST.DEPOSIT,
        ST.DEPOSIT_PAID,
        S.COST,
        S.SERVICE_CATEGORY
    FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION ST
    LEFT JOIN OPERATIONAL.CARPET.SERVICES S ON ST.SERVICE_NAME = S.SERVICE_NAME
    WHERE ST.CUSTOMER_ID = ?
    ORDER BY ST.SERVICE_DATE DESC, ST.START_TIME ASC
    """
    try:
        result = snowflake_conn.execute_query(query, [customer_id])
        return pd.DataFrame(result) if result else pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching customer services: {str(e)}")
        return pd.DataFrame()

def update_service_status(service_id: int, status: str) -> bool:
    """Update service status"""
    try:
        query = """
        UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
        SET STATUS = ?,
            LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
        WHERE ID = ?
        """
        snowflake_conn.execute_query(query, [status, service_id])
        return True
    except Exception as e:
        st.error(f"Error updating service status: {str(e)}")
        return False

def get_service_id_by_name(service_name: str) -> Optional[int]:
    """Get service ID from service name"""
    query = """
    SELECT SERVICE_ID 
    FROM OPERATIONAL.CARPET.SERVICES 
    WHERE SERVICE_NAME = ?
    """
    try:
        result = snowflake_conn.execute_query(query, [service_name])
        return result[0]['SERVICE_ID'] if result else None
    except Exception as e:
        st.error(f"Error getting service ID: {str(e)}")
        return None

def fetch_upcoming_services(start_date: date, end_date: date) -> pd.DataFrame:
    """Fetch upcoming services scheduled between the specified dates"""
    query = """
    SELECT 
        ST.ID as SERVICE_ID,
        COALESCE(C.CUSTOMER_ID, A.ACCOUNT_ID) AS CUSTOMER_OR_ACCOUNT_ID,
        COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
        ST.SERVICE_NAME,
        ST.SERVICE_DATE,
        ST.START_TIME as SERVICE_TIME,
        ST.COMMENTS as NOTES,
        ST.DEPOSIT,
        ST.DEPOSIT_PAID,
        ST.IS_RECURRING,
        ST.RECURRENCE_PATTERN,
        CASE 
            WHEN C.CUSTOMER_ID IS NOT NULL THEN 'Residential'
            ELSE 'Commercial'
        END AS SERVICE_TYPE,
        S.SERVICE_CATEGORY,
        S.SERVICE_DESCRIPTION,
        S.COST
    FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION ST
    LEFT JOIN OPERATIONAL.CARPET.CUSTOMER C ON ST.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN OPERATIONAL.CARPET.ACCOUNTS A ON ST.ACCOUNT_ID = A.ACCOUNT_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES S ON ST.SERVICE_NAME = S.SERVICE_NAME
    WHERE ST.SERVICE_DATE BETWEEN ? AND ?
    AND ST.STATUS = 'SCHEDULED'
    ORDER BY ST.SERVICE_DATE, ST.START_TIME
    """

    try:
        results = snowflake_conn.execute_query(query, [
            start_date.strftime('%Y-%m-%d'), 
            end_date.strftime('%Y-%m-%d')
        ])
        return pd.DataFrame(results) if results else pd.DataFrame()
    except Exception as e:
        st.error(f"Error fetching upcoming services: {str(e)}")
        return pd.DataFrame()

def save_service_schedule(
    services: List[str],
    service_date: date,
    service_time: time,
    customer_id: Optional[int] = None,
    account_id: Optional[int] = None,
    deposit_amount: float = 0.0,
    notes: Optional[str] = None,
    is_recurring: bool = False,
    recurrence_pattern: Optional[str] = None
) -> bool:
    """Save service schedule and create initial transaction record."""
    try:
        # Convert single service to list for consistent handling
        service_list = services if isinstance(services, list) else [services]

        # Get service IDs and calculate total cost
        service_ids = []
        total_cost = 0.0
        base_cost = 0.0
        
        for service_name in service_list:
            service_query = "SELECT SERVICE_ID, COST FROM OPERATIONAL.CARPET.SERVICES WHERE SERVICE_NAME = ?"
            result = snowflake_conn.execute_query(service_query, [service_name])
            if result:
                service_ids.append(result[0]['SERVICE_ID'])
                service_cost = float(result[0]['COST'])
                total_cost += service_cost
                if len(service_ids) == 1:  # Primary service cost
                    base_cost = service_cost

        if not service_ids:
            st.error("No valid services found")
            return False

        # Create initial service transaction record
        query = """
        INSERT INTO OPERATIONAL.CARPET.SERVICE_TRANSACTION (
            CUSTOMER_ID,
            ACCOUNT_ID,
            SERVICE_NAME,
            SERVICE_ID,
            SERVICE2_ID,
            SERVICE3_ID,
            SERVICE_DATE,
            START_TIME,
            IS_RECURRING,
            RECURRENCE_PATTERN,
            COMMENTS,
            DEPOSIT,
            DEPOSIT_PAID,
            BASE_SERVICE_COST,
            AMOUNT,
            STATUS
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, ?, ?, 'SCHEDULED')
        """
        
        params = [
            customer_id,
            account_id,
            service_list[0],  # Primary service name
            service_ids[0],   # Primary service ID
            service_ids[1] if len(service_ids) > 1 else None,
            service_ids[2] if len(service_ids) > 2 else None,
            service_date,
            service_time,
            is_recurring,
            recurrence_pattern if is_recurring else None,
            notes,
            float(deposit_amount),
            base_cost,
            total_cost
        ]
        
        # Execute transaction insert
        snowflake_conn.execute_query(query, params)

        # Schedule recurring services if needed
        if is_recurring and recurrence_pattern:
            schedule_recurring_services(
                services=service_list,
                service_date=service_date,
                service_time=service_time,
                customer_id=customer_id,
                account_id=account_id,
                recurrence_pattern=recurrence_pattern,
                notes=notes
            )

        return True

    except Exception as e:
        st.error(f"Error saving service schedule: {str(e)}")
        if st.session_state.get('debug_mode'):
            st.error(f"Debug - Full error details:")
            st.error(f"Error type: {type(e).__name__}")
            st.error(f"Error message: {str(e)}")
            import traceback
            st.error(f"Traceback: {traceback.format_exc()}")
        return False

def schedule_recurring_services(
    services: List[str],
    service_date: date,
    service_time: time,
    recurrence_pattern: str,
    customer_id: Optional[int] = None,
    account_id: Optional[int] = None,
    notes: Optional[str] = None
) -> bool:
    """Schedule recurring services for up to one year."""
    try:
        if not customer_id and not account_id:
            raise ValueError("Either customer_id or account_id must be provided")

        future_dates = []
        current_date = service_date
        one_year_from_now = service_date + timedelta(days=365)

        # Calculate future dates based on recurrence pattern
        while current_date < one_year_from_now:
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
                    if month + 1 > 12:
                        next_month = current_date.replace(year=year + 1, month=1, day=1)
                    else:
                        next_month = current_date.replace(year=year, month=month + 1, day=1)
                    current_date = next_month - timedelta(days=1)
            
            if current_date < one_year_from_now:
                future_dates.append(current_date)

        # Create transaction records for each future date
        for future_date in future_dates:
            save_service_schedule(
                services=services,
                service_date=future_date,
                service_time=service_time,
                customer_id=customer_id,
                account_id=account_id,
                deposit_amount=0.0,  # No deposit for recurring services
                notes=notes,
                is_recurring=True,
                recurrence_pattern=recurrence_pattern
            )

        return True

    except Exception as e:
        st.error(f"Error scheduling recurring services: {str(e)}")
        return False

def get_available_time_slots(service_date: date, selected_services: List[str] = None) -> List[time]:
    """Get available time slots considering service duration"""
    try:
        # Calculate total duration for selected services
        total_duration = 60  # Default duration
        if selected_services:
            duration_query = """
            SELECT SUM(SERVICE_DURATION) as TOTAL_DURATION
            FROM OPERATIONAL.CARPET.SERVICES
            WHERE SERVICE_NAME IN ({})
            """.format(','.join(['?' for _ in selected_services]))
            duration_result = snowflake_conn.execute_query(duration_query, selected_services)
            if duration_result and duration_result[0]['TOTAL_DURATION']:
                total_duration = int(duration_result[0]['TOTAL_DURATION'])

        # Get booked slots from SERVICE_TRANSACTION
        booked_slots_query = """
        SELECT 
            ST.START_TIME,
            COALESCE(S.SERVICE_DURATION, 60) as SERVICE_DURATION
        FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION ST
        LEFT JOIN OPERATIONAL.CARPET.SERVICES S ON ST.SERVICE_NAME = S.SERVICE_NAME
        WHERE ST.SERVICE_DATE = ?
        AND ST.STATUS IN ('SCHEDULED', 'IN_PROGRESS')
        """
        booked_slots = snowflake_conn.execute_query(booked_slots_query, [service_date.strftime('%Y-%m-%d')])

        # Generate all possible time slots
        available_slots = []
        current_time = datetime.combine(service_date, time(8, 0))  # Start at 8 AM
        end_time = datetime.combine(service_date, time(17, 0))     # End at 5 PM

        while current_time <= end_time:
            slot_available = True
            service_end_time = current_time + timedelta(minutes=total_duration)

            # Check if this slot would end after business hours
            if service_end_time.time() > time(17, 0):
                break

            # Check for conflicts with booked slots
            if booked_slots:
                for booked in booked_slots:
                    booked_time = booked['START_TIME']
                    if isinstance(booked_time, str):
                        hour, minute, second = map(int, booked_time.split(':'))
                        booked_time = time(hour, minute, second)
                    elif isinstance(booked_time, datetime):
                        booked_time = booked_time.time()
                    
                    booked_duration = int(booked['SERVICE_DURATION'])
                    booked_start = datetime.combine(service_date, booked_time)
                    booked_end = booked_start + timedelta(minutes=booked_duration)

                    if (current_time < booked_end and service_end_time > booked_start):
                        slot_available = False
                        break

            if slot_available:
                available_slots.append(current_time.time())

            current_time += timedelta(minutes=30)

        return available_slots

    except Exception as e:
        st.error(f"Error getting time slots: {str(e)}")
        st.error(f"Error details: {type(e)}")  # Add more error details
        return []

def check_service_availability(service_date: date, service_time: time, selected_services: List[str] = None) -> Tuple[bool, Optional[str]]:
    """Check if the selected time slot is available considering service duration"""
    try:
        # Get total service duration
        total_duration = 60  # Default duration
        if selected_services:
            duration_query = """
            SELECT SUM(SERVICE_DURATION) as TOTAL_DURATION
            FROM OPERATIONAL.CARPET.SERVICES
            WHERE SERVICE_NAME IN ({})
            """.format(','.join(['?' for _ in selected_services]))
            duration_result = snowflake_conn.execute_query(duration_query, selected_services)
            if duration_result and duration_result[0]['TOTAL_DURATION']:
                total_duration = int(duration_result[0]['TOTAL_DURATION'])

        # Calculate the requested end time
        requested_start = datetime.combine(service_date, service_time)
        requested_end = requested_start + timedelta(minutes=total_duration)

        # Check business hours
        if service_time < time(8, 0) or requested_end.time() > time(17, 0):
            return False, "Service must be scheduled between 8 AM and 5 PM."

        # Check for overlapping bookings
        booked_slots_query = """
        SELECT 
            ST.START_TIME,
            COALESCE(S.SERVICE_DURATION, 60) as SERVICE_DURATION
        FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION ST
        LEFT JOIN OPERATIONAL.CARPET.SERVICES S ON ST.SERVICE_NAME = S.SERVICE_NAME
        WHERE ST.SERVICE_DATE = ?
        AND ST.STATUS IN ('SCHEDULED', 'IN_PROGRESS')
        """
        booked_slots = snowflake_conn.execute_query(booked_slots_query, [service_date.strftime('%Y-%m-%d')])

        if booked_slots:
            for booked in booked_slots:
                # Handle different time formats
                booked_time = booked['START_TIME']
                if isinstance(booked_time, str):
                    hour, minute, second = map(int, booked_time.split(':'))
                    booked_time = time(hour, minute, second)
                elif isinstance(booked_time, datetime):
                    booked_time = booked_time.time()

                booked_duration = int(booked['SERVICE_DURATION'])
                booked_start = datetime.combine(service_date, booked_time)
                booked_end = booked_start + timedelta(minutes=booked_duration)

                # Check for overlap
                if (requested_start < booked_end and requested_end > booked_start):
                    formatted_time = booked_time.strftime('%I:%M %p')
                    return False, f"Time slot conflicts with existing service at {formatted_time}."

        return True, None

    except Exception as e:
        st.error(f"Error checking service availability: {str(e)}")
        st.error(f"Error details: {type(e)}")
        return False, str(e)

__all__ = [
    "ServiceModel",
    "fetch_services",
    "fetch_upcoming_services",
    "get_available_time_slots",
    "check_service_availability",
    "save_service_schedule",
    "schedule_recurring_services",
    "fetch_customer_services",
    "update_service_status",
    "get_service_id_by_name"
]