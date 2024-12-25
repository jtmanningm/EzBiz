from database.connection import SnowflakeConnection
from typing import Optional, Dict, Any, List, Union, Tuple
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
import streamlit as st
import pandas as pd

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
    status: str = "Scheduled"

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
    snowflake_conn = SnowflakeConnection.get_instance()
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
        US.SERVICE_ID,
        US.SERVICE_NAME,
        US.SERVICE_DATE,
        US.SERVICE_TIME,
        US.IS_RECURRING,
        US.RECURRENCE_PATTERN,
        US.NOTES,
        US.DEPOSIT,
        US.DEPOSIT_PAID,
        S.COST,
        S.SERVICE_CATEGORY
    FROM OPERATIONAL.CARPET.UPCOMING_SERVICES US
    LEFT JOIN OPERATIONAL.CARPET.SERVICES S ON US.SERVICE_NAME = S.SERVICE_NAME
    WHERE US.CUSTOMER_ID = ?
    ORDER BY US.SERVICE_DATE DESC, US.SERVICE_TIME ASC
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
        UPDATE OPERATIONAL.CARPET.UPCOMING_SERVICES
        SET STATUS = ?
        WHERE SERVICE_ID = ?
        """
        snowflake_conn.execute_query(query, [status, service_id])
        return True
    except Exception as e:
        st.error(f"Error updating service status: {str(e)}")
        return False

def get_service_id_by_name(service_name: str) -> Optional[int]:
    """Get service ID from service name"""
    snowflake_conn = SnowflakeConnection.get_instance()
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
    """
    Fetch upcoming services scheduled between the specified dates from UPCOMING_SERVICES table.
    """
    snowflake_conn = SnowflakeConnection.get_instance()
    query = """
    SELECT 
        US.SERVICE_ID,
        COALESCE(C.CUSTOMER_ID, A.ACCOUNT_ID) AS CUSTOMER_OR_ACCOUNT_ID,
        COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
        US.SERVICE_NAME,
        US.SERVICE_DATE,
        US.SERVICE_TIME,
        US.NOTES,
        US.DEPOSIT,
        US.DEPOSIT_PAID,
        US.IS_RECURRING,
        US.RECURRENCE_PATTERN,
        CASE 
            WHEN C.CUSTOMER_ID IS NOT NULL THEN 'Residential'
            ELSE 'Commercial'
        END AS SERVICE_TYPE,
        S.SERVICE_CATEGORY,
        S.SERVICE_DESCRIPTION,
        S.COST
    FROM OPERATIONAL.CARPET.UPCOMING_SERVICES US
    LEFT JOIN OPERATIONAL.CARPET.CUSTOMER C ON US.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN OPERATIONAL.CARPET.ACCOUNTS A ON US.ACCOUNT_ID = A.ACCOUNT_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES S ON US.SERVICE_NAME = S.SERVICE_NAME
    WHERE US.SERVICE_DATE BETWEEN ? AND ?
    ORDER BY US.SERVICE_DATE, US.SERVICE_TIME
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
    customer_id: int,
    services: List[str],
    service_date: date,
    service_time: time,
    deposit_amount: float = 0.0,
    notes: Optional[str] = None,
    is_recurring: bool = False,
    recurrence_pattern: Optional[str] = None
) -> bool:
    """Save service schedule and create initial transaction record."""
    snowflake_conn = SnowflakeConnection.get_instance()
    try:
        # Convert single service to list for consistent handling
        service_list = services if isinstance(services, list) else [services]
        
        # Insert main schedule with service details
        upcoming_service_query = """
        INSERT INTO OPERATIONAL.CARPET.UPCOMING_SERVICES (
            CUSTOMER_ID, 
            SERVICE_NAME, 
            SERVICE_DATE, 
            SERVICE_TIME,
            IS_RECURRING, 
            RECURRENCE_PATTERN, 
            NOTES, 
            DEPOSIT,
            DEPOSIT_PAID
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """
        
        # Use first service as primary service
        params = [
            customer_id,
            service_list[0],  # Primary service
            service_date.strftime('%Y-%m-%d'),
            service_time.strftime('%H:%M:%S'),
            is_recurring,
            recurrence_pattern if is_recurring else None,
            notes,
            float(deposit_amount),
            False
        ]
        
        # Save upcoming service
        snowflake_conn.execute_query(upcoming_service_query, params)
        
        # Get service IDs for the selected services
        service_ids = []
        for service_name in service_list:
            service_id_query = "SELECT SERVICE_ID FROM OPERATIONAL.CARPET.SERVICES WHERE SERVICE_NAME = ?"
            result = snowflake_conn.execute_query(service_id_query, [service_name])
            if result:
                service_ids.append(result[0]['SERVICE_ID'])

        # Create initial service transaction record
        transaction_query = """
        INSERT INTO OPERATIONAL.CARPET.SERVICE_TRANSACTION (
            CUSTOMER_ID,
            TRANSACTION_DATE,
            TRANSACTION_TIME,
            SERVICE_ID,
            SERVICE2_ID,
            SERVICE3_ID,
            AMOUNT,
            DEPOSIT,
            DEPOSIT_PAYMENT_METHOD
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """

        # Calculate total amount from services
        total_amount_query = """
        SELECT SUM(COST) as TOTAL_COST
        FROM OPERATIONAL.CARPET.SERVICES
        WHERE SERVICE_NAME IN ({})
        """.format(','.join(['?'] * len(service_list)))

        total_result = snowflake_conn.execute_query(total_amount_query, service_list)
        total_amount = float(total_result[0]['TOTAL_COST']) if total_result else 0.0

        # Prepare service IDs for transaction record
        service_id_params = [
            service_ids[0] if len(service_ids) > 0 else None,
            service_ids[1] if len(service_ids) > 1 else None,
            service_ids[2] if len(service_ids) > 2 else None
        ]

        # Save initial transaction record
        transaction_params = [
            customer_id,
            service_date.strftime('%Y-%m-%d'),
            service_time.strftime('%H:%M:%S'),
            *service_id_params,  # Unpacks SERVICE_ID, SERVICE2_ID, SERVICE3_ID
            total_amount,
            float(deposit_amount),
            'Pending'  # Initial deposit payment method
        ]
        
        snowflake_conn.execute_query(transaction_query, transaction_params)
        
        # Schedule recurring services if needed
        if is_recurring:
            future_dates = []
            current_date = service_date

            # Calculate next 6 occurrences
            for _ in range(6):
                if recurrence_pattern == "Weekly":
                    current_date = current_date + timedelta(days=7)
                elif recurrence_pattern == "Bi-Weekly":
                    current_date = current_date + timedelta(days=14)
                elif recurrence_pattern == "Monthly":
                    current_date = current_date + timedelta(days=30)
                future_dates.append(current_date)

            # Schedule future services (only UPCOMING_SERVICES records)
            for future_date in future_dates:
                recurring_params = [
                    customer_id,
                    service_list[0],
                    future_date.strftime('%Y-%m-%d'),
                    service_time.strftime('%H:%M:%S'),
                    is_recurring,
                    recurrence_pattern,
                    notes,
                    0.0,  # No deposit for recurring services
                    False
                ]
                snowflake_conn.execute_query(upcoming_service_query, recurring_params)
            
        return True

    except Exception as e:
        st.error(f"Error saving service schedule: {str(e)}")
        return False

def schedule_recurring_services(
    customer_id: int,
    services: List[str],
    service_date: date,
    service_time: time,
    recurrence_pattern: str,
    notes: Optional[str] = None
) -> bool:
    """Schedule recurring services based on the specified recurrence pattern."""
    try:
        future_dates = []
        current_date = service_date

        # Calculate next 6 occurrences based on recurrence pattern
        for _ in range(6):
            if recurrence_pattern == "Weekly":
                current_date += timedelta(days=7)
            elif recurrence_pattern == "Bi-Weekly":
                current_date += timedelta(days=14)
            elif recurrence_pattern == "Monthly":
                current_date = current_date.replace(
                    month=(current_date.month % 12) + 1, 
                    day=min(current_date.day, 28)  # Handle February and shorter months
                )
            future_dates.append(current_date)

        # Schedule future services
        for future_date in future_dates:
            save_service_schedule(
                customer_id=customer_id,
                services=services,
                service_date=future_date,
                service_time=service_time,
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

        # Get booked slots
        booked_slots_query = """
        SELECT 
            US.SERVICE_TIME,
            COALESCE(S.SERVICE_DURATION, 60) as SERVICE_DURATION
        FROM OPERATIONAL.CARPET.UPCOMING_SERVICES US
        LEFT JOIN OPERATIONAL.CARPET.SERVICES S ON US.SERVICE_NAME = S.SERVICE_NAME
        WHERE US.SERVICE_DATE = ?
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
                    # Handle different time formats
                    booked_time = booked['SERVICE_TIME']
                    if isinstance(booked_time, str):
                        # If time comes as string like '08:00:00'
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
            US.SERVICE_TIME,
            COALESCE(S.SERVICE_DURATION, 60) as SERVICE_DURATION
        FROM OPERATIONAL.CARPET.UPCOMING_SERVICES US
        LEFT JOIN OPERATIONAL.CARPET.SERVICES S ON US.SERVICE_NAME = S.SERVICE_NAME
        WHERE US.SERVICE_DATE = ?
        """
        booked_slots = snowflake_conn.execute_query(booked_slots_query, [service_date.strftime('%Y-%m-%d')])

        if booked_slots:
            for booked in booked_slots:
                # Handle different time formats
                booked_time = booked['SERVICE_TIME']
                if isinstance(booked_time, str):
                    # If time comes as string like '08:00:00'
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
        st.error(f"Error details: {type(e)}")  # Add more error details
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