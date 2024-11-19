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
    """Save service schedule with services"""
    snowflake_conn = SnowflakeConnection.get_instance()
    try:
        # Convert single service to list for consistent handling
        service_list = services if isinstance(services, list) else [services]
        
        # Insert main schedule with service details
        query = """
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
        
        snowflake_conn.execute_query(query, params)
        
        # Schedule recurring services if needed
        if is_recurring:
            schedule_recurring_services(
                customer_id=customer_id,
                services=service_list,
                service_date=service_date,
                service_time=service_time,
                recurrence_pattern=recurrence_pattern,
                notes=notes
            )
            
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
    """Schedule recurring services based on pattern"""
    try:
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

def get_available_time_slots(service_date: date, service_duration: int = 60) -> List[time]:
    """Get available time slots for a given date"""
    try:
        # Get booked slots from both tables
        upcoming_query = """
        SELECT SERVICE_TIME
        FROM OPERATIONAL.CARPET.UPCOMING_SERVICES
        WHERE SERVICE_DATE = ?
        """
        
        transaction_query = """
        SELECT START_TIME as SERVICE_TIME
        FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION
        WHERE TRANSACTION_DATE = ?
        """
        
        upcoming_slots = snowflake_conn.execute_query(upcoming_query, [service_date.strftime('%Y-%m-%d')])
        transaction_slots = snowflake_conn.execute_query(transaction_query, [service_date.strftime('%Y-%m-%d')])
        
        # Combine booked slots from both sources
        booked_slots = []
        if upcoming_slots:
            booked_slots.extend(upcoming_slots)
        if transaction_slots:
            booked_slots.extend(transaction_slots)

        # Generate all possible time slots from 8 AM to 5 PM with 30-minute intervals
        all_slots = []
        current_time = datetime.combine(service_date, time(8, 0))  # Start at 8 AM
        end_time = datetime.combine(service_date, time(17, 0))     # Last slot at 5 PM

        while current_time <= end_time:
            slot_available = True
            slot_time = current_time.time()

            if booked_slots:
                for booked in booked_slots:
                    booked_time = booked['SERVICE_TIME']
                    if isinstance(booked_time, str):
                        booked_time = datetime.strptime(booked_time, '%H:%M:%S').time()
                    elif isinstance(booked_time, time):
                        booked_time = booked_time
                    
                    booked_dt = datetime.combine(service_date, booked_time)
                    service_end = current_time + timedelta(minutes=60)
                    booked_end = booked_dt + timedelta(minutes=60)
                    
                    if (current_time <= booked_dt < service_end) or \
                       (current_time < booked_end <= service_end) or \
                       (booked_dt <= current_time < booked_end):
                        slot_available = False
                        break

            if slot_available:
                all_slots.append(slot_time)

            current_time += timedelta(minutes=30)

        return all_slots

    except Exception as e:
        st.error(f"Error getting time slots: {str(e)}")
        return []

def check_service_availability(service_date: date, service_time: time, service_duration: int = 60) -> Tuple[bool, Optional[str]]:
    """Check if a specific time slot is available"""
    try:
        requested_start = datetime.combine(service_date, service_time)
        requested_end = requested_start + timedelta(minutes=60)  # Fixed 1-hour block
        
        if service_time < time(8, 0) or service_time > time(17, 0):
            return False, "Service time must be between 8 AM and 5 PM"

        # Check both upcoming and completed services
        upcoming_query = """
        SELECT SERVICE_TIME
        FROM OPERATIONAL.CARPET.UPCOMING_SERVICES
        WHERE SERVICE_DATE = ?
        """
        
        transaction_query = """
        SELECT START_TIME as SERVICE_TIME
        FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION
        WHERE TRANSACTION_DATE = ?
        """
        
        existing_services = []
        upcoming_services = snowflake_conn.execute_query(upcoming_query, [service_date.strftime('%Y-%m-%d')])
        completed_services = snowflake_conn.execute_query(transaction_query, [service_date.strftime('%Y-%m-%d')])
        
        if upcoming_services:
            existing_services.extend(upcoming_services)
        if completed_services:
            existing_services.extend(completed_services)
        
        for service in existing_services:
            booked_time = service['SERVICE_TIME']
            if isinstance(booked_time, str):
                booked_time = datetime.strptime(booked_time, '%H:%M:%S').time()
            elif isinstance(booked_time, time):
                booked_time = booked_time
                
            booked_start = datetime.combine(service_date, booked_time)
            booked_end = booked_start + timedelta(minutes=60)

            if (requested_start < booked_end and requested_end > booked_start):
                return False, f"Time slot conflicts with existing service at {booked_time.strftime('%I:%M %p')}"

        return True, None

    except Exception as e:
        st.error(f"Error checking service availability: {str(e)}")
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