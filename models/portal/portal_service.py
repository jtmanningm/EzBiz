# models/portal/portal_service.py
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from typing import Optional, List, Dict, Any
from database.connection import snowflake_conn

@dataclass
class PortalServiceModel:
    """Model for customer portal service bookings"""
    service_id: int
    service_name: str
    service_description: Optional[str]
    cost: float
    duration: int
    deposit_required: bool = False
    deposit_amount: float = 0.0
    service_category: str = ""

    @classmethod
    def from_db_row(cls, row: Dict[str, Any]) -> 'PortalServiceModel':
        return cls(
            service_id=row['SERVICE_ID'],
            service_name=row['SERVICE_NAME'],
            service_description=row.get('SERVICE_DESCRIPTION'),
            cost=float(row['COST']),
            duration=row['SERVICE_DURATION'],
            deposit_required=row.get('DEPOSIT_REQUIRED', False),
            deposit_amount=float(row.get('DEPOSIT_AMOUNT', 0)),
            service_category=row.get('SERVICE_CATEGORY', '')
        )

def get_available_services() -> List[PortalServiceModel]:
    """Get services available for customer booking"""
    query = """
    SELECT 
        SERVICE_ID,
        SERVICE_NAME,
        SERVICE_DESCRIPTION,
        SERVICE_CATEGORY,
        COST,
        SERVICE_DURATION,
        DEPOSIT_REQUIRED,
        DEPOSIT_AMOUNT
    FROM SERVICES
    WHERE ACTIVE_STATUS = TRUE
    AND CUSTOMER_BOOKABLE = TRUE
    ORDER BY SERVICE_CATEGORY, SERVICE_NAME
    """
    try:
        result = snowflake_conn.execute_query(query)
        return [PortalServiceModel.from_db_row(row) for row in result] if result else []
    except Exception as e:
        print(f"Error fetching available services: {str(e)}")
        return []

def get_available_time_slots(service_id: int, date: datetime.date) -> List[time]:
    """Get available time slots for a given service and date"""
    try:
        # Get service duration
        duration_query = """
        SELECT SERVICE_DURATION 
        FROM SERVICES 
        WHERE SERVICE_ID = :1
        AND ACTIVE_STATUS = TRUE
        AND CUSTOMER_BOOKABLE = TRUE
        """
        result = snowflake_conn.execute_query(duration_query, [service_id])
        if not result:
            return []
            
        service_duration = result[0]['SERVICE_DURATION']
        
        # Get booked slots
        bookings_query = """
        SELECT START_TIME, SERVICE_DURATION
        FROM SERVICE_TRANSACTION
        WHERE SERVICE_DATE = :1
        AND STATUS = 'SCHEDULED'
        ORDER BY START_TIME
        """
        
        booked_times = snowflake_conn.execute_query(bookings_query, [date]) or []
        
        # Generate available slots
        business_start = datetime.strptime("08:00", "%H:%M").time()
        business_end = datetime.strptime("17:00", "%H:%M").time()
        slot_duration = 30  # minutes
        
        current_slot = datetime.combine(date, business_start)
        end_time = datetime.combine(date, business_end)
        
        available_slots = []
        while current_slot + timedelta(minutes=service_duration) <= end_time:
            slot_available = True
            slot_end = current_slot + timedelta(minutes=service_duration)
            
            for booking in booked_times:
                booking_start = datetime.combine(date, booking['START_TIME'])
                booking_end = booking_start + timedelta(minutes=booking['SERVICE_DURATION'])
                
                if (current_slot < booking_end and slot_end > booking_start):
                    slot_available = False
                    break
            
            if slot_available:
                available_slots.append(current_slot.time())
            
            current_slot += timedelta(minutes=slot_duration)
            
        return available_slots
        
    except Exception as e:
        print(f"Error getting available slots: {str(e)}")
        return []

def get_upcoming_services(customer_id: int) -> List[Dict[str, Any]]:
    """Get upcoming services for customer"""
    query = """
    SELECT 
        ID as TRANSACTION_ID,
        SERVICE_NAME,
        SERVICE_DATE,
        START_TIME,
        AMOUNT,
        DEPOSIT_REQUIRED,
        DEPOSIT_AMOUNT,
        DEPOSIT_PAID,
        IS_RECURRING,
        RECURRENCE_PATTERN,
        COMMENTS
    FROM SERVICE_TRANSACTION
    WHERE CUSTOMER_ID = :1
    AND SERVICE_DATE >= CURRENT_DATE()
    AND STATUS = 'SCHEDULED'
    ORDER BY SERVICE_DATE, START_TIME
    """
    try:
        return snowflake_conn.execute_query(query, [customer_id]) or []
    except Exception as e:
        print(f"Error fetching upcoming services: {str(e)}")
        return []

def save_booking(
    service_id: int,
    customer_id: int,
    booking_date: datetime.date,
    start_time: time,
    is_recurring: bool = False,
    recurrence_pattern: Optional[str] = None,
    notes: Optional[str] = None
) -> Optional[int]:
    """Save service booking and return transaction ID"""
    try:
        # Get service details
        service_query = """
        SELECT 
            SERVICE_NAME,
            COST,
            DEPOSIT_REQUIRED,
            DEPOSIT_AMOUNT
        FROM SERVICES
        WHERE SERVICE_ID = :1
        AND ACTIVE_STATUS = TRUE
        AND CUSTOMER_BOOKABLE = TRUE
        """
        
        service = snowflake_conn.execute_query(service_query, [service_id])
        if not service:
            return None
            
        # Save booking
        booking_query = """
        INSERT INTO SERVICE_TRANSACTION (
            SERVICE_ID,
            CUSTOMER_ID,
            SERVICE_NAME,
            SERVICE_DATE,
            START_TIME,
            AMOUNT,
            STATUS,
            DEPOSIT_REQUIRED,
            DEPOSIT_AMOUNT,
            DEPOSIT_PAID,
            IS_RECURRING,
            RECURRENCE_PATTERN,
            COMMENTS,
            BOOKING_ORIGIN
        ) VALUES (
            :1, :2, :3, :4, :5, :6, 'SCHEDULED',
            :7, :8, FALSE, :9, :10, :11, 'CUSTOMER_PORTAL'
        )
        RETURNING ID
        """
        
        result = snowflake_conn.execute_query(booking_query, [
            service_id,
            customer_id,
            service[0]['SERVICE_NAME'],
            booking_date,
            start_time,
            service[0]['COST'],
            service[0]['DEPOSIT_REQUIRED'],
            service[0]['DEPOSIT_AMOUNT'],
            is_recurring,
            recurrence_pattern,
            notes
        ])
        
        return result[0]['ID'] if result else None
        
    except Exception as e:
        print(f"Error saving booking: {str(e)}")
        return None