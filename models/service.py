from database.connection import SnowflakeConnection
from typing import Optional, Dict, Any, List, Union, Tuple
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
import streamlit as st
import pandas as pd
import json
from utils.business.info import fetch_business_info
from utils.email import generate_service_scheduled_email
from utils.null_handling import (
    safe_get_value,
    safe_get_float,
    safe_get_int,
    safe_get_string,
    safe_get_bool
)

# Initialize database connection
snowflake_conn = SnowflakeConnection.get_instance()

def debug_print(msg: str) -> None:
    """Helper function for debug logging with defensive access to debug_mode."""
    if st.session_state.get('debug_mode', False):
        print(f"DEBUG: {msg}")
        st.write(f"DEBUG: {msg}")

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
        debug_print("Fetching services from database...")
        results = snowflake_conn.execute_query(query)
        debug_print(f"Query returned {len(results) if results else 0} services")
        
        if results:
            df = pd.DataFrame(results)
            debug_print(f"Created DataFrame with columns: {list(df.columns)}")
            return df
        else:
            debug_print("No services found in database")
            st.warning("No active services found in the database. Please add services in Settings.")
            return pd.DataFrame()
            
    except Exception as e:
        error_msg = f"Error fetching services: {str(e)}"
        debug_print(error_msg)
        st.error(error_msg)
        if st.session_state.get('debug_mode'):
            st.exception(e)
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
    recurrence_pattern: Optional[str] = None,
    customer_data: Optional[Dict[str, Any]] = None
) -> bool:
    """Save service schedule and create initial transaction record with enhanced double booking prevention."""
    try:
        # Final availability check before saving
        from utils.double_booking_prevention import check_for_booking_conflicts
        
        service_list = services if isinstance(services, list) else [services]
        is_available, error_message, conflicts = check_for_booking_conflicts(
            service_date=service_date,
            service_time=service_time,
            service_names=service_list
        )
        
        if not is_available:
            st.error(f"❌ Cannot schedule service: {error_message}")
            return False

        # Get service IDs and calculate total cost
        service_ids = []
        total_cost = 0.0
        base_cost = 0.0
        
        for service_name in service_list:
            service_query = "SELECT SERVICE_ID, COST FROM OPERATIONAL.CARPET.SERVICES WHERE SERVICE_NAME = ?"
            result = snowflake_conn.execute_query(service_query, [service_name])
            if result:
                # Convert numpy.int64 to Python int
                service_ids.append(int(result[0]['SERVICE_ID']))
                service_cost = float(result[0]['COST'])
                total_cost += service_cost
                if len(service_ids) == 1:  # Primary service cost
                    base_cost = service_cost

        if not service_ids:
            st.error("No valid services found")
            return False

        # Convert IDs to Python int if present
        safe_customer_id = int(customer_id) if customer_id is not None else None
        safe_account_id = int(account_id) if account_id is not None else None

        # Get additional service IDs with safe conversion
        service2_id = int(service_ids[1]) if len(service_ids) > 1 else None
        service3_id = int(service_ids[2]) if len(service_ids) > 2 else None

        # Fetch customer details if customer_id is provided
        customer = None
        if safe_customer_id:
            customer_query = "SELECT * FROM OPERATIONAL.CARPET.CUSTOMER WHERE CUSTOMER_ID = ?"
            customer_result = snowflake_conn.execute_query(customer_query, [safe_customer_id])
            if customer_result:
                customer = customer_result[0]

        # Convert monetary values to float
        safe_deposit = float(deposit_amount)
        safe_base_cost = float(base_cost)
        safe_total_cost = float(total_cost)

        # Create initial service transaction record
        query = """
        INSERT INTO OPERATIONAL.CARPET.SERVICE_TRANSACTION (
            CUSTOMER_ID,
            ACCOUNT_ID,
            ADDRESS_ID,
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
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, ?, ?, 'SCHEDULED')
        """
        
        # Get address ID from customer data if available
        address_id = None
        if customer_data and 'service_address_id' in customer_data:
            address_id = customer_data['service_address_id']
        
        params = [
            safe_customer_id,
            safe_account_id,
            address_id,
            service_list[0],  # Primary service name
            int(service_ids[0]),   # Primary service ID
            service2_id,
            service3_id,
            service_date,
            service_time,
            is_recurring,
            recurrence_pattern if is_recurring else None,
            notes,
            safe_deposit,
            safe_base_cost,
            safe_total_cost
        ]

        # Debug logging for params
        if st.session_state.get('debug_mode'):
            debug_print("Query parameters:")
            for i, param in enumerate(params):
                debug_print(f"Param {i}: {param} (type: {type(param)})")
        
        # Execute transaction insert
        snowflake_conn.execute_query(query, params)
        
        # Get the newly created transaction ID
        transaction_id_query = """
        SELECT ID FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION 
        WHERE CUSTOMER_ID = ? AND SERVICE_DATE = ? AND START_TIME = ?
        ORDER BY ID DESC 
        LIMIT 1
        """
        transaction_result = snowflake_conn.execute_query(transaction_id_query, [
            safe_customer_id, service_date, service_time
        ])
        transaction_id = transaction_result[0]['ID'] if transaction_result else None

        # Schedule recurring services if needed
        if is_recurring and recurrence_pattern:
            # Validate recurring availability
            from utils.double_booking_prevention import validate_recurring_service_availability
            all_available, conflict_messages, conflict_dates = validate_recurring_service_availability(
                base_date=service_date,
                service_time=service_time,
                service_names=service_list,
                recurrence_pattern=recurrence_pattern
            )
            
            if not all_available:
                st.warning("⚠️ Some recurring service dates have conflicts:")
                for message in conflict_messages[:3]:  # Show first 3 conflicts
                    st.write(f"• {message}")
                if len(conflict_messages) > 3:
                    st.write(f"... and {len(conflict_messages) - 3} more conflicts")
                st.info("Recurring services will be created for available dates only. Conflicted dates will be skipped.")
            
            schedule_recurring_services(
                services=service_list,
                service_date=service_date,
                service_time=service_time,
                customer_id=safe_customer_id,
                account_id=safe_account_id,
                recurrence_pattern=recurrence_pattern,
                notes=notes
            )

        # Handle email confirmation
        if customer:
            try:
                # Safely access customer email
                customer_email = None
                try:
                    customer_email = customer['EMAIL_ADDRESS']
                except (TypeError, KeyError, AttributeError):
                    try:
                        customer_email = customer.EMAIL_ADDRESS
                    except (AttributeError, TypeError):
                        customer_email = None
                
                if customer_email:
                    # Safely access first and last name
                    first_name = ''
                    try:
                        first_name = customer['FIRST_NAME']
                    except (TypeError, KeyError, AttributeError):
                        try:
                            first_name = customer.FIRST_NAME
                        except (AttributeError, TypeError):
                            first_name = ''
                    
                    last_name = ''
                    try:
                        last_name = customer['LAST_NAME']
                    except (TypeError, KeyError, AttributeError):
                        try:
                            last_name = customer.LAST_NAME
                        except (AttributeError, TypeError):
                            last_name = ''
                    
                    customer_name = f"{first_name} {last_name}".strip()
                    
                    service_details = {
                        'customer_name': customer_name or "Customer",
                        'customer_email': customer_email,
                        'service_type': service_list[0],
                        'date': service_date.strftime('%Y-%m-%d'),
                        'time': service_time.strftime('%I:%M %p'),
                        'total_cost': safe_total_cost,
                        'deposit_amount': safe_deposit,
                        'notes': notes,
                        'is_recurring': is_recurring,
                        'recurrence_pattern': recurrence_pattern
                    }

                    try:
                        # Use already imported functions
                        business_info = fetch_business_info()
                        if business_info:
                            email_status = generate_service_scheduled_email(service_details, business_info)
                            if not email_status.success:
                                debug_print(f"Failed to send confirmation email: {email_status.message}")
                                st.warning("Confirmation email could not be sent, but service was scheduled successfully.")
                    except Exception as e:
                        debug_print(f"Error in email process: {str(e)}")
                        st.warning("Confirmation email could not be sent, but service was scheduled successfully.")

            except Exception as e:
                debug_print(f"Error preparing email data: {str(e)}")
                st.warning("Unable to send confirmation email, but service was scheduled successfully.")

        return transaction_id

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
                # Convert numpy.int64 to Python int
                service_ids.append(int(result[0]['SERVICE_ID']))
                service_cost = float(result[0]['COST'])
                total_cost += service_cost
                if len(service_ids) == 1:  # Primary service cost
                    base_cost = service_cost

        if not service_ids:
            st.error("No valid services found")
            return False

        # Convert IDs to Python int if present
        safe_customer_id = int(customer_id) if customer_id is not None else None
        safe_account_id = int(account_id) if account_id is not None else None

        # Get additional service IDs with safe conversion
        service2_id = int(service_ids[1]) if len(service_ids) > 1 else None
        service3_id = int(service_ids[2]) if len(service_ids) > 2 else None

        # Calculate future dates based on recurrence pattern
        future_dates = []
        current_date = service_date
        six_months_from_now = service_date + timedelta(days=180)  # Limit to 6 months

        # Calculate future dates based on recurrence pattern
        while current_date < six_months_from_now:
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
            
            if current_date < six_months_from_now:
                future_dates.append(current_date)

        # Create transaction records directly for each future date
        for future_date in future_dates:
            # Check availability for this specific date before creating record
            from utils.double_booking_prevention import check_for_booking_conflicts
            
            is_available, _, _ = check_for_booking_conflicts(
                service_date=future_date,
                service_time=service_time,
                service_names=service_list
            )
            
            # Skip this date if there's a conflict
            if not is_available:
                debug_print(f"Skipping recurring service on {future_date} due to conflict")
                continue
            
            # Create recurring service transaction record directly
            query = """
            INSERT INTO OPERATIONAL.CARPET.SERVICE_TRANSACTION (
                CUSTOMER_ID,
                ACCOUNT_ID,
                ADDRESS_ID,
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, ?, ?, 'SCHEDULED')
            """
            
            params = [
                safe_customer_id,
                safe_account_id,
                address_id,  # Use the same address_id from the initial service
                service_list[0],  # Primary service name
                int(service_ids[0]),   # Primary service ID
                service2_id,
                service3_id,
                future_date,  # Use the future date
                service_time,
                True,
                recurrence_pattern,
                notes,
                0.0,  # No deposit for recurring services
                base_cost,
                total_cost
            ]
            
            # Execute transaction insert
            snowflake_conn.execute_query(query, params)

        return True

    except Exception as e:
        import traceback
        print(f"Error scheduling recurring services: {str(e)}")
        print(traceback.format_exc())
        return False

def get_available_time_slots(selected_date: date, selected_services: List[str] = None) -> List[time]:
    """Get available time slots for a given date and selected services using enhanced double booking prevention"""
    from utils.double_booking_prevention import get_available_time_slots_enhanced
    
    if not selected_services:
        selected_services = ["Standard Service"]  # Default service
    
    return get_available_time_slots_enhanced(
        service_date=selected_date,
        service_names=selected_services
    )


def get_transaction_service_details(transaction_id: int) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Get service details for a transaction including any additional services.
    
    Args:
        transaction_id: ID of the transaction
        
    Returns:
        Tuple[Dict, List[Dict]]: Primary service details and list of additional services
    """
    query = """
    SELECT 
        t.ID as TRANSACTION_ID,
        t.SERVICE_ID,
        t.SERVICE_NAME as PRIMARY_SERVICE_NAME,
        COALESCE(t.BASE_SERVICE_COST, 0) as PRIMARY_COST,
        COALESCE(s1.SERVICE_DURATION, 60) as PRIMARY_DURATION,
        NULLIF(t.SERVICE2_ID, 0) as SERVICE2_ID,
        NULLIF(t.SERVICE3_ID, 0) as SERVICE3_ID,
        CASE 
            WHEN t.SERVICE2_ID IS NOT NULL AND t.SERVICE2_ID != 0 
            THEN s2.SERVICE_NAME 
            ELSE NULL 
        END as SERVICE2_NAME,
        CASE 
            WHEN t.SERVICE3_ID IS NOT NULL AND t.SERVICE3_ID != 0 
            THEN s3.SERVICE_NAME 
            ELSE NULL 
        END as SERVICE3_NAME,
        CASE 
            WHEN t.SERVICE2_ID IS NOT NULL AND t.SERVICE2_ID != 0 
            THEN COALESCE(s2.COST, 0) 
            ELSE 0 
        END as SERVICE2_COST,
        CASE 
            WHEN t.SERVICE3_ID IS NOT NULL AND t.SERVICE3_ID != 0 
            THEN COALESCE(s3.COST, 0) 
            ELSE 0 
        END as SERVICE3_COST,
        CASE 
            WHEN t.SERVICE2_ID IS NOT NULL AND t.SERVICE2_ID != 0 
            THEN COALESCE(s2.SERVICE_DURATION, 60)
            ELSE NULL 
        END as SERVICE2_DURATION,
        CASE 
            WHEN t.SERVICE3_ID IS NOT NULL AND t.SERVICE3_ID != 0 
            THEN COALESCE(s3.SERVICE_DURATION, 60)
            ELSE NULL 
        END as SERVICE3_DURATION,
        COALESCE(t.STATUS, 'PENDING') as STATUS,
        t.COMMENTS
    FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION t
    LEFT JOIN OPERATIONAL.CARPET.SERVICES s1 ON t.SERVICE_ID = s1.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES s2 ON t.SERVICE2_ID = s2.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES s3 ON t.SERVICE3_ID = s3.SERVICE_ID
    WHERE t.ID = :1
    """
    
    try:
        result = snowflake_conn.execute_query(query, [transaction_id])
        if not result:
            print(f"No results found for transaction ID: {transaction_id}")
            return None, []

        transaction = result[0]
        
        # Primary service details with null handling
        primary_service = {
            'id': safe_get_int(transaction['SERVICE_ID']),
            'name': safe_get_string(transaction['PRIMARY_SERVICE_NAME']),
            'cost': safe_get_float(transaction['PRIMARY_COST']),
            'duration': safe_get_int(transaction['PRIMARY_DURATION'], 60)
        }
        
        # Additional services with null handling
        additional_services = []
        
        # Only add service2 if it exists and has a valid ID
        service2_id = transaction.get('SERVICE2_ID')
        if service2_id and service2_id != 0:
            service2_name = safe_get_string(transaction['SERVICE2_NAME'])
            if service2_name:  # Only add if we have a valid name
                additional_services.append({
                    'id': safe_get_int(service2_id),
                    'name': service2_name,
                    'cost': safe_get_float(transaction['SERVICE2_COST']),
                    'duration': safe_get_int(transaction['SERVICE2_DURATION'], 60)
                })
        
        # Only add service3 if it exists and has a valid ID
        service3_id = transaction.get('SERVICE3_ID')
        if service3_id and service3_id != 0:
            service3_name = safe_get_string(transaction['SERVICE3_NAME'])
            if service3_name:  # Only add if we have a valid name
                additional_services.append({
                    'id': safe_get_int(service3_id),
                    'name': service3_name,
                    'cost': safe_get_float(transaction['SERVICE3_COST']),
                    'duration': safe_get_int(transaction['SERVICE3_DURATION'], 60)
                })

        if not primary_service['name']:
            print(f"Warning: Primary service name is missing for transaction {transaction_id}")

        return primary_service, additional_services

    except Exception as e:
        print(f"Error getting transaction service details: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        st.error("Error retrieving service details")
        return None, []

def check_service_availability(service_date: date, service_time: time, selected_services: List[str] = None) -> Tuple[bool, Optional[str]]:
    """Check if the selected time slot is available using enhanced double booking prevention"""
    from utils.double_booking_prevention import check_for_booking_conflicts
    
    if not selected_services:
        selected_services = ["Standard Service"]  # Default service
    
    is_available, error_message, _ = check_for_booking_conflicts(
        service_date=service_date,
        service_time=service_time,
        service_names=selected_services
    )
    
    return is_available, error_message

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