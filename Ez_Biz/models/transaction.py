from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from datetime import datetime
import streamlit as st
import pandas as pd
from database.connection import SnowflakeConnection
from models.service import fetch_services
import json

@dataclass
class TransactionModel:
    transaction_id: Optional[int] = None
    service_id: int = 0
    customer_id: int = 0
    amount: float = 0.0
    payment_type: str = "Cash"
    transaction_date: datetime = None
    status: str = "Pending"
    notes: Optional[str] = None
    is_deposit: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "service_id": self.service_id,
            "customer_id": self.customer_id,
            "amount": self.amount,
            "payment_type": self.payment_type,
            "transaction_date": self.transaction_date or datetime.now(),
            "status": self.status,
            "notes": self.notes,
            "is_deposit": self.is_deposit
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TransactionModel':
        return cls(
            transaction_id=data.get('TRANSACTION_ID'),
            service_id=data.get('SERVICE_ID', 0),
            customer_id=data.get('CUSTOMER_ID', 0),
            amount=data.get('AMOUNT', 0.0),
            payment_type=data.get('PAYMENT_TYPE', 'Cash'),
            transaction_date=data.get('TRANSACTION_DATE', datetime.now()),
            status=data.get('STATUS', 'Pending'),
            notes=data.get('NOTES'),
            is_deposit=data.get('IS_DEPOSIT', False)
        )

# Get database connection
snowflake_conn = SnowflakeConnection.get_instance()

def get_service_costs(service_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    """
    Get costs and details for specified services from the database.
    """
    if not service_ids:
        return {}
        
    placeholders = ', '.join(['?' for _ in service_ids])
    query = f"""
    SELECT SERVICE_ID, SERVICE_NAME, COST
    FROM OPERATIONAL.CARPET.SERVICES
    WHERE SERVICE_ID IN ({placeholders})
    """
    
    try:
        results = snowflake_conn.execute_query(query, service_ids)
        return {
            row['SERVICE_ID']: {
                'name': row['SERVICE_NAME'],
                'cost': float(row['COST'])
            } for row in results
        }
    except Exception as e:
        st.error(f"Error fetching service costs: {str(e)}")
        return {}

def get_additional_services(service1_id: int) -> Tuple[Optional[int], Optional[int], float]:
    """
    Get additional services and calculate total cost while preserving original service cost.
    """
    # Get the selected service cost from session state
    selected_service = st.session_state.get('selected_service', {})
    primary_cost = selected_service.get('COST', 0.0)
    total_cost = primary_cost
    
    # Fetch available services for additional selections
    services_df = fetch_services()
    if services_df.empty:
        return None, None, total_cost

    # Filter out the primary service from available options
    available_services = services_df[services_df['SERVICE_ID'] != service1_id]
    
    # Service 2 Selection
    service2_id = None
    col1, col2 = st.columns([3, 1])
    with col1:
        service2 = st.selectbox(
            "Add Second Service",
            ["None"] + available_services['SERVICE_NAME'].tolist(),
            key="service2_select"
        )
    
    if service2 != "None":
        service2_details = available_services[available_services['SERVICE_NAME'] == service2]
        if not service2_details.empty:
            service2_id = int(service2_details['SERVICE_ID'].iloc[0])
            service2_cost = float(service2_details['COST'].iloc[0])
            total_cost += service2_cost
            with col2:
                st.write(f"Cost: ${service2_cost:.2f}")

    # Service 3 Selection (only if service 2 is selected)
    service3_id = None
    if service2_id:
        # Filter out both primary and secondary services
        remaining_services = available_services[
            ~available_services['SERVICE_ID'].isin([service1_id, service2_id])
        ]
        
        col1, col2 = st.columns([3, 1])
        with col1:
            service3 = st.selectbox(
                "Add Third Service",
                ["None"] + remaining_services['SERVICE_NAME'].tolist(),
                key="service3_select"
            )

        if service3 != "None":
            service3_details = remaining_services[remaining_services['SERVICE_NAME'] == service3]
            if not service3_details.empty:
                service3_id = int(service3_details['SERVICE_ID'].iloc[0])
                service3_cost = float(service3_details['COST'].iloc[0])
                total_cost += service3_cost
                with col2:
                    st.write(f"Cost: ${service3_cost:.2f}")

    # Display cost breakdown
    st.write("---")
    st.write("**Cost Breakdown:**")
    st.write(f"Primary Service: ${primary_cost:.2f}")
    if service2_id:
        st.write(f"Second Service: ${service2_cost:.2f}")
    if service3_id:
        st.write(f"Third Service: ${service3_cost:.2f}")
    st.write(f"**Total Base Cost: ${total_cost:.2f}**")

    return service2_id, service3_id, total_cost

def save_transaction(transaction_data: Dict[str, Any]) -> bool:
    """Save transaction with pricing details"""
    try:
        # Debug print
        st.write("Saving transaction with data:")
        st.write(f"Service ID: {transaction_data['service_id']}")
        st.write(f"Amount: {transaction_data['final_amount']}")
        
        query = """
        INSERT INTO OPERATIONAL.CARPET.SERVICE_TRANSACTION (
            CUSTOMER_ID,
            ACCOUNT_ID,
            SERVICE_ID,
            SERVICE2_ID,
            SERVICE3_ID,
            AMOUNT,
            DISCOUNT,
            DEPOSIT,
            AMOUNT_RECEIVED,
            PYMT_MTHD_1,
            PYMT_MTHD_1_AMT,
            PYMT_MTHD_2,
            PYMT_MTHD_2_AMT,
            DEPOSIT_PAYMENT_METHOD,
            EMPLOYEE1_ID,
            EMPLOYEE2_ID,
            EMPLOYEE3_ID,
            START_TIME,
            END_TIME,
            TRANSACTION_DATE,
            TRANSACTION_TIME,
            COMMENTS,
            BASE_SERVICE_COST,
            TOTAL_LABOR_COST,
            MATERIAL_COST,
            MARKUP_PERCENTAGE,
            PRICE_ADJUSTMENTS_JSON
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """
        
        # Convert price details to JSON for storage
        price_details = transaction_data.get('price_details', {})
        price_adjustments_json = json.dumps({
            'base_cost': float(price_details.get('base_cost', 0)),
            'labor_cost': float(price_details.get('labor_cost', 0)),
            'material_cost': float(price_details.get('material_cost', 0)),
            'adjustment_amount': float(price_details.get('adjustment_amount', 0)),
            'final_price': float(price_details.get('final_price', 0))
        })
        
        params = [
            transaction_data['customer_id'],
            None,  # account_id
            transaction_data['service_id'],
            transaction_data.get('service2_id'),
            transaction_data.get('service3_id'),
            transaction_data['final_amount'],
            transaction_data.get('discount', 0),
            transaction_data['deposit'],
            transaction_data['amount_received'],
            transaction_data['payment_method_1'],
            transaction_data['payment_amount_1'],
            transaction_data['payment_method_2'],
            transaction_data['payment_amount_2'],
            transaction_data.get('deposit_payment_method'),
            transaction_data['employee1_id'],
            transaction_data['employee2_id'],
            transaction_data['employee3_id'],
            transaction_data['start_time'],
            transaction_data['end_time'],
            transaction_data['transaction_date'],
            transaction_data['transaction_time'],
            transaction_data['notes'],
            float(price_details.get('base_cost', 0)),  # BASE_SERVICE_COST
            float(price_details.get('labor_cost', 0)),  # TOTAL_LABOR_COST
            float(price_details.get('material_cost', 0)),  # MATERIAL_COST
            0.0,  # MARKUP_PERCENTAGE - no longer used
            price_adjustments_json  # Store all price details as JSON
        ]

        # Execute insert
        snowflake_conn.execute_query(query, params)
        
        # Update scheduled service status
        update_query = """
        UPDATE OPERATIONAL.CARPET.SCHEDULED_SERVICES
        SET STATUS = 'Completed',
            COMPLETION_DATE = CURRENT_DATE()
        WHERE SERVICE_ID = ?
        """
        snowflake_conn.execute_query(update_query, [transaction_data['service_id']])
            
        return True
            
    except Exception as e:
        st.error(f"Error saving transaction: {str(e)}")
        return False

def fetch_transaction(transaction_id: int) -> Optional[TransactionModel]:
    """Fetch transaction details by ID"""
    query = """
    SELECT *
    FROM OPERATIONAL.CARPET.TRANSACTIONS
    WHERE TRANSACTION_ID = :1
    """
    result = snowflake_conn.execute_query(query, [transaction_id])
    return TransactionModel.from_dict(result[0]) if result else None

def fetch_service_transactions(service_id: int) -> pd.DataFrame:
    """Fetch all transactions for a service"""
    query = """
    SELECT 
        t.TRANSACTION_ID, t.AMOUNT, t.PAYMENT_TYPE,
        t.TRANSACTION_DATE, t.STATUS, t.NOTES,
        t.IS_DEPOSIT,
        c.FIRST_NAME, c.LAST_NAME
    FROM OPERATIONAL.CARPET.TRANSACTIONS t
    JOIN OPERATIONAL.CARPET.CUSTOMER c 
        ON t.CUSTOMER_ID = c.CUSTOMER_ID
    WHERE t.SERVICE_ID = :1
    ORDER BY t.TRANSACTION_DATE DESC
    """
    result = snowflake_conn.execute_query(query, [service_id])
    if result:
        df = pd.DataFrame(result)
        df['FULL_NAME'] = df['FIRST_NAME'] + " " + df['LAST_NAME']
        return df
    return pd.DataFrame()

def get_customer_balance(customer_id: int) -> float:
    """Get total balance for a customer"""
    query = """
    SELECT 
        COALESCE(SUM(t.AMOUNT), 0) as TOTAL_AMOUNT
    FROM OPERATIONAL.CARPET.TRANSACTIONS t
    WHERE t.CUSTOMER_ID = :1
    AND t.STATUS = 'Completed'
    """
    result = snowflake_conn.execute_query(query, [customer_id])
    return float(result[0]['TOTAL_AMOUNT']) if result else 0.0

def update_transaction_status(transaction_id: int, status: str) -> bool:
    """Update transaction status"""
    try:
        query = """
        UPDATE OPERATIONAL.CARPET.TRANSACTIONS
        SET STATUS = :1
        WHERE TRANSACTION_ID = :2
        """
        snowflake_conn.execute_query(query, [status, transaction_id])
        return True
    except Exception as e:
        st.error(f"Error updating transaction status: {str(e)}")
        return False

def get_transaction_summary(start_date: datetime, end_date: datetime) -> Dict[str, float]:
    """Get transaction summary for a date range"""
    query = """
    SELECT 
        COUNT(*) as TOTAL_TRANSACTIONS,
        COALESCE(SUM(AMOUNT), 0) as TOTAL_AMOUNT,
        COALESCE(SUM(CASE WHEN STATUS = 'Completed' THEN AMOUNT ELSE 0 END), 0) as COMPLETED_AMOUNT
    FROM OPERATIONAL.CARPET.TRANSACTIONS
    WHERE TRANSACTION_DATE BETWEEN :1 AND :2
    """
    result = snowflake_conn.execute_query(query, [start_date, end_date])
    if result:
        return {
            "total_transactions": result[0]['TOTAL_TRANSACTIONS'],
            "total_amount": float(result[0]['TOTAL_AMOUNT']),
            "completed_amount": float(result[0]['COMPLETED_AMOUNT'])
        }
    return {
        "total_transactions": 0,
        "total_amount": 0.0,
        "completed_amount": 0.0
    }