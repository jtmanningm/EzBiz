from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime
import streamlit as st
import pandas as pd
from database.connection import SnowflakeConnection

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

def save_transaction(data: Dict[str, Any]) -> Optional[int]:
    """Save new transaction"""
    try:
        query = """
        INSERT INTO OPERATIONAL.CARPET.TRANSACTIONS (
            SERVICE_ID, CUSTOMER_ID, AMOUNT,
            PAYMENT_TYPE, TRANSACTION_DATE, STATUS,
            NOTES, IS_DEPOSIT
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8
        )
        """
        params = [
            data['service_id'],
            data['customer_id'],
            data['amount'],
            data.get('payment_type', 'Cash'),
            data.get('transaction_date', datetime.now()),
            data.get('status', 'Pending'),
            data.get('notes'),
            data.get('is_deposit', False)
        ]
        
        snowflake_conn.execute_query(query, params)
        
        # Get the newly created transaction ID
        result = snowflake_conn.execute_query(
            """
            SELECT TRANSACTION_ID 
            FROM OPERATIONAL.CARPET.TRANSACTIONS 
            WHERE SERVICE_ID = :1 
            AND CUSTOMER_ID = :2
            ORDER BY CREATED_AT DESC 
            LIMIT 1
            """,
            [data['service_id'], data['customer_id']]
        )
        
        return result[0]['TRANSACTION_ID'] if result else None
        
    except Exception as e:
        st.error(f"Error saving transaction: {str(e)}")
        return None

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