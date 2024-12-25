from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import streamlit as st
import pandas as pd
from database.connection import SnowflakeConnection

@dataclass
class EmployeeModel:
    employee_id: Optional[int] = None
    first_name: str = ""
    last_name: str = ""
    phone_number: str = ""
    email_address: Optional[str] = None
    role: str = "Technician"
    status: str = "Active"

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}".strip()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "first_name": self.first_name,
            "last_name": self.last_name,
            "phone": self.phone_number,
            "email": self.email_address,
            "role": self.role,
            "status": self.status
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EmployeeModel':
        return cls(
            employee_id=data.get('EMPLOYEE_ID'),
            first_name=data.get('FIRST_NAME', ''),
            last_name=data.get('LAST_NAME', ''),
            phone_number=data.get('PHONE_NUMBER', ''),
            email_address=data.get('EMAIL_ADDRESS'),
            role=data.get('ROLE', 'Technician'),
            status=data.get('STATUS', 'Active')
        )

# Get database connection
snowflake_conn = SnowflakeConnection.get_instance()

def fetch_employee(employee_id: int) -> Optional[EmployeeModel]:
    """Fetch employee details by ID"""
    query = """
    SELECT *
    FROM OPERATIONAL.CARPET.EMPLOYEE
    WHERE EMPLOYEE_ID = :1
    """
    result = snowflake_conn.execute_query(query, [employee_id])
    return EmployeeModel.from_dict(result[0]) if result else None

def fetch_all_employees() -> pd.DataFrame:
    """Fetch all employees"""
    query = """
    SELECT 
        EMPLOYEE_ID, FIRST_NAME, LAST_NAME,
        PHONE_NUMBER, EMAIL_ADDRESS,
        ROLE, STATUS
    FROM OPERATIONAL.CARPET.EMPLOYEE
    WHERE STATUS = 'Active'
    ORDER BY FIRST_NAME, LAST_NAME
    """
    result = snowflake_conn.execute_query(query)
    if result:
        df = pd.DataFrame(result)
        df['FULL_NAME'] = df['FIRST_NAME'] + " " + df['LAST_NAME']
        return df
    return pd.DataFrame()

def fetch_employees():
    """Fetch list of employees from database"""
    conn = SnowflakeConnection.get_instance()
    query = """
    SELECT 
        EMPLOYEE_ID,
        FIRST_NAME || ' ' || LAST_NAME as FULL_NAME
    FROM OPERATIONAL.CARPET.EMPLOYEE
    ORDER BY FULL_NAME
    """
    results = conn.execute_query(query)
    return pd.DataFrame(results, columns=['EMPLOYEE_ID', 'FULL_NAME'])

def get_employee_by_name(full_name):
    """Get employee ID from full name"""
    employees_df = fetch_employees()
    employee = employees_df[employees_df['FULL_NAME'] == full_name]
    return employee.iloc[0]['EMPLOYEE_ID'] if not employee.empty else None

def get_employee_rate(employee_name):
    """Fetch employee's hourly wage from database"""
    conn = SnowflakeConnection.get_instance()
    query = """
    SELECT HOURLY_WAGE, SALARY
    FROM OPERATIONAL.CARPET.EMPLOYEE
    WHERE FIRST_NAME || ' ' || LAST_NAME = ?
    """
    results = conn.execute_query(query, [employee_name])
    if results:
        hourly_wage = results[0][0]  # Use hourly_wage if available
        salary = results[0][1]       # Use salary as backup
        return hourly_wage if hourly_wage is not None else (salary / 2080) if salary is not None else 0.0
    return 0.0

def save_employee(data: Dict[str, Any]) -> Optional[int]:
    """Save new employee or update existing"""
    try:
        query = """
        INSERT INTO OPERATIONAL.CARPET.EMPLOYEE (
            FIRST_NAME, LAST_NAME, PHONE_NUMBER,
            EMAIL_ADDRESS, ROLE, STATUS
        ) VALUES (
            :1, :2, :3, :4, :5, :6
        )
        """
        params = [
            data['first_name'],
            data['last_name'],
            data['phone'],
            data.get('email'),
            data.get('role', 'Technician'),
            data.get('status', 'Active')
        ]
        
        snowflake_conn.execute_query(query, params)
        
        # Get the newly created employee ID
        result = snowflake_conn.execute_query(
            """
            SELECT EMPLOYEE_ID 
            FROM OPERATIONAL.CARPET.EMPLOYEE 
            WHERE FIRST_NAME = :1 
            AND LAST_NAME = :2 
            ORDER BY CREATED_AT DESC 
            LIMIT 1
            """,
            [data['first_name'], data['last_name']]
        )
        
        return result[0]['EMPLOYEE_ID'] if result else None
        
    except Exception as e:
        st.error(f"Error saving employee: {str(e)}")
        return None

def update_employee_status(employee_id: int, status: str) -> bool:
    """Update employee status (Active/Inactive)"""
    try:
        query = """
        UPDATE OPERATIONAL.CARPET.EMPLOYEE
        SET STATUS = :1
        WHERE EMPLOYEE_ID = :2
        """
        snowflake_conn.execute_query(query, [status, employee_id])
        return True
    except Exception as e:
        st.error(f"Error updating employee status: {str(e)}")
        return False

def assign_employee_to_service(service_id: int, employee_id: int) -> bool:
    """Assign an employee to a service"""
    try:
        query = """
        INSERT INTO OPERATIONAL.CARPET.SERVICE_ASSIGNMENTS (
            SERVICE_ID, EMPLOYEE_ID
        ) VALUES (
            :1, :2
        )
        """
        snowflake_conn.execute_query(query, [service_id, employee_id])
        return True
    except Exception as e:
        st.error(f"Error assigning employee to service: {str(e)}")
        return False

def get_service_assignments(service_id: int) -> List[Dict[str, Any]]:
    """Get all employees assigned to a service"""
    query = """
    SELECT 
        e.EMPLOYEE_ID, e.FIRST_NAME, e.LAST_NAME,
        e.ROLE, e.STATUS
    FROM OPERATIONAL.CARPET.SERVICE_ASSIGNMENTS sa
    JOIN OPERATIONAL.CARPET.EMPLOYEE e 
        ON sa.EMPLOYEE_ID = e.EMPLOYEE_ID
    WHERE sa.SERVICE_ID = :1
    """
    result = snowflake_conn.execute_query(query, [service_id])
    return result if result else []