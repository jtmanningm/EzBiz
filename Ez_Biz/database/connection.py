import streamlit as st
import os
from snowflake.snowpark import Session
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from typing import Optional, List, Any

class SnowflakeConnection:
    """
    Singleton class to manage Snowflake database connection
    """
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get or create singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    
    def __init__(self):
        """Initialize connection"""
        self.session = self._create_session()
    
    def _create_session(self) -> Optional[Session]:
        """Create Snowflake session"""
        try:
            private_key = self._load_private_key()
            connection_parameters = {
                "account": "uvfnphy-okb79182",
                "user": "JTMANNINGM",
                "private_key": private_key,
                "role": "ACCOUNTADMIN",
                "warehouse": "COMPUTE_WH",
                "database": "OPERATIONAL",
                "schema": "CARPET"
            }
            return Session.builder.configs(connection_parameters).create()
        except Exception as e:
            st.error(f"Failed to create Snowpark session: {e}")
            return None

    def _load_private_key(self) -> bytes:
        """Load private key for authentication"""
        PRIVATE_KEY_PATH = os.path.expanduser('~/Documents/Key/rsa_key.p8')
        PRIVATE_KEY_PASSPHRASE = 'Lizard24'
        
        try:
            with open(PRIVATE_KEY_PATH, 'rb') as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=PRIVATE_KEY_PASSPHRASE.encode(),
                    backend=default_backend()
                )
            return private_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        except Exception as e:
            st.error(f"Error loading private key: {e}")
            raise

    def execute_query(self, 
                     query: str, 
                     params: Optional[List[Any]] = None, 
                     error_msg: str = "Error executing query") -> Optional[List[dict]]:
        """
        Execute SQL query with parameters
        
        Args:
            query (str): SQL query to execute
            params (Optional[List[Any]]): Query parameters
            error_msg (str): Custom error message
        
        Returns:
            Optional[List[dict]]: Query results or None if error
        """
        try:
            if not self.session:
                raise Exception("No database session available")
                
            result = self.session.sql(query, params).collect() if params else \
                     self.session.sql(query).collect()
            return result
        except Exception as e:
            st.error(f"{error_msg}: {str(e)}")
            return None

# Create and export the singleton instance
snowflake_conn = SnowflakeConnection.get_instance()

__all__ = ['snowflake_conn']