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
                "account": st.secrets.get("snowflake", {}).get("account", ""),
                "user": st.secrets.get("snowflake", {}).get("user", ""),
                "private_key": private_key,
                "role": st.secrets.get("snowflake", {}).get("role", "ACCOUNTADMIN"),
                "warehouse": st.secrets.get("snowflake", {}).get("warehouse", "COMPUTE_WH"),
                "database": st.secrets.get("snowflake", {}).get("database", "OPERATIONAL"),
                "schema": st.secrets.get("snowflake", {}).get("schema", "CARPET")
            }
            return Session.builder.configs(connection_parameters).create()
        except Exception as e:
            st.error(f"Failed to create Snowpark session: {e}")
            return None

    def _load_private_key(self) -> bytes:
        """Load private key for authentication"""
        PRIVATE_KEY_PATH = os.path.expanduser(st.secrets.get("snowflake", {}).get("private_key_path", ''))
        PRIVATE_KEY_PASSPHRASE = st.secrets.get("snowflake", {}).get("private_key_passphrase", '')
        
        try:
            with open(PRIVATE_KEY_PATH, 'rb') as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=PRIVATE_KEY_PASSPHRASE.encode() if PRIVATE_KEY_PASSPHRASE else None,
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
                print("DEBUG: No database session available")
                self.session = self._create_session()
                if not self.session:
                    raise Exception("Failed to create database session")
                    
            # Print query and params for debugging
            print(f"DEBUG: Executing query: {query}")
            print(f"DEBUG: With parameters: {params}")
                
            result = self.session.sql(query, params).collect() if params else \
                     self.session.sql(query).collect()
            print(f"DEBUG: Query result: {result}")
            return result
        except Exception as e:
            print(f"DEBUG: Database error: {str(e)}")
            import traceback
            print(f"DEBUG: Database error traceback: {traceback.format_exc()}")
            st.error(f"{error_msg}: {str(e)}")
            return None

# Create and export the singleton instance
snowflake_conn = SnowflakeConnection.get_instance()

__all__ = ['snowflake_conn']