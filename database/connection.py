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
        # Check if in cloud environment (detect by checking if private_key is in secrets directly)
        if st.secrets.get("snowflake", {}).get("private_key"):
            # Use the private key directly from secrets
            PRIVATE_KEY_DATA = st.secrets.get("snowflake", {}).get("private_key", '')
            PRIVATE_KEY_PASSPHRASE = st.secrets.get("snowflake", {}).get("private_key_passphrase", '')
            
            try:
                # First try to load as encrypted PKCS#8
                try:
                    private_key = serialization.load_pem_private_key(
                        PRIVATE_KEY_DATA.encode(),
                        password=PRIVATE_KEY_PASSPHRASE.encode() if PRIVATE_KEY_PASSPHRASE else None,
                        backend=default_backend()
                    )
                except Exception:
                    # If that fails, try loading as unencrypted
                    private_key = serialization.load_pem_private_key(
                        PRIVATE_KEY_DATA.encode(),
                        password=None,
                        backend=default_backend()
                    )
                
                return private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption()
                )
            except Exception as e:
                st.error(f"Error loading private key from secrets: {e}")
                st.error("Key format may be incompatible. Try converting to unencrypted PKCS#8 format.")
                raise
        else:
            # Local development - load from file
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
                st.error(f"Error loading private key from file: {e}")
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
                self.session = self._create_session()
                if not self.session:
                    raise Exception("Failed to create database session")
                    
            result = self.session.sql(query, params).collect() if params else \
                     self.session.sql(query).collect()
            return result
        except Exception as e:
            st.error(f"{error_msg}: {str(e)}")
            return None

# Create and export the singleton instance
snowflake_conn = SnowflakeConnection.get_instance()

__all__ = ['snowflake_conn']