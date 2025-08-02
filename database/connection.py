import streamlit as st
import os
import base64
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
                # Clean the key data - remove any whitespace/newlines
                clean_key_data = PRIVATE_KEY_DATA.strip().replace('\n', '').replace('\r', '')
                
                # Check if key is in base64 format (no PEM headers)
                if not clean_key_data.startswith('-----BEGIN'):
                    # Key is in base64 format, decode it directly
                    try:
                        return base64.b64decode(clean_key_data)
                    except Exception as decode_error:
                        st.error(f"Failed to decode base64 key: {decode_error}")
                        raise
                else:
                    # Key is in PEM format, load and convert
                    private_key = serialization.load_pem_private_key(
                        clean_key_data.encode('utf-8'),
                        password=PRIVATE_KEY_PASSPHRASE.encode('utf-8') if PRIVATE_KEY_PASSPHRASE else None,
                        backend=default_backend()
                    )
                    
                    # Convert to DER format for Snowflake
                    private_key_der = private_key.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption()
                    )
                    
                    return private_key_der
                
            except ValueError as e:
                st.error(f"Error loading private key from secrets: {e}")
                st.error("Key format may be incompatible. Use base64 encoded DER format or proper PEM format.")
                raise
            except Exception as e:
                st.error(f"Unexpected error loading private key: {e}")
                st.error(f"Key starts with: '{PRIVATE_KEY_DATA[:50]}...'")
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
                st.info("Creating new database session...")
                self.session = self._create_session()
                if not self.session:
                    raise Exception("Failed to create database session")
            
            # Execute query with better error handling
            if params:
                result = self.session.sql(query, params).collect()
            else:
                result = self.session.sql(query).collect()
                
            # Convert Snowpark Row objects to dictionaries
            if result:
                return [dict(row.asDict()) for row in result]
            else:
                return []
                
        except Exception as e:
            error_details = f"{error_msg}: {str(e)}"
            st.error(error_details)
            
            # Show more debug info if in debug mode
            if st.session_state.get('debug_mode', False):
                st.error(f"Query: {query}")
                if params:
                    st.error(f"Parameters: {params}")
                st.exception(e)
                
            # Try to recreate session on connection errors
            if "connection" in str(e).lower() or "session" in str(e).lower():
                st.info("Attempting to reconnect to database...")
                self.session = None  # Force session recreation
                
            return None

# Create and export the singleton instance
snowflake_conn = SnowflakeConnection.get_instance()

__all__ = ['snowflake_conn']