# database/__init__.py
# from database.connection import snowflake_conn
from .connection import SnowflakeConnection, snowflake_conn

__all__ = ['snowflake_conn']