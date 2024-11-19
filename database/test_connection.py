import streamlit as st
from connection import snowflake_conn

def test_database_connection():
    """Test database connection and basic queries"""
    st.title("Database Connection Test")
    
    # Test connection
    if snowflake_conn.test_connection():
        st.success("✅ Database connection successful!")
        
        # Test basic query
        result = snowflake_conn.execute_query("SELECT CURRENT_TIMESTAMP()")
        if result:
            st.write("Current database time:", result[0]['CURRENT_TIMESTAMP()'])
            
        # Test table access
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'CARPET'
        """
        tables = snowflake_conn.execute_query(tables_query)
        if tables:
            st.write("Available tables:")
            for table in tables:
                st.write(f"- {table['TABLE_NAME']}")
    else:
        st.error("❌ Database connection failed!")

if __name__ == "__main__":
    test_database_connection()