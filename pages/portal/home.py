# pages/portal/home.py
import streamlit as st
from utils.auth.middleware import require_customer_auth
from database.connection import snowflake_conn
from datetime import datetime, timedelta

@require_customer_auth
def show_customer_portal():
    """Display customer portal home page"""
    st.title("Welcome to Your Portal")
    
    try:
        # Updated query to match actual table schema
        customer_query = """
        SELECT 
            FIRST_NAME,
            LAST_NAME,
            EMAIL_ADDRESS,
            PHONE_NUMBER,
            BILLING_ADDRESS,
            BILLING_CITY,
            BILLING_STATE,
            BILLING_ZIP,
            PRIMARY_CONTACT_METHOD,
            TEXT_FLAG,
            MEMBER_FLAG
        FROM OPERATIONAL.CARPET.CUSTOMER
        WHERE CUSTOMER_ID = ?
        """
        
        customer = snowflake_conn.execute_query(
            customer_query, 
            [st.session_state.customer_id]
        )
        
        if customer and len(customer) > 0:
            customer = customer[0]
            st.header(f"Welcome back, {customer['FIRST_NAME']}!")
            
            # Split into two columns
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Contact Information")
                if customer['EMAIL_ADDRESS']:
                    st.write("📧", customer['EMAIL_ADDRESS'])
                if customer['PHONE_NUMBER']:
                    st.write("📱", customer['PHONE_NUMBER'])
                if customer['TEXT_FLAG']:
                    st.write("✉️ Text updates enabled")
                if customer['MEMBER_FLAG']:
                    st.write("🌟 Member")
                
                st.subheader("Billing Address")
                if customer['BILLING_ADDRESS']:
                    st.write(customer['BILLING_ADDRESS'])
                if all([customer['BILLING_CITY'], customer['BILLING_STATE']]):
                    # Handle BILLING_ZIP being a number
                    zip_code = str(int(customer['BILLING_ZIP'])) if customer['BILLING_ZIP'] else ''
                    address_line = f"{customer['BILLING_CITY']}, {customer['BILLING_STATE']}"
                    if zip_code:
                        address_line += f" {zip_code}"
                    st.write(address_line)
            
            with col2:
                st.subheader("Upcoming Appointments")
                
                # Updated query to handle nullable fields and proper date filtering
                upcoming_query = """
                SELECT 
                    COALESCE(SERVICE_NAME, 'Service') as SERVICE_NAME,
                    SERVICE_DATE,
                    START_TIME,
                    COALESCE(DEPOSIT, 0) as DEPOSIT,
                    DEPOSIT_PAID,
                    COALESCE(AMOUNT, 0) as AMOUNT,
                    STATUS
                FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION
                WHERE CUSTOMER_ID = ?
                AND SERVICE_DATE >= CURRENT_DATE()
                AND SERVICE_DATE <= DATEADD(days, 30, CURRENT_DATE())
                AND STATUS IN ('SCHEDULED', 'PENDING')
                ORDER BY SERVICE_DATE ASC, START_TIME ASC
                LIMIT 5
                """
                
                upcoming = snowflake_conn.execute_query(
                    upcoming_query,
                    [st.session_state.customer_id]
                )
                
                if upcoming and len(upcoming) > 0:
                    for appt in upcoming:
                        # Get service name or default
                        service_name = appt['SERVICE_NAME'] if appt['SERVICE_NAME'] else 'Service'
                        st.write("💠 " + service_name)
                        
                        # Format date
                        if appt['SERVICE_DATE']:
                            st.write("📅 " + appt['SERVICE_DATE'].strftime('%B %d, %Y'))
                        
                        # Format time if available
                        if appt['START_TIME']:
                            st.write("⏰ " + appt['START_TIME'].strftime('%I:%M %p'))
                        
                        # Show status
                        status_icons = {
                            'SCHEDULED': '✅',
                            'PENDING': '⏳'
                        }
                        status = appt['STATUS'] or 'PENDING'
                        st.write(f"{status_icons.get(status, '❓')} {status}")
                        
                        # Show deposit info if applicable
                        deposit = float(appt['DEPOSIT'] or 0)
                        if deposit > 0:
                            deposit_status = "✅ Paid" if appt['DEPOSIT_PAID'] else "❌ Unpaid"
                            st.write(f"💰 Deposit: ${deposit:.2f} - {deposit_status}")
                        
                        # Show total amount
                        amount = float(appt['AMOUNT'] or 0)
                        if amount > 0:
                            st.write(f"💵 Total: ${amount:.2f}")
                        
                        st.write("---")
                else:
                    st.write("No upcoming appointments")
                    if st.button("Schedule a Service", type="primary"):
                        st.session_state.page = 'book_service'
                        st.rerun()
        
        else:
            st.error("Error loading customer information")
            if st.session_state.get('debug_mode'):
                st.write("Debug - Customer ID:", st.session_state.customer_id)
            
    except Exception as e:
        st.error("Error loading portal data")
        if st.session_state.get('debug_mode'):
            st.exception(e)
        

if __name__ == "__main__":
    show_customer_portal()