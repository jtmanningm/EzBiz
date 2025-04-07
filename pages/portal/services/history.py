# pages/portal/services/history.py
import streamlit as st
from datetime import datetime, timedelta
from utils.auth.middleware import require_customer_auth
from database.connection import snowflake_conn

@require_customer_auth
def service_history_page():
    """Customer service history page"""
    st.title("Service History")
    
    try:
        # Date range filters
        st.subheader("Filter Services")
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "From Date",
                value=datetime.now().date() - timedelta(days=90)
            )
        with col2:
            end_date = st.date_input(
                "To Date",
                value=datetime.now().date()
            )

        # Initialize query parameters
        params = [st.session_state.customer_id, start_date, end_date]
        selected_service = 'All'  # Default value
            
        # Base query for services
        base_query = """
        SELECT 
            t.ID as TRANSACTION_ID,
            t.SERVICE_NAME,
            t.SERVICE_DATE,
            t.AMOUNT,
            t.START_TIME,
            t.END_TIME,
            t.COMMENTS,
            t.COMPLETION_DATE,
            t.DEPOSIT_PAID,
            t.DEPOSIT,
            t.AMOUNT_RECEIVED,
            e1.FIRST_NAME || ' ' || e1.LAST_NAME as TECHNICIAN_NAME
        FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION t
        LEFT JOIN OPERATIONAL.CARPET.EMPLOYEE e1 
            ON t.EMPLOYEE1_ID = e1.EMPLOYEE_ID
        WHERE t.CUSTOMER_ID = ?
        AND t.STATUS = 'COMPLETED'
        AND t.SERVICE_DATE BETWEEN ? AND ?
        """
            
        # Service type filter
        service_query = """
        SELECT DISTINCT SERVICE_NAME 
        FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION 
        WHERE CUSTOMER_ID = ?
        AND STATUS = 'COMPLETED'
        ORDER BY SERVICE_NAME
        """
        
        services = snowflake_conn.execute_query(
            service_query, 
            [st.session_state.customer_id]
        )
        
        if services:
            service_types = ['All'] + [s['SERVICE_NAME'] for s in services]
            selected_service = st.selectbox(
                "Service Type",
                options=service_types
            )
        
            if selected_service != 'All':
                base_query += " AND t.SERVICE_NAME = ?"
                params.append(selected_service)
        
        # Add ordering
        base_query += " ORDER BY t.SERVICE_DATE DESC, t.START_TIME DESC"
        
        # Execute final query
        services = snowflake_conn.execute_query(base_query, params)
        
        if not services:
            st.info("No completed services found for the selected criteria")
            return
            
        # Display services
        st.subheader("Completed Services")
        total_spent = 0
        
        for service in services:
            with st.expander(
                f"{service['SERVICE_DATE'].strftime('%B %d, %Y')} - "
                f"{service['SERVICE_NAME']}"
            ):
                # Service details
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Service Details**")
                    st.write(f"Service: {service['SERVICE_NAME']}")
                    st.write(f"Date: {service['SERVICE_DATE'].strftime('%B %d, %Y')}")
                    if service['START_TIME']:
                        st.write(f"Time: {service['START_TIME'].strftime('%I:%M %p')}")
                    if service['TECHNICIAN_NAME']:
                        st.write(f"Technician: {service['TECHNICIAN_NAME']}")
                        
                with col2:
                    st.write("**Payment Details**")
                    st.write(f"Amount: ${float(service['AMOUNT']):.2f}")
                    if service['DEPOSIT'] and float(service['DEPOSIT']) > 0:
                        st.write(
                            f"Deposit: ${float(service['DEPOSIT']):.2f} "
                            f"({'Paid' if service['DEPOSIT_PAID'] else 'Unpaid'})"
                        )
                    if service['AMOUNT_RECEIVED']:
                        st.write(f"Amount Received: ${float(service['AMOUNT_RECEIVED']):.2f}")
                
                # Service notes
                if service['COMMENTS']:
                    st.write("**Notes:**")
                    st.write(service['COMMENTS'])
                
                # Generate invoice button
                if st.button("Download Invoice", key=f"invoice_{service['TRANSACTION_ID']}"):
                    generate_service_invoice(service['TRANSACTION_ID'])
                
                total_spent += float(service['AMOUNT'])
        
        # Summary statistics
        st.markdown("---")
        st.subheader("Summary")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Services", len(services))
        with col2:
            st.metric("Total Spent", f"${total_spent:.2f}")
            
    except Exception as e:
        st.error("Error loading service history")
        print(f"Service history error: {str(e)}")

def generate_service_invoice(transaction_id: int) -> None:
    """Generate and download service invoice"""
    try:
        # Get service details
        query = """
        SELECT 
            t.SERVICE_NAME,
            t.SERVICE_DATE,
            t.START_TIME,
            t.END_TIME,
            t.AMOUNT,
            t.COMMENTS,
            t.DEPOSIT,
            t.DEPOSIT_PAID,
            t.AMOUNT_RECEIVED,
            c.FIRST_NAME,
            c.LAST_NAME,
            c.STREET_ADDRESS,
            c.CITY,
            c.STATE,
            c.ZIP_CODE,
            b.BUSINESS_NAME,
            b.STREET_ADDRESS as BUSINESS_ADDRESS,
            b.CITY as BUSINESS_CITY,
            b.STATE as BUSINESS_STATE,
            b.ZIP_CODE as BUSINESS_ZIP,
            b.PHONE_NUMBER as BUSINESS_PHONE
        FROM SERVICE_TRANSACTION t
        JOIN CUSTOMER c 
            ON t.CUSTOMER_ID = c.CUSTOMER_ID
        CROSS JOIN BUSINESS_INFO b
        WHERE t.ID = ?
        """
        
        result = snowflake_conn.execute_query(query, [transaction_id])
        if not result:
            st.error("Error generating invoice")
            return
            
        service = result[0]
        
        # Generate invoice content
        invoice = f"""
        {service['BUSINESS_NAME']}
        {service['BUSINESS_ADDRESS']}
        {service['BUSINESS_CITY']}, {service['BUSINESS_STATE']} {service['BUSINESS_ZIP']}
        Phone: {service['BUSINESS_PHONE']}
        
        INVOICE
        
        Bill To:
        {service['FIRST_NAME']} {service['LAST_NAME']}
        {service['STREET_ADDRESS']}
        {service['CITY']}, {service['STATE']} {service['ZIP_CODE']}
        
        Service Details:
        Service: {service['SERVICE_NAME']}
        Date: {service['SERVICE_DATE'].strftime('%B %d, %Y')}
        Time: {service['START_TIME'].strftime('%I:%M %p')}
        
        Amount: ${float(service['AMOUNT']):.2f}
        """
        
        if service['DEPOSIT'] > 0:
            invoice += f"""
        Deposit: ${float(service['DEPOSIT']):.2f}
        Deposit Status: {'Paid' if service['DEPOSIT_PAID'] else 'Unpaid'}
            """
            
        invoice += f"""
        Amount Received: ${float(service['AMOUNT_RECEIVED']):.2f}
        Balance Due: ${float(service['AMOUNT'] - service['AMOUNT_RECEIVED']):.2f}
        """
        
        if service['COMMENTS']:
            invoice += f"""
        Notes:
        {service['COMMENTS']}
        """
            
        # Offer download
        st.download_button(
            "Save Invoice",
            invoice,
            file_name=f"invoice_{service['SERVICE_DATE'].strftime('%Y%m%d')}_{transaction_id}.txt",
            mime="text/plain"
        )
        
    except Exception as e:
        st.error("Error generating invoice")
        print(f"Invoice generation error: {str(e)}")

if __name__ == "__main__":
    service_history_page()