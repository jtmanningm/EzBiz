import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from models.service import ServiceModel
from utils.formatting import format_currency, format_date, format_time
from database.connection import SnowflakeConnection

def scheduled_services_page():

    snowflake_conn = SnowflakeConnection.get_instance()

    """Display scheduled services page"""
    st.title('Scheduled Services')
    
    # Initialize confirmation state
    if 'deposit_confirmation_state' not in st.session_state:
        st.session_state.deposit_confirmation_state = None
    
    # Date range selection
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date", 
            value=datetime.now().date()
        )
    with col2:
        end_date = st.date_input(
            "End Date", 
            value=datetime.now().date() + timedelta(days=30)
        )

    # Fetch upcoming services
    query = """
    SELECT 
        US.SERVICE_ID,
        COALESCE(C.CUSTOMER_ID, A.ACCOUNT_ID) AS CUSTOMER_OR_ACCOUNT_ID,
        COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
        US.SERVICE_NAME,
        US.SERVICE_DATE,
        US.SERVICE_TIME,
        US.NOTES,
        US.DEPOSIT,
        US.DEPOSIT_PAID,
        S.COST
    FROM UPCOMING_SERVICES US
    LEFT JOIN CUSTOMER C ON US.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN ACCOUNTS A ON US.ACCOUNT_ID = A.ACCOUNT_ID
    LEFT JOIN SERVICES S ON US.SERVICE_NAME = S.SERVICE_NAME
    WHERE US.SERVICE_DATE BETWEEN :1 AND :2
    ORDER BY US.SERVICE_DATE, US.SERVICE_TIME
    """
    
    services_df = pd.DataFrame(snowflake_conn.execute_query(
        query,
        [start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')]
    ))

    if not services_df.empty:
        current_date = None
        
        # Group services by date
        for _, row in services_df.iterrows():
            # Display date header when date changes
            if current_date != row['SERVICE_DATE']:
                current_date = row['SERVICE_DATE']
                st.markdown(f"### {format_date(current_date)}")
            
            # Service card
            with st.container():
                col1, col2, col3 = st.columns([2, 3, 1])
                
                # Time column
                with col1:
                    st.write(f"🕒 {format_time(row['SERVICE_TIME'])}")
                
                # Service details column
                with col2:
                    # Basic info
                    service_info = f"📋 {row['SERVICE_NAME']} - {row['CUSTOMER_NAME']}"
                    
                    # Deposit info
                    deposit_amount = float(row['DEPOSIT']) if pd.notnull(row['DEPOSIT']) else 0.0
                    deposit_paid = bool(row['DEPOSIT_PAID']) if pd.notnull(row['DEPOSIT_PAID']) else False
                    
                    if deposit_amount > 0:
                        deposit_status = "✅" if deposit_paid else "❌"
                        service_info += f"\n💰 Deposit Required: {format_currency(deposit_amount)} {deposit_status}"
                    
                    # Notes
                    if pd.notnull(row['NOTES']):
                        service_info += f"\n📝 {row['NOTES']}"
                    
                    st.write(service_info)
                
                # Actions column
                with col3:
                    service_id = int(row['SERVICE_ID'])
                    
                    # Deposit confirmation button
                    if deposit_amount > 0 and not deposit_paid:
                        if st.session_state.deposit_confirmation_state == service_id:
                            st.success("Deposit confirmed!")
                            if st.button("Continue", key=f"continue_{service_id}"):
                                st.session_state.deposit_confirmation_state = None
                                st.rerun()
                        else:
                            if st.button("Confirm Deposit", key=f"confirm_deposit_{service_id}"):
                                # Update deposit status
                                update_query = """
                                UPDATE UPCOMING_SERVICES
                                SET DEPOSIT_PAID = TRUE
                                WHERE SERVICE_ID = :1
                                """
                                snowflake_conn.execute_query(update_query, [service_id])
                                st.session_state.deposit_confirmation_state = service_id
                                st.rerun()
                    else:
                        # Start service button
                        if st.button("✓ Start", key=f"start_{service_id}"):
                            # Store service data for transaction
                            st.session_state['selected_service'] = {
                                'SERVICE_ID': service_id,
                                'CUSTOMER_OR_ACCOUNT_ID': int(row['CUSTOMER_OR_ACCOUNT_ID']),
                                'CUSTOMER_NAME': str(row['CUSTOMER_NAME']),
                                'SERVICE_NAME': str(row['SERVICE_NAME']),
                                'SERVICE_DATE': row['SERVICE_DATE'],
                                'SERVICE_TIME': row['SERVICE_TIME'],
                                'NOTES': str(row['NOTES']) if pd.notnull(row['NOTES']) else None,
                                'DEPOSIT': deposit_amount,
                                'DEPOSIT_PAID': deposit_paid,
                                'COST': float(row['COST']) if pd.notnull(row['COST']) else 0.0
                            }
                            st.session_state['service_start_time'] = datetime.now()
                            st.session_state['page'] = 'transaction_details'
                            st.rerun()

        # Summary statistics
        st.markdown("### Summary")
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Services", len(services_df))
        
        with col2:
            pending_deposits = len(services_df[
                (services_df['DEPOSIT'] > 0) & 
                (~services_df['DEPOSIT_PAID'].fillna(False))
            ])
            st.metric("Pending Deposits", pending_deposits)
        
        with col3:
            confirmed_deposits = len(services_df[
                (services_df['DEPOSIT'] > 0) & 
                (services_df['DEPOSIT_PAID'] == True)
            ])
            st.metric("Confirmed Deposits", confirmed_deposits)
    
    else:
        st.info("No services scheduled for the selected date range.")