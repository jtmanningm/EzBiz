import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from models.service import ServiceModel
from utils.formatting import format_currency, format_date, format_time, add_back_navigation
from database.connection import SnowflakeConnection
from utils.null_handling import safe_get_float, safe_get_int, safe_get_string, safe_get_bool

def store_service_session_data(row: pd.Series) -> None:
    """
    Safely store service data in session state with proper null handling.
    
    Args:
        row: DataFrame row containing service information
    """
    st.session_state['selected_service'] = {
        'TRANSACTION_ID': safe_get_int(row['TRANSACTION_ID']),
        'SERVICE_ID': safe_get_int(row['SERVICE_ID']),  # This was previously allowing None
        'CUSTOMER_OR_ACCOUNT_ID': (
            safe_get_int(row['ACCOUNT_ID']) if pd.notnull(row['ACCOUNT_ID']) 
            else safe_get_int(row['CUSTOMER_ID'])
        ),
        'CUSTOMER_NAME': safe_get_string(row['CUSTOMER_NAME']),
        'SERVICE_NAME': safe_get_string(row['SERVICE_NAME']),
        'SERVICE_DATE': row['SERVICE_DATE'],
        'SERVICE_TIME': row['START_TIME'],
        'NOTES': safe_get_string(row['COMMENTS']),
        'DEPOSIT': safe_get_float(row['DEPOSIT']),
        'DEPOSIT_PAID': safe_get_bool(row['DEPOSIT_PAID']),
        'COST': safe_get_float(row['BASE_SERVICE_COST']),
    }
    st.session_state['service_start_time'] = datetime.now().time()
    st.session_state['page'] = 'transaction_details'

def update_service_status(snowflake_conn: SnowflakeConnection, transaction_id: int, status: str = 'IN_PROGRESS') -> None:
    """
    Update the status of a service transaction.
    
    Args:
        snowflake_conn: Snowflake connection instance
        transaction_id: ID of the transaction to update
        status: New status to set
    """
    update_query = """
    UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
    SET STATUS = :1,
        START_TIME = CURRENT_TIME(),
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE ID = :2
    """
    try:
        snowflake_conn.execute_query(update_query, [status, transaction_id])
    except Exception as e:
        st.error(f"Failed to update service status: {str(e)}")
        raise

def handle_deposit_confirmation(snowflake_conn: SnowflakeConnection, transaction_id: int) -> None:
    """Handle deposit confirmation logic"""
    update_query = """
    UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
    SET DEPOSIT_PAID = TRUE,
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE ID = ?
    """
    snowflake_conn.execute_query(update_query, [transaction_id])
    st.session_state.deposit_confirmation_state = transaction_id

def handle_service_start(snowflake_conn: SnowflakeConnection, row: pd.Series) -> None:
    """Handle service start logic"""
    transaction_id = safe_get_int(row['TRANSACTION_ID'])
    
    # Store service information in session state
    st.session_state['selected_service'] = {
        'ID': transaction_id,  # Add ID field
        'TRANSACTION_ID': transaction_id,
        'SERVICE_ID': safe_get_int(row['SERVICE_ID']),
        'CUSTOMER_OR_ACCOUNT_ID': (
            safe_get_int(row['ACCOUNT_ID']) 
            if pd.notnull(row['ACCOUNT_ID']) 
            else safe_get_int(row['CUSTOMER_ID'])
        ),
        'CUSTOMER_NAME': safe_get_string(row['CUSTOMER_NAME']),
        'SERVICE_NAME': safe_get_string(row['SERVICE_NAME']),
        'SERVICE_DATE': row['SERVICE_DATE'],
        'SERVICE_TIME': row['START_TIME'],
        'NOTES': safe_get_string(row['COMMENTS']),
        'DEPOSIT': safe_get_float(row['DEPOSIT']),
        'DEPOSIT_PAID': safe_get_bool(row['DEPOSIT_PAID']),
        'BASE_SERVICE_COST': safe_get_float(row['BASE_SERVICE_COST']),
        'SERVICE_ADDRESS': safe_get_string(row['SERVICE_ADDRESS']),
        'SERVICE_CITY': safe_get_string(row['SERVICE_CITY']),
        'SERVICE_STATE': safe_get_string(row['SERVICE_STATE']),
        'SERVICE_ZIP': safe_get_int(row['SERVICE_ZIP'])
    }
    
    # Update service status
    update_query = """
    UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
    SET STATUS = 'IN_PROGRESS',
        START_TIME = CURRENT_TIME(),
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE ID = ?
    """
    snowflake_conn.execute_query(update_query, [transaction_id])
    
    st.session_state['service_start_time'] = datetime.now().time()
    st.session_state['page'] = 'transaction_details'

def handle_service_restart(snowflake_conn: SnowflakeConnection, row: pd.Series) -> None:
    """Handle service restart logic - change status from CANCELLED back to SCHEDULED"""
    transaction_id = safe_get_int(row['TRANSACTION_ID'])
    
    try:
        # Update service status back to SCHEDULED
        update_query = """
        UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
        SET STATUS = 'SCHEDULED',
            COMMENTS = COALESCE(COMMENTS || ' | ', '') || 'Service restarted on ' || CURRENT_DATE(),
            LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
        WHERE ID = ?
        """
        snowflake_conn.execute_query(update_query, [transaction_id])
        
        st.success("Service restarted successfully! Status changed back to SCHEDULED.")
        
    except Exception as e:
        st.error(f"Error restarting service: {str(e)}")
        print(f"Restart error: {str(e)}")

def render_service_card(row: pd.Series, snowflake_conn: SnowflakeConnection) -> None:
    """Render a single service card with all its details and actions."""
    with st.container():
        col1, col2, col3 = st.columns([2, 3, 1])

        # Time column
        with col1:
            st.write(f"🕒 {format_time(row['START_TIME'])}")

        # Service details column
        with col2:
            # Customer and Service Info with Status
            status_icon = {
                'SCHEDULED': '✅',
                'IN_PROGRESS': '🔄', 
                'CANCELLED': '❌',
                'COMPLETED': '🏁'
            }.get(row['STATUS'], '❓')
            
            service_info = f"{status_icon} {row['SERVICE_NAME']} - {row['CUSTOMER_NAME']}"
            
            # Add service cost display
            service_cost = safe_get_float(row['BASE_SERVICE_COST'])
            if service_cost > 0:
                service_info += f"\n💵 Service Cost: {format_currency(service_cost)}"
            
            if row['STATUS'] == 'CANCELLED':
                service_info += " (CANCELLED - Can be restarted)"
            
            # Service Address
            address_parts = [
                safe_get_string(row['SERVICE_ADDRESS']),
                safe_get_string(row['SERVICE_CITY']),
                safe_get_string(row['SERVICE_STATE']),
                str(safe_get_int(row['SERVICE_ZIP']))
            ]
            address = ', '.join(filter(None, address_parts))
            if address:
                service_info += f"\n📍 {address}"

            # Deposit Info
            deposit_amount = safe_get_float(row['DEPOSIT'])
            deposit_paid = safe_get_bool(row['DEPOSIT_PAID'])
            
            if deposit_amount > 0:
                payment_status = "✅ Deposit Paid" if deposit_paid else "⚠️ Deposit Required"
                service_info += f"\n💰 {payment_status}: {format_currency(deposit_amount)}"

            # Recurring Service Info
            if safe_get_bool(row['IS_RECURRING']):
                service_info += f"\n🔄 {row['RECURRENCE_PATTERN']} Service"

            # Notes
            if pd.notnull(row['COMMENTS']):
                service_info += f"\n📝 {row['COMMENTS']}"

            st.write(service_info)

        # Actions column
        with col3:
            transaction_id = safe_get_int(row['TRANSACTION_ID'])
            unique_key_prefix = f"{transaction_id}_{row['SERVICE_DATE'].strftime('%Y%m%d')}"

            # Handle deposit confirmation and start button logic
            if deposit_amount > 0:
                if not deposit_paid:
                    # Only show confirm deposit button when deposit isn't paid
                    if st.session_state.deposit_confirmation_state == transaction_id:
                        st.success("Deposit confirmed!")
                        if st.button("Continue", key=f"continue_{unique_key_prefix}"):
                            st.session_state.deposit_confirmation_state = None
                            st.rerun()
                    else:
                        if st.button("Confirm Deposit", key=f"confirm_deposit_{unique_key_prefix}"):
                            handle_deposit_confirmation(snowflake_conn, transaction_id)
                            st.rerun()
                elif row['STATUS'] == 'SCHEDULED':
                    # Only show start button after deposit is confirmed
                    if st.button("✓ Start", key=f"start_{unique_key_prefix}"):
                        handle_service_start(snowflake_conn, row)
                        st.rerun()
                elif row['STATUS'] == 'CANCELLED':
                    # Show restart button for cancelled services
                    if st.button("🔄 Restart", key=f"restart_{unique_key_prefix}"):
                        handle_service_restart(snowflake_conn, row)
                        st.rerun()
            else:
                # If no deposit required, show start button directly
                if row['STATUS'] == 'SCHEDULED':
                    if st.button("✓ Start", key=f"start_{unique_key_prefix}"):
                        handle_service_start(snowflake_conn, row)
                        st.rerun()
                elif row['STATUS'] == 'CANCELLED':
                    # Show restart button for cancelled services (no deposit)
                    if st.button("🔄 Restart", key=f"restart_no_deposit_{unique_key_prefix}"):
                        handle_service_restart(snowflake_conn, row)
                        st.rerun()

def scheduled_services_page():
    """Display scheduled services page with improved error handling"""
    try:
        snowflake_conn = SnowflakeConnection.get_instance()
    except Exception as e:
        st.error(f"Failed to connect to database: {str(e)}")
        return

    st.title('Scheduled Services')
    add_back_navigation()

    # Initialize confirmation state
    if 'deposit_confirmation_state' not in st.session_state:
        st.session_state.deposit_confirmation_state = None

    # Date range selection with quick options
    st.subheader("Filter by Date Range")
    
    # Quick date range options
    col1, col2, col3, col4 = st.columns(4)
    today = datetime.now().date()
    
    with col1:
        if st.button("📅 Today", use_container_width=True):
            st.session_state.scheduled_start_date = today
            st.session_state.scheduled_end_date = today
            st.rerun()
    
    with col2:
        if st.button("📆 This Week", use_container_width=True):
            # Monday of current week
            start_of_week = today - timedelta(days=today.weekday())
            end_of_week = start_of_week + timedelta(days=6)
            st.session_state.scheduled_start_date = start_of_week
            st.session_state.scheduled_end_date = end_of_week
            st.rerun()
    
    with col3:
        if st.button("🗓️ This Month", use_container_width=True):
            start_of_month = today.replace(day=1)
            # Last day of current month
            if today.month == 12:
                end_of_month = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                end_of_month = today.replace(month=today.month + 1, day=1) - timedelta(days=1)
            st.session_state.scheduled_start_date = start_of_month
            st.session_state.scheduled_end_date = end_of_month
            st.rerun()
    
    with col4:
        if st.button("📋 Next 30 Days", use_container_width=True):
            st.session_state.scheduled_start_date = today
            st.session_state.scheduled_end_date = today + timedelta(days=30)
            st.rerun()
    
    # Custom date range inputs
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Start Date", 
            value=st.session_state.get('scheduled_start_date', today),
            key="scheduled_start_input"
        )
        st.session_state.scheduled_start_date = start_date
    with col2:
        end_date = st.date_input(
            "End Date", 
            value=st.session_state.get('scheduled_end_date', today + timedelta(days=30)),
            key="scheduled_end_input"
        )
        st.session_state.scheduled_end_date = end_date

    try:
        query = """
        WITH DirectAddresses AS (
            SELECT 
                ADDRESS_ID,
                STREET_ADDRESS,
                CITY,
                STATE,
                ZIP_CODE
            FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES
        ),
        RankedCustomerAddresses AS (
            SELECT 
                CUSTOMER_ID,
                STREET_ADDRESS,
                CITY,
                STATE,
                ZIP_CODE,
                ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY 
                    CASE WHEN IS_PRIMARY_SERVICE = TRUE THEN 0 ELSE 1 END,
                    ADDRESS_ID DESC
                ) as rn
            FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES
            WHERE CUSTOMER_ID IS NOT NULL AND ACCOUNT_ID IS NULL
        ),
        RankedAccountAddresses AS (
            SELECT 
                ACCOUNT_ID,
                STREET_ADDRESS,
                CITY,
                STATE,
                ZIP_CODE,
                ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY 
                    CASE WHEN IS_PRIMARY_SERVICE = TRUE THEN 0 ELSE 1 END,
                    ADDRESS_ID DESC
                ) as rn
            FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES
            WHERE ACCOUNT_ID IS NOT NULL
        )
        SELECT DISTINCT
            ST.ID as TRANSACTION_ID,
            ST.SERVICE_ID,
            COALESCE(ST.SERVICE_NAME, S.SERVICE_NAME, 'Unknown Service') as SERVICE_NAME,
            ST.CUSTOMER_ID,
            ST.ACCOUNT_ID,
            ST.DEPOSIT,
            ST.DEPOSIT_PAID,
            ST.SERVICE_DATE,
            ST.START_TIME,
            ST.STATUS,
            CASE 
                WHEN ST.CUSTOMER_ID IS NOT NULL THEN C.FIRST_NAME || ' ' || C.LAST_NAME
                WHEN ST.ACCOUNT_ID IS NOT NULL THEN A.ACCOUNT_NAME
                ELSE 'Unknown Customer'
            END as CUSTOMER_NAME,
            ST.COMMENTS,
            ST.IS_RECURRING,
            ST.RECURRENCE_PATTERN,
            COALESCE(ST.BASE_SERVICE_COST, ST.AMOUNT, S.COST, 0) as BASE_SERVICE_COST,
            COALESCE(DA.STREET_ADDRESS, RCA.STREET_ADDRESS, RAA.STREET_ADDRESS) as SERVICE_ADDRESS,
            COALESCE(DA.CITY, RCA.CITY, RAA.CITY) as SERVICE_CITY,
            COALESCE(DA.STATE, RCA.STATE, RAA.STATE) as SERVICE_STATE,
            COALESCE(DA.ZIP_CODE, RCA.ZIP_CODE, RAA.ZIP_CODE) as SERVICE_ZIP
        FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION ST
        LEFT JOIN OPERATIONAL.CARPET.SERVICES S ON ST.SERVICE_ID = S.SERVICE_ID
        LEFT JOIN OPERATIONAL.CARPET.CUSTOMER C ON ST.CUSTOMER_ID = C.CUSTOMER_ID
        LEFT JOIN OPERATIONAL.CARPET.ACCOUNTS A ON ST.ACCOUNT_ID = A.ACCOUNT_ID
        LEFT JOIN DirectAddresses DA ON ST.ADDRESS_ID = DA.ADDRESS_ID
        LEFT JOIN RankedCustomerAddresses RCA ON ST.CUSTOMER_ID = RCA.CUSTOMER_ID AND RCA.rn = 1
        LEFT JOIN RankedAccountAddresses RAA ON ST.ACCOUNT_ID = RAA.ACCOUNT_ID AND RAA.rn = 1
        WHERE ST.SERVICE_DATE >= ?
        AND ST.SERVICE_DATE <= ?
        AND ST.STATUS IN ('SCHEDULED', 'IN_PROGRESS', 'CANCELLED')  -- Include scheduled, in-progress, and cancelled
        ORDER BY ST.SERVICE_DATE, ST.START_TIME;
        """
        
        services_df = pd.DataFrame(snowflake_conn.execute_query(query, [start_date, end_date]))
        
        if not services_df.empty:
            current_date = None
            
            for _, row in services_df.iterrows():
                if current_date != row['SERVICE_DATE']:
                    current_date = row['SERVICE_DATE']
                    st.markdown(f"### {format_date(current_date)}")
                
                render_service_card(row, snowflake_conn)

            # Summary statistics
            render_summary_statistics(services_df)
        else:
            st.info("No services scheduled for the selected date range.")
            
    except Exception as e:
        st.error(f"Failed to load scheduled services: {str(e)}")
        if st.session_state.get('debug_mode'):
            st.exception(e)

def render_summary_statistics(services_df: pd.DataFrame) -> None:
    """
    Render summary statistics with proper null handling.
    
    Args:
        services_df: DataFrame containing services information
    """
    st.markdown("### Summary")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Services", len(services_df))

    with col2:
        pending_deposits = len(services_df[
            (services_df['DEPOSIT'].notna()) & 
            (services_df['DEPOSIT'] > 0) &
            (~services_df['DEPOSIT_PAID'].fillna(False))
        ])
        st.metric("Pending Deposits", pending_deposits)

    with col3:
        confirmed_deposits = len(services_df[
            (services_df['DEPOSIT'].notna()) &
            (services_df['DEPOSIT'] > 0) &
            (services_df['DEPOSIT_PAID'] == True)
        ])
        st.metric("Confirmed Deposits", confirmed_deposits)