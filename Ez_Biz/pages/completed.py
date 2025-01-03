# completed.py
import streamlit as st
from datetime import datetime, date, timedelta
import pandas as pd
from decimal import Decimal
from database.connection import SnowflakeConnection
from database.connection import snowflake_conn
from utils.email import send_service_completed_email

def update_payment(transaction_id, payment_data):
    """Update payment information in the database"""
    conn = SnowflakeConnection.get_instance()
    query = """
    UPDATE SERVICE_TRANSACTION
    SET 
        PYMT_MTHD_1 = ?,
        PYMT_MTHD_1_AMT = ?,
        PYMT_MTHD_2 = ?,
        PYMT_MTHD_2_AMT = ?,
        AMOUNT_RECEIVED = ?,
        PYMT_DATE = ?,
        COMMENTS = ?,
        DEPOSIT = ?,
        DEPOSIT_PAYMENT_METHOD = ?,
        LAST_MODIFIED_DATE = CURRENT_TIMESTAMP()
    WHERE ID = ?
    """
    try:
        params = [
            payment_data['payment_method_1'],
            payment_data['payment_method_1_amount'],
            payment_data['payment_method_2'],
            payment_data['payment_method_2_amount'],
            payment_data['amount_received'],
            payment_data['payment_date'],
            payment_data['comments'],
            payment_data['deposit'],
            payment_data['deposit_payment_method'],
            transaction_id
        ]
        conn.execute_query(query, params)
        return True
    except Exception as e:
        st.error(f"Error updating payment: {str(e)}")
        return False

def display_payment_form(row: pd.Series) -> bool:
    """Display and handle payment edit form"""
    payment_methods = ["Cash", "Credit Card", "Check", "Digital Payment"]
    
    with st.form(key=f"payment_form_{row['TRANSACTION_ID']}"):
        st.subheader("Edit Payment")
        
        col1, col2 = st.columns(2)
        with col1:
            payment_method_1 = st.selectbox(
                "Payment Method 1",
                options=payment_methods,
                key=f"pm1_{row['TRANSACTION_ID']}"
            )
            payment_method_1_amount = st.number_input(
                "Amount 1",
                value=float(row.get('PYMT_MTHD_1_AMT', 0.0)),
                min_value=0.0,
                step=0.01,
                key=f"amt1_{row['TRANSACTION_ID']}"
            )
        
        with col2:
            payment_method_2 = st.selectbox(
                "Payment Method 2 (Optional)",
                options=["None"] + payment_methods,
                key=f"pm2_{row['TRANSACTION_ID']}"
            )
            payment_method_2_amount = st.number_input(
                "Amount 2",
                value=float(row.get('PYMT_MTHD_2_AMT', 0.0)),
                min_value=0.0,
                step=0.01,
                key=f"amt2_{row['TRANSACTION_ID']}"
            ) if payment_method_2 != "None" else 0.0

        deposit_col1, deposit_col2 = st.columns(2)
        with deposit_col1:
            deposit = st.number_input(
                "Deposit",
                value=float(row['DEPOSIT']) if pd.notnull(row['DEPOSIT']) else 0.0,
                min_value=0.0,
                step=0.01
            )
        
        with deposit_col2:
            deposit_payment_method = st.selectbox(
                "Deposit Payment Method",
                options=payment_methods,
                index=payment_methods.index(row['DEPOSIT_PAYMENT_METHOD']) if pd.notnull(row.get('DEPOSIT_PAYMENT_METHOD')) and row['DEPOSIT_PAYMENT_METHOD'] in payment_methods else 0
            )

        payment_date = st.date_input(
            "Payment Date",
            value=datetime.now().date()
        )
        
        comments = st.text_area(
            "Comments",
            value=row['COMMENTS'] if pd.notnull(row['COMMENTS']) else ""
        )
        
        # Calculate total amount received
        total_received = payment_method_1_amount
        if payment_method_2 != "None":
            total_received += payment_method_2_amount
        total_received += deposit
        
        # Show remaining balance
        total_due = float(row['AMOUNT']) - float(row.get('DISCOUNT', 0))
        remaining_balance = total_due - total_received
        
        st.write(f"Total Amount Due: ${total_due:.2f}")
        st.write(f"Total Received: ${total_received:.2f}")
        st.write(f"Remaining Balance: ${remaining_balance:.2f}")
        
        submitted = st.form_submit_button("Update Payment")
        
        if submitted:
            payment_data = {
                'payment_method_1': payment_method_1,
                'payment_method_1_amount': payment_method_1_amount,
                'payment_method_2': payment_method_2 if payment_method_2 != "None" else None,
                'payment_method_2_amount': payment_method_2_amount,
                'amount_received': total_received,
                'payment_date': payment_date,
                'comments': comments,
                'deposit': deposit,
                'deposit_payment_method': deposit_payment_method
            }
            
            if update_payment(row['TRANSACTION_ID'], payment_data):
                # Send payment update email
                if row.get('EMAIL_ADDRESS'):
                    service_details = {
                        'customer_name': row['CUSTOMER_NAME'],
                        'customer_email': row['EMAIL_ADDRESS'],
                        'service_type': row['SERVICE1_NAME'],
                        'date': row['TRANSACTION_DATE'].strftime('%Y-%m-%d'),
                        'time': row['START_TIME'].strftime('%I:%M %p') if pd.notnull(row['START_TIME']) else '',
                        'total_cost': float(row['AMOUNT']),
                        'deposit_amount': float(deposit),
                        'amount_received': total_received,
                        'notes': comments
                    }
                    if send_service_completed_email(service_details):
                        st.success("Payment updated and confirmation email sent!")
                    else:
                        st.success("Payment updated but email could not be sent.")
                else:
                    st.success("Payment updated successfully!")
                return True
            
    return False

def format_invoice(data: dict) -> str:
    """Format invoice for customer"""
    invoice = f"""
    {data['business_name']}
    -----------------------
    Invoice Date: {datetime.now().strftime('%Y-%m-%d')}
    
    Bill To:
    {data['customer_name']}
    
    Service Information:
    Date: {data['service_date']}
    Services Provided:
    {chr(10).join(f'- {service}' for service in data['services'])}
    
    Payment Summary:
    ---------------
    Total Amount: ${data['total_cost']:.2f}
    Amount Paid: ${data['amount_paid']:.2f}
    Outstanding Balance: ${data['balance_due']:.2f}
    
    Payment Methods Accepted:
    - Cash
    - Credit Card
    - Check
    - Digital Payment
    
    Please contact us for payment arrangements.
    Thank you for your business!
    """
    return invoice

def send_payment_reminder(data: dict) -> bool:
    """Send payment reminder via customer's preferred method"""
    try:
        if not data.get('email_address') and not data.get('phone_number'):
            st.warning("No contact method available for reminder")
            return False

        service_details = {
            'customer_name': data['customer_name'],
            'customer_email': data['email_address'],
            'service_type': "Past Service",
            'date': data['service_date'],
            'time': '',
            'total_cost': data['balance_due'],
            'amount_received': 0,
            'notes': "This is a reminder for your outstanding balance."
        }
        
        if data['preferred_contact'] in ["Email", "Both"] and data.get('email_address'):
            send_service_completed_email(service_details)
            return True
            
        if data['preferred_contact'] in ["SMS", "Both"] and data.get('phone_number'):
            # TODO: Implement SMS functionality when available
            st.info(f"SMS reminder would be sent to: {data['phone_number']}")
            return True
            
        st.warning("No valid contact method available")
        return False
            
    except Exception as e:
        st.error(f"Error sending reminder: {str(e)}")
        return False

def add_reminder_section(row: pd.Series, total_received: float, total_due: float) -> None:
    """Add payment reminder section to interface"""
    remaining_balance = total_due - total_received
    
    if remaining_balance > 0:
        st.write("### Payment Reminder Options")
        reminder_type = st.selectbox(
            "Reminder Method",
            ["Email", "SMS", "Both"],
            key=f"reminder_type_{row['TRANSACTION_ID']}"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Send Reminder", key=f"send_reminder_{row['TRANSACTION_ID']}"):
                reminder_data = {
                    'customer_name': row['CUSTOMER_NAME'],
                    'service_date': row['TRANSACTION_DATE'].strftime('%Y-%m-%d'),
                    'balance_due': remaining_balance,
                    'email_address': row['EMAIL_ADDRESS'],
                    'phone_number': row['PHONE_NUMBER'],
                    'preferred_contact': reminder_type
                }
                if send_payment_reminder(reminder_data):
                    st.success("Payment reminder sent successfully!")
        
        with col2:
            if st.button("Generate Invoice", key=f"invoice_{row['TRANSACTION_ID']}"):
                invoice_data = {
                    'customer_name': row['CUSTOMER_NAME'],
                    'service_date': row['TRANSACTION_DATE'].strftime('%Y-%m-%d'),
                    'services': [s for s in [row['SERVICE1_NAME'], row['SERVICE2_NAME'], row['SERVICE3_NAME']] if s],
                    'total_cost': total_due,
                    'amount_paid': total_received,
                    'balance_due': remaining_balance
                }
                invoice = format_invoice(invoice_data)
                st.download_button(
                    "Download Invoice",
                    invoice,
                    file_name=f"invoice_{row['TRANSACTION_ID']}.txt",
                    mime="text/plain"
                )

def completed_services_page():
    """Display and manage completed services"""
    st.title('Completed Services')
    
    # Date and Filter Controls Section
    with st.container():
        # Date Selection
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date", 
                value=datetime.now().date() - timedelta(days=30)
            )
        with col2:
            end_date = st.date_input(
                "End Date", 
                value=datetime.now().date()
            )

        # Filter Options
        payment_status = st.selectbox(
            "Filter by Payment Status", 
            ["All", "Paid", "Unpaid"]
        )

    # Query to fetch completed services
    query = """
    SELECT 
        ST.ID AS TRANSACTION_ID,
        COALESCE(C.CUSTOMER_ID, A.ACCOUNT_ID) AS CUSTOMER_OR_ACCOUNT_ID,
        COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
        S1.SERVICE_NAME as SERVICE1_NAME,
        S2.SERVICE_NAME as SERVICE2_NAME,
        S3.SERVICE_NAME as SERVICE3_NAME,
        ST.TRANSACTION_DATE,
        ST.START_TIME,
        ST.END_TIME,
        CAST(ST.AMOUNT AS FLOAT) AS AMOUNT,
        CAST(COALESCE(ST.DISCOUNT, 0) AS FLOAT) AS DISCOUNT,
        CAST(COALESCE(ST.AMOUNT_RECEIVED, 0) AS FLOAT) AS AMOUNT_RECEIVED,
        CAST(COALESCE(ST.DEPOSIT, 0) AS FLOAT) AS DEPOSIT,
        ST.PYMT_MTHD_1,
        ST.PYMT_MTHD_2,
        ST.PYMT_MTHD_1_AMT,
        ST.PYMT_MTHD_2_AMT,
        ST.DEPOSIT_PAYMENT_METHOD,
        COALESCE(C.EMAIL_ADDRESS, A.CONTACT_EMAIL) as EMAIL_ADDRESS,
        COALESCE(C.PHONE_NUMBER, A.CONTACT_PHONE) as PHONE_NUMBER,
        ST.COMMENTS,
        CASE 
            WHEN (COALESCE(ST.AMOUNT_RECEIVED, 0) + COALESCE(ST.DEPOSIT, 0)) >= 
                 (ST.AMOUNT - COALESCE(ST.DISCOUNT, 0)) THEN 'Paid'
            ELSE 'Unpaid'
        END AS PAYMENT_STATUS
    FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION ST
    LEFT JOIN OPERATIONAL.CARPET.CUSTOMER C ON ST.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN OPERATIONAL.CARPET.ACCOUNTS A ON ST.ACCOUNT_ID = A.ACCOUNT_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES S1 ON ST.SERVICE_ID = S1.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES S2 ON ST.SERVICE2_ID = S2.SERVICE_ID
    LEFT JOIN OPERATIONAL.CARPET.SERVICES S3 ON ST.SERVICE3_ID = S3.SERVICE_ID
    WHERE ST.STATUS = 'COMPLETED'
    AND ST.COMPLETION_DATE BETWEEN ? AND ?
    ORDER BY ST.COMPLETION_DATE DESC, ST.END_TIME DESC
    """
    
    results = snowflake_conn.execute_query(query, [
        start_date.strftime('%Y-%m-%d'), 
        end_date.strftime('%Y-%m-%d')
    ])
    
    if results:
        completed_df = pd.DataFrame(results)
        
        # Apply payment status filter
        if payment_status != "All":
            completed_df = completed_df[completed_df['PAYMENT_STATUS'] == payment_status]
        
        if completed_df.empty:
            st.info(f"No {payment_status.lower()} services found in the selected date range.")
            return

        # Display services with expandable details
        for _, row in completed_df.iterrows():
            with st.expander(f"{row['TRANSACTION_DATE'].strftime('%Y-%m-%d')} - {row['CUSTOMER_NAME']}"):
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Services:**")
                    services_list = [
                        name for name in [row['SERVICE1_NAME'], row['SERVICE2_NAME'], row['SERVICE3_NAME']]
                        if pd.notnull(name)
                    ]
                    for service in services_list:
                        st.write(f"- {service}")

                with col2:
                    total_received = float(row['AMOUNT_RECEIVED']) + float(row['DEPOSIT'])
                    total_due = float(row['AMOUNT']) - float(row['DISCOUNT'])
                    remaining_balance = total_due - total_received

                    st.write("**Payment Details:**")
                    st.write(f"Amount: ${float(row['AMOUNT']):.2f}")
                    if row['DISCOUNT'] > 0:
                        st.write(f"Discount: ${float(row['DISCOUNT']):.2f}")
                    st.write(f"Total Received: ${total_received:.2f}")
                    st.markdown(f"Status: **{row['PAYMENT_STATUS']}**")
                    if remaining_balance > 0:
                        st.write(f"**Remaining Balance: ${remaining_balance:.2f}**")

                # Actions Section
                st.write("### Actions")
                action_col1, action_col2, action_col3 = st.columns(3)
                with action_col1:
                    if st.button("Edit Payment", key=f"edit_{row['TRANSACTION_ID']}"):
                        st.session_state[f"show_payment_form_{row['TRANSACTION_ID']}"] = True
                
                with action_col2:
                    if row['PAYMENT_STATUS'] == 'Unpaid':
                        if st.button("Send Reminder", key=f"remind_{row['TRANSACTION_ID']}"):
                            reminder_data = {
                                'customer_name': row['CUSTOMER_NAME'],
                                'service_date': row['TRANSACTION_DATE'].strftime('%Y-%m-%d'),
                                'balance_due': remaining_balance,
                                'email_address': row.get('EMAIL_ADDRESS'),
                                'phone_number': row.get('PHONE_NUMBER'),
                                'preferred_contact': 'Email'  # Default to email
                            }
                            if send_payment_reminder(reminder_data):
                                st.success("Payment reminder sent!")

                with action_col3:
                    if st.button("Generate Invoice", key=f"invoice_{row['TRANSACTION_ID']}"):
                        invoice_data = {
                            'customer_name': row['CUSTOMER_NAME'],
                            'service_date': row['TRANSACTION_DATE'].strftime('%Y-%m-%d'),
                            'services': services_list,
                            'total_cost': total_due,
                            'amount_paid': total_received,
                            'balance_due': remaining_balance
                        }
                        invoice = format_invoice(invoice_data)
                        st.download_button(
                            "Download Invoice",
                            invoice,
                            file_name=f"invoice_{row['TRANSACTION_ID']}.txt",
                            mime="text/plain"
                        )

                # Show payment form if edit button was clicked
                if st.session_state.get(f"show_payment_form_{row['TRANSACTION_ID']}", False):
                    if display_payment_form(row):
                        st.session_state[f"show_payment_form_{row['TRANSACTION_ID']}"] = False
                        st.rerun()

        # Summary Statistics
        st.markdown("### Summary")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Services", len(completed_df))
        
        with col2:
            total_amount = completed_df['AMOUNT'].astype(float).sum()
            st.metric("Total Amount", f"${total_amount:,.2f}")
        
        with col3:
            total_received = (completed_df['AMOUNT_RECEIVED'].astype(float).sum() + 
                            completed_df['DEPOSIT'].astype(float).sum())
            st.metric("Total Received", f"${total_received:,.2f}")
        
        with col4:
            total_outstanding = total_amount - total_received
            st.metric("Outstanding Balance", f"${total_outstanding:,.2f}")
    
    else:
        st.info("No completed services found for the selected date range.")

if __name__ == "__main__":
    completed_services_page()