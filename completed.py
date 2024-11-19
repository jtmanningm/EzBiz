# pages/completed.py
import streamlit as st
from datetime import datetime, date, timedelta
import pandas as pd
from decimal import Decimal
from database.connection import SnowflakeConnection

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

def display_payment_form(row):
    """Display payment edit form"""
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
                value=0.0,
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
                value=0.0,
                min_value=0.0,
                step=0.01,
                key=f"amt2_{row['TRANSACTION_ID']}"
            ) if payment_method_2 != "None" else 0.0
        
        deposit_col1, deposit_col2 = st.columns(2)
        with deposit_col1:
            deposit = st.number_input(
                "Deposit",
                value=float(row['DEPOSIT']) if row['DEPOSIT'] else 0.0,
                min_value=0.0,
                step=0.01
            )
        with deposit_col2:
            deposit_payment_method = st.selectbox(
                "Deposit Payment Method",
                options=payment_methods
            )
        
        payment_date = st.date_input(
            "Payment Date",
            value=datetime.now().date()
        )
        
        comments = st.text_area(
            "Comments",
            value=row['COMMENTS'] if row['COMMENTS'] else ""
        )
        
        # Calculate total amount received
        total_received = payment_method_1_amount
        if payment_method_2 != "None":
            total_received += payment_method_2_amount
        total_received += deposit
        
        # Show remaining balance
        total_due = float(row['AMOUNT']) - float(row['DISCOUNT'] or 0)
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
                st.success("Payment updated successfully!")
                return True
    return False

def completed_services_page():
    st.title('Completed Services')
    
    # Mobile-friendly date selection
    if st.session_state.get('_is_mobile', False):
        start_date = st.date_input(
            "Start Date", 
            value=datetime.now().date() - timedelta(days=30)
        )
        end_date = st.date_input(
            "End Date", 
            value=datetime.now().date()
        )
    else:
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

    # Filter options
    payment_status = st.selectbox(
        "Filter by Payment Status", 
        ["All", "Paid", "Unpaid"]
    )

    # Query to fetch completed services with COMMENTS
    query = """
    SELECT 
        ST.ID AS TRANSACTION_ID,
        COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
        S1.SERVICE_NAME as SERVICE1_NAME,
        S2.SERVICE_NAME as SERVICE2_NAME,
        S3.SERVICE_NAME as SERVICE3_NAME,
        ST.TRANSACTION_DATE,
        ST.START_TIME,
        ST.END_TIME,
        CAST(ST.AMOUNT AS FLOAT) as AMOUNT,
        CAST(COALESCE(ST.DISCOUNT, 0) AS FLOAT) as DISCOUNT,
        CAST(COALESCE(ST.AMOUNT_RECEIVED, 0) AS FLOAT) as AMOUNT_RECEIVED,
        CAST(COALESCE(ST.DEPOSIT, 0) AS FLOAT) as DEPOSIT,
        ST.PYMT_MTHD_1,
        ST.PYMT_MTHD_2,
        ST.PYMT_MTHD_1_AMT,
        ST.PYMT_MTHD_2_AMT,
        ST.DEPOSIT_PAYMENT_METHOD,
        E1.FIRST_NAME || ' ' || E1.LAST_NAME AS EMPLOYEE1_NAME,
        E2.FIRST_NAME || ' ' || E2.LAST_NAME AS EMPLOYEE2_NAME,
        E3.FIRST_NAME || ' ' || E3.LAST_NAME AS EMPLOYEE3_NAME,
        ST.COMMENTS,
        CASE 
            WHEN (COALESCE(ST.AMOUNT_RECEIVED, 0) + COALESCE(ST.DEPOSIT, 0)) >= 
                (ST.AMOUNT - COALESCE(ST.DISCOUNT, 0)) THEN 'Paid'
            ELSE 'Unpaid'
        END AS PAYMENT_STATUS
    FROM SERVICE_TRANSACTION ST
    LEFT JOIN CUSTOMER C ON ST.CUSTOMER_ID = C.CUSTOMER_ID
    LEFT JOIN ACCOUNTS A ON ST.ACCOUNT_ID = A.ACCOUNT_ID
    LEFT JOIN SERVICES S1 ON ST.SERVICE_ID = S1.SERVICE_ID
    LEFT JOIN SERVICES S2 ON ST.SERVICE2_ID = S2.SERVICE_ID
    LEFT JOIN SERVICES S3 ON ST.SERVICE3_ID = S3.SERVICE_ID
    LEFT JOIN EMPLOYEE E1 ON ST.EMPLOYEE1_ID = E1.EMPLOYEE_ID
    LEFT JOIN EMPLOYEE E2 ON ST.EMPLOYEE2_ID = E2.EMPLOYEE_ID
    LEFT JOIN EMPLOYEE E3 ON ST.EMPLOYEE3_ID = E3.EMPLOYEE_ID
    WHERE ST.TRANSACTION_DATE BETWEEN ? AND ?
    ORDER BY ST.TRANSACTION_DATE DESC, ST.TRANSACTION_TIME DESC
    """
    
    conn = SnowflakeConnection.get_instance()
    results = conn.execute_query(query, [
        start_date.strftime('%Y-%m-%d'), 
        end_date.strftime('%Y-%m-%d')
    ])
    
    if results:
        completed_df = pd.DataFrame(results)
        
        # Apply payment status filter
        if payment_status != "All":
            completed_df = completed_df[completed_df['PAYMENT_STATUS'] == payment_status]

        # Display services with expandable details
        for _, row in completed_df.iterrows():
            with st.expander(
                f"{row['TRANSACTION_DATE'].strftime('%Y-%m-%d')} - {row['CUSTOMER_NAME']}"
            ):
                if st.session_state.get('_is_mobile', False):
                    # Mobile view - stack vertically
                    st.write("**Services:**")
                    services_list = [
                        name for name in [row['SERVICE1_NAME'], row['SERVICE2_NAME'], row['SERVICE3_NAME']]
                        if name is not None
                    ]
                    for service in services_list:
                        st.write(f"- {service}")
                    
                    st.write("**Employees:**")
                    employees_list = [
                        name for name in [row['EMPLOYEE1_NAME'], row['EMPLOYEE2_NAME'], row['EMPLOYEE3_NAME']]
                        if name is not None
                    ]
                    for employee in employees_list:
                        st.write(f"- {employee}")

                    st.write("**Payment Details:**")
                    st.write(f"Amount: ${float(row['AMOUNT']):.2f}")
                    if float(row['DEPOSIT']) > 0:
                        st.write(f"Deposit: ${float(row['DEPOSIT']):.2f}")
                    if float(row['DISCOUNT']) > 0:
                        st.write(f"Discount: ${float(row['DISCOUNT']):.2f}")
                    st.write(f"Total Received: ${float(row['AMOUNT_RECEIVED']):.2f}")
                    st.markdown(f"Status: **{row['PAYMENT_STATUS']}**")
                else:
                    # Desktop view - two columns
                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("**Services:**")
                        services_list = [
                            name for name in [row['SERVICE1_NAME'], row['SERVICE2_NAME'], row['SERVICE3_NAME']]
                            if name is not None
                        ]
                        for service in services_list:
                            st.write(f"- {service}")
                        
                        st.write("**Employees:**")
                        employees_list = [
                            name for name in [row['EMPLOYEE1_NAME'], row['EMPLOYEE2_NAME'], row['EMPLOYEE3_NAME']]
                            if name is not None
                        ]
                        for employee in employees_list:
                            st.write(f"- {employee}")

                    with col2:
                        st.write("**Payment Details:**")
                        st.write(f"Amount: ${float(row['AMOUNT']):.2f}")
                        if float(row['DEPOSIT']) > 0:
                            st.write(f"Deposit: ${float(row['DEPOSIT']):.2f}")
                        if float(row['DISCOUNT']) > 0:
                            st.write(f"Discount: ${float(row['DISCOUNT']):.2f}")
                        st.write(f"Total Received: ${float(row['AMOUNT_RECEIVED']):.2f}")
                        st.markdown(f"Status: **{row['PAYMENT_STATUS']}**")
                
                if row['COMMENTS']:
                    st.write("**Service Comments:**")
                    st.write(row['COMMENTS'])
                
                # Add payment edit button
                if st.button("Edit Payment", key=f"edit_{row['TRANSACTION_ID']}"):
                    st.session_state[f"show_payment_form_{row['TRANSACTION_ID']}"] = True
                
                # Show payment form if button was clicked
                if st.session_state.get(f"show_payment_form_{row['TRANSACTION_ID']}", False):
                    if display_payment_form(row):
                        st.session_state[f"show_payment_form_{row['TRANSACTION_ID']}"] = False
                        st.experimental_rerun()

        # Summary statistics
        st.markdown("### Summary")
        if st.session_state.get('_is_mobile', False):
            # Mobile view - stack metrics vertically
            st.metric("Total Services", len(completed_df))
            total_amount = completed_df['AMOUNT'].astype(float).sum()
            st.metric("Total Amount", f"${total_amount:,.2f}")
            total_received = (completed_df['AMOUNT_RECEIVED'].astype(float).sum() + 
                            completed_df['DEPOSIT'].astype(float).sum())
            st.metric("Total Received", f"${total_received:,.2f}")
            total_outstanding = total_amount - total_received
            st.metric("Outstanding Balance", f"${total_outstanding:,.2f}")
        else:
            # Desktop view - metrics in columns
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