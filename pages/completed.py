# completed.py
from typing import Dict, List, Optional
import streamlit as st
from datetime import datetime, timedelta
import pandas as pd
from database.connection import snowflake_conn
from utils.email import send_service_completed_email
from utils.formatting import format_currency, format_date, format_time, add_back_navigation
from pages.settings.business import fetch_business_info
import json

class CompletedServicesPage:
    """Main class for the completed services page"""
    
    def __init__(self):
        """Initialize the completed services page"""
        if 'payment_form_state' not in st.session_state:
            st.session_state.payment_form_state = None
            
    def run(self):
        """Display and manage completed services page"""
        st.title('Completed Services')
        
        # Date range selection and filters
        dates = self._get_date_range()
        payment_status = self._get_payment_status_filter()
        
        try:
            services_df = self._fetch_completed_services(dates)
            if services_df is not None and not services_df.empty:
                self._display_services(services_df, payment_status)
                self._display_summary_statistics(services_df)
            else:
                st.info("No completed services found for the selected date range.")
            
        except Exception as e:
            st.error(f"Error loading completed services: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)

    def _get_date_range(self) -> Dict[str, datetime.date]:
        """Get date range for filtering services with quick options"""
        st.subheader("Filter by Date Range")
        
        # Quick date range options
        col1, col2, col3, col4 = st.columns(4)
        today = datetime.now().date()
        
        with col1:
            if st.button("📅 Today", use_container_width=True, key="completed_today"):
                st.session_state.completed_start_date = today
                st.session_state.completed_end_date = today
                st.rerun()
        
        with col2:
            if st.button("📆 This Week", use_container_width=True, key="completed_week"):
                # Monday of current week
                start_of_week = today - timedelta(days=today.weekday())
                end_of_week = start_of_week + timedelta(days=6)
                st.session_state.completed_start_date = start_of_week
                st.session_state.completed_end_date = end_of_week
                st.rerun()
        
        with col3:
            if st.button("🗓️ This Month", use_container_width=True, key="completed_month"):
                start_of_month = today.replace(day=1)
                # Last day of current month
                if today.month == 12:
                    end_of_month = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
                else:
                    end_of_month = today.replace(month=today.month + 1, day=1) - timedelta(days=1)
                st.session_state.completed_start_date = start_of_month
                st.session_state.completed_end_date = end_of_month
                st.rerun()
        
        with col4:
            if st.button("📋 Last 30 Days", use_container_width=True, key="completed_30days"):
                st.session_state.completed_start_date = today - timedelta(days=30)
                st.session_state.completed_end_date = today
                st.rerun()
        
        # Custom date range inputs
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date", 
                value=st.session_state.get('completed_start_date', today - timedelta(days=30)),
                key="completed_start_input"
            )
            st.session_state.completed_start_date = start_date
        with col2:
            end_date = st.date_input(
                "End Date", 
                value=st.session_state.get('completed_end_date', today),
                key="completed_end_input"
            )
            st.session_state.completed_end_date = end_date
        
        return {'start_date': start_date, 'end_date': end_date}

    def _get_payment_status_filter(self) -> str:
        """Get payment status filter"""
        return st.selectbox(
            "Filter by Payment Status", 
            ["All", "Paid", "Unpaid"],
            help="Filter services by payment status"
        )

    def _fetch_completed_services(self, dates: Dict[str, datetime.date]) -> Optional[pd.DataFrame]:
        """Fetch completed services from database"""
        query = """
        SELECT 
            ST.ID AS TRANSACTION_ID,
            COALESCE(C.CUSTOMER_ID, A.ACCOUNT_ID) AS CUSTOMER_OR_ACCOUNT_ID,
            COALESCE(C.FIRST_NAME || ' ' || C.LAST_NAME, A.ACCOUNT_NAME) AS CUSTOMER_NAME,
            ST.SERVICE_NAME as SERVICE1_NAME,
            S2.SERVICE_NAME as SERVICE2_NAME,
            S3.SERVICE_NAME as SERVICE3_NAME,
            ST.SERVICE_DATE as TRANSACTION_DATE,
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
            END AS PAYMENT_STATUS,
            COALESCE(E1.FIRST_NAME || ' ' || E1.LAST_NAME, '') as EMPLOYEE1_NAME,
            COALESCE(E2.FIRST_NAME || ' ' || E2.LAST_NAME, '') as EMPLOYEE2_NAME,
            COALESCE(E3.FIRST_NAME || ' ' || E3.LAST_NAME, '') as EMPLOYEE3_NAME,
            ST.COMPLETION_DATE,
            ST.TOTAL_LABOR_COST,
            ST.MATERIAL_COST,
            ST.BASE_SERVICE_COST,
            ST.PRICE_ADJUSTMENTS_JSON
        FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION ST
        LEFT JOIN OPERATIONAL.CARPET.CUSTOMER C ON ST.CUSTOMER_ID = C.CUSTOMER_ID
        LEFT JOIN OPERATIONAL.CARPET.ACCOUNTS A ON ST.ACCOUNT_ID = A.ACCOUNT_ID
        LEFT JOIN OPERATIONAL.CARPET.SERVICES S2 ON ST.SERVICE2_ID = S2.SERVICE_ID
        LEFT JOIN OPERATIONAL.CARPET.SERVICES S3 ON ST.SERVICE3_ID = S3.SERVICE_ID
        LEFT JOIN OPERATIONAL.CARPET.EMPLOYEE E1 ON ST.EMPLOYEE1_ID = E1.EMPLOYEE_ID
        LEFT JOIN OPERATIONAL.CARPET.EMPLOYEE E2 ON ST.EMPLOYEE2_ID = E2.EMPLOYEE_ID
        LEFT JOIN OPERATIONAL.CARPET.EMPLOYEE E3 ON ST.EMPLOYEE3_ID = E3.EMPLOYEE_ID
        WHERE ST.STATUS = 'COMPLETED'
        AND ST.COMPLETION_DATE BETWEEN ? AND ?
        ORDER BY ST.COMPLETION_DATE DESC, ST.END_TIME DESC
        """
        
        try:
            results = snowflake_conn.execute_query(query, [
                dates['start_date'].strftime('%Y-%m-%d'),
                dates['end_date'].strftime('%Y-%m-%d')
            ])
            
            if not results:
                return None
                
            df = pd.DataFrame(results)
            
            # Add price breakdown from JSON if available
            if 'PRICE_ADJUSTMENTS_JSON' in df.columns:
                df['PRICE_ADJUSTMENTS'] = df['PRICE_ADJUSTMENTS_JSON'].apply(
                    lambda x: json.loads(x) if pd.notnull(x) else {}
                )
                
            return df
                
        except Exception as e:
            st.error(f"Database error: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)
            return None

    def _update_payment(self, transaction_id: int, payment_data: Dict) -> bool:
        """Update payment information in database"""
        try:
            query = """
            UPDATE OPERATIONAL.CARPET.SERVICE_TRANSACTION
            SET 
                PYMT_MTHD_1 = ?,
                PYMT_MTHD_1_AMT = ?,
                PYMT_MTHD_2 = ?,
                PYMT_MTHD_2_AMT = ?,
                PYMT_MTHD_3 = ?,
                PYMT_MTHD_3_AMT = ?,
                AMOUNT_RECEIVED = ?,
                PYMT_DATE = CURRENT_DATE(),
                COMMENTS = ?,
                DEPOSIT = ?,
                LAST_MODIFIED_DATE = CURRENT_TIMESTAMP(),
                STATUS = 'COMPLETED'
            WHERE ID = ?
            """
            
            # Calculate total received including deposit
            amount_received = float(payment_data['amount_received'])
            
            params = [
                payment_data['payment_method_1'],
                float(payment_data['payment_amount_1']),
                payment_data.get('payment_method_2'),
                float(payment_data.get('payment_amount_2', 0)),
                payment_data.get('payment_method_3'),
                float(payment_data.get('payment_amount_3', 0)),
                amount_received,
                payment_data.get('comments', ''),
                float(payment_data.get('deposit', 0)),
                transaction_id
            ]
            
            snowflake_conn.execute_query(query, params)
            return True
                
        except Exception as e:
            st.error(f"Error updating payment: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)
            return False

    def _send_payment_reminder(self, row: pd.Series, balance_due: float) -> None:
        """Send payment reminder to customer"""
        try:
            if not row.get('EMAIL_ADDRESS'):
                st.warning("No email address available for reminder")
                return

            reminder_data = {
                'customer_name': row['CUSTOMER_NAME'],
                'customer_email': row['EMAIL_ADDRESS'],
                'service_type': "Past Service",
                'date': row['TRANSACTION_DATE'].strftime('%Y-%m-%d'),
                'time': '',
                'total_cost': balance_due,
                'amount_received': 0,
                'notes': "This is a reminder for your outstanding balance."
            }
            
            business_info = fetch_business_info()
            if send_service_completed_email(reminder_data, business_info):
                st.success("Payment reminder sent successfully!")
            else:
                st.error("Failed to send payment reminder")
                
        except Exception as e:
            st.error(f"Error sending reminder: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)

    def _display_services(self, df: pd.DataFrame, payment_status: str) -> None:
        """Display completed services with filtering"""
        if payment_status != "All":
            df = df[df['PAYMENT_STATUS'] == payment_status]
            
        if df.empty:
            st.info(f"No {payment_status.lower()} services found in the selected date range.")
            return

        current_date = None
        
        # Group services by date
        for _, row in df.iterrows():
            # Display date header when date changes
            service_date = row['COMPLETION_DATE']
            if current_date != service_date:
                current_date = service_date
                st.markdown(f"### {format_date(current_date)}")

            # Service card
            with st.expander(f"{row['SERVICE1_NAME']} - {row['CUSTOMER_NAME']}"):
                # Service details
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Service Details**")
                    services_list = [
                        name for name in [row['SERVICE1_NAME'], row['SERVICE2_NAME'], row['SERVICE3_NAME']]
                        if pd.notnull(name)
                    ]
                    for service in services_list:
                        st.write(f"- {service}")
                        
                    if pd.notnull(row['COMMENTS']):
                        st.write("**Notes:**")
                        st.write(row['COMMENTS'])

                with col2:
                    st.write("**Payment Information**")
                    total_received = float(row['AMOUNT_RECEIVED']) 
                    total_due = float(row['AMOUNT']) - float(row['DISCOUNT'])
                    remaining_balance = total_due - total_received

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
                    if row['PAYMENT_STATUS'] == 'Unpaid' and row.get('EMAIL_ADDRESS'):
                        if st.button("Send Reminder", key=f"remind_{row['TRANSACTION_ID']}"):
                            # Fixed: Pass row and remaining_balance to _send_payment_reminder
                            self._send_payment_reminder(row, remaining_balance)

                with action_col3:
                    if st.button("Generate Invoice", key=f"invoice_{row['TRANSACTION_ID']}"):
                        self._generate_invoice(row, services_list, total_due, total_received, remaining_balance)

                # Show payment form if edit button was clicked
                if st.session_state.get(f"show_payment_form_{row['TRANSACTION_ID']}", False):
                    if self._display_payment_form(row):
                        st.session_state[f"show_payment_form_{row['TRANSACTION_ID']}"] = False
                        st.rerun()

    def _display_payment_form(self, row: pd.Series) -> None:
        """Display mobile-friendly payment form"""
        with st.form(key=f"payment_form_{row['TRANSACTION_ID']}"):
            st.subheader("Update Payment")
            
            # Calculate accurate totals from transaction data
            total_due = float(row['AMOUNT'])
            current_deposit = float(row.get('DEPOSIT', 0))
            current_payments = sum([
                float(row.get('PYMT_MTHD_1_AMT', 0)),
                float(row.get('PYMT_MTHD_2_AMT', 0)),
                float(row.get('PYMT_MTHD_3_AMT', 0))
            ])
            total_received = current_deposit + current_payments
            
            # Show existing payment details
            st.markdown("### Current Payments")
            if current_deposit > 0:
                st.write(f"Deposit Paid: ${current_deposit:.2f}")
            for i, (method, amount) in enumerate([
                ('PYMT_MTHD_1', 'PYMT_MTHD_1_AMT'),
                ('PYMT_MTHD_2', 'PYMT_MTHD_2_AMT'),
                ('PYMT_MTHD_3', 'PYMT_MTHD_3_AMT')
            ], 1):
                if float(row.get(amount, 0)) > 0:
                    st.write(f"Payment {i}: ${float(row[amount]):.2f} ({row.get(method, 'N/A')})")

            # New Payment Entry
            st.markdown("### Add Payment")
            payment_methods = ["Cash", "Credit Card", "Check", "Digital Payment"]
            
            # First Payment Method
            payment_method_1 = st.selectbox(
                "Payment Method",
                options=payment_methods,
                key=f"pm1_{row['TRANSACTION_ID']}"
            )

            remaining_balance = total_due - total_received
            payment_amount_1 = st.number_input(
                "Amount",
                value=0.0,
                min_value=0.0,
                max_value=remaining_balance,
                step=0.01,
                key=f"payment_amount_1_{row['TRANSACTION_ID']}"
            )
            
            # Optional Second Payment Method
            use_split = st.checkbox("Add Second Payment Method")
            payment_method_2 = None
            payment_amount_2 = 0.0
            
            if use_split:
                payment_method_2 = st.selectbox(
                    "Second Payment Method",
                    options=payment_methods,
                    key=f"pm2_{row['TRANSACTION_ID']}"
                )
                
                remaining_after_first = remaining_balance - payment_amount_1
                payment_amount_2 = st.number_input(
                    "Second Amount",
                    value=0.0,
                    min_value=0.0,
                    max_value=remaining_after_first,
                    step=0.01,
                    key=f"payment_amount_2_{row['TRANSACTION_ID']}"
                )
                
                # Optional Third Payment Method
                use_third = st.checkbox("Add Third Payment Method")
                payment_method_3 = None
                payment_amount_3 = 0.0
                
                if use_third:
                    payment_method_3 = st.selectbox(
                        "Third Payment Method",
                        options=payment_methods,
                        key=f"pm3_{row['TRANSACTION_ID']}"
                    )
                    
                    remaining_after_second = remaining_after_first - payment_amount_2
                    payment_amount_3 = st.number_input(
                        "Third Amount",
                        value=0.0,
                        min_value=0.0,
                        max_value=remaining_after_second,
                        step=0.01,
                        key=f"payment_amount_3_{row['TRANSACTION_ID']}"
                    )

            # Calculate updated totals
            new_payments = payment_amount_1 + payment_amount_2 + (payment_amount_3 if use_split and use_third else 0.0)
            new_total_received = total_received + new_payments
            final_remaining_balance = total_due - new_total_received

            # Payment Summary
            st.markdown("### Payment Summary")
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Total Amount Due:**")
                st.write("**Previously Received:**")
                st.write("**New Payment:**")
                st.write("**Remaining Balance:**")
            with col2:
                st.write(f"${total_due:.2f}")
                st.write(f"${total_received:.2f}")
                st.write(f"${new_payments:.2f}")
                st.write(f"${final_remaining_balance:.2f}")

            # Comments
            st.markdown("### Notes")
            comments = st.text_area(
                "Payment Notes",
                value=row.get('COMMENTS', ''),
                help="Add any notes about this payment"
            )

            # Submit/Cancel buttons
            col1, col2 = st.columns(2)
            with col1:
                submit = st.form_submit_button("Update Payment", use_container_width=True)
            with col2:
                cancel = st.form_submit_button("Cancel", use_container_width=True)
            
            if submit:
                # Update the payment record
                update_data = {
                    'payment_method_1': payment_method_1,
                    'payment_amount_1': payment_amount_1,
                    'payment_method_2': payment_method_2 if use_split else None,
                    'payment_amount_2': payment_amount_2 if use_split else 0.0,
                    'payment_method_3': payment_method_3 if use_split and use_third else None,
                    'payment_amount_3': payment_amount_3 if use_split and use_third else 0.0,
                    'amount_received': new_total_received,
                    'comments': comments,
                    'deposit': current_deposit
                }
                if self._update_payment(row['TRANSACTION_ID'], update_data):
                    st.success("Payment updated successfully!")
                    st.session_state.payment_form_state = None
                    st.rerun()
                else:
                    st.error("Failed to update payment")
                    
            if cancel:
                st.session_state.payment_form_state = None
                st.rerun()

    def _generate_invoice(self, row: pd.Series, services_list: List[str], 
                         total_due: float, amount_paid: float, balance_due: float) -> None:
        """Generate and offer invoice download"""
        try:
            business_info = fetch_business_info()
            
            invoice = f"""
            {business_info.get('BUSINESS_NAME', 'Your Business')}
            {business_info.get('STREET_ADDRESS', '')}
            {business_info.get('CITY', '')}, {business_info.get('STATE', '')} {business_info.get('ZIP_CODE', '')}
            Phone: {business_info.get('PHONE_NUMBER', '')}
            -----------------------
            Invoice Date: {datetime.now().strftime('%Y-%m-%d')}
            
            Bill To:
            {row['CUSTOMER_NAME']}
            
            Service Information:
            Date: {row['TRANSACTION_DATE'].strftime('%Y-%m-%d')}
            Services Provided:
            {chr(10).join(f'- {service}' for service in services_list)}
            
            Payment Summary:
            ---------------
            Total Amount: {format_currency(total_due)}
            Amount Paid: {format_currency(amount_paid)}
            Outstanding Balance: {format_currency(balance_due)}
            
            Payment Methods Accepted:
            - Cash
            - Credit Card
            - Check
            - Digital Payment
            
            Please contact us for payment arrangements.
            Thank you for your business!
            """
            
            st.download_button(
                "Download Invoice",
                invoice,
                file_name=f"invoice_{row['TRANSACTION_ID']}.txt",
                mime="text/plain"
            )
            
        except Exception as e:
            st.error(f"Error generating invoice: {str(e)}")
            if st.session_state.get('debug_mode'):
                st.exception(e)

    def _display_summary_statistics(self, df: pd.DataFrame) -> None:
        """Display summary statistics in a mobile-friendly format"""
        st.markdown("### Summary")
        
        # Calculate statistics
        total_services = len(df)
        total_amount = df['AMOUNT'].astype(float).sum()
        total_received = (df['AMOUNT_RECEIVED'].astype(float).sum() + 
                         df['DEPOSIT'].astype(float).sum())
        total_outstanding = total_amount - total_received
        total_unpaid = len(df[df['PAYMENT_STATUS'] == 'Unpaid'])
        
        # Display metrics in a grid
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Services", total_services)
            st.metric("Total Amount", format_currency(total_amount))
        
        with col2:
            st.metric("Outstanding Balance", format_currency(total_outstanding))
            st.metric("Unpaid Services", total_unpaid)

# Entry point function
def completed_services_page():

    # Add back navigation at the very start
    add_back_navigation()

    """Entry point for the completed services page"""
    page = CompletedServicesPage()
    page.run()

if __name__ == "__main__":
    completed_services_page()