import streamlit as st
from typing import Optional

def accounts_settings_page():
    st.title("Accounts & Billing Settings")
    
    with st.container():
        st.header("Payment Methods")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Accepted Payment Types")
            payment_types = {
                "cash": st.checkbox("Cash", value=True),
                "credit_card": st.checkbox("Credit Card", value=True),
                "check": st.checkbox("Check", value=True),
                "venmo": st.checkbox("Venmo"),
                "paypal": st.checkbox("PayPal")
            }
            
            st.subheader("Default Payment Method")
            default_payment = st.selectbox(
                "Select default payment method",
                ["Cash", "Credit Card", "Check", "Venmo", "PayPal"]
            )
        
        with col2:
            st.subheader("Credit Card Processing")
            processor = st.selectbox(
                "Select payment processor",
                ["Stripe", "Square", "PayPal", "Other"]
            )
            
            if processor != "Other":
                api_key = st.text_input(
                    f"{processor} API Key",
                    type="password"
                )
    
    with st.container():
        st.header("Pricing Settings")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Tax Settings")
            enable_tax = st.checkbox("Enable Sales Tax")
            if enable_tax:
                tax_rate = st.number_input(
                    "Sales Tax Rate (%)",
                    min_value=0.0,
                    max_value=20.0,
                    value=8.25,
                    step=0.25
                )
        
        with col2:
            st.subheader("Deposit Settings")
            require_deposit = st.checkbox("Require Deposit for Services")
            if require_deposit:
                deposit_type = st.radio(
                    "Deposit Type",
                    ["Percentage", "Fixed Amount"]
                )
                if deposit_type == "Percentage":
                    deposit_amount = st.number_input(
                        "Deposit Percentage",
                        min_value=0,
                        max_value=100,
                        value=25,
                        step=5
                    )
                else:
                    deposit_amount = st.number_input(
                        "Deposit Amount ($)",
                        min_value=0,
                        value=50,
                        step=10
                    )
    
    with st.container():
        st.header("Invoice Settings")
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Invoice Numbering")
            prefix = st.text_input(
                "Invoice Number Prefix",
                value="INV-",
                max_chars=5
            )
            next_number = st.number_input(
                "Next Invoice Number",
                min_value=1,
                value=1001,
                step=1
            )
        
        with col2:
            st.subheader("Payment Terms")
            default_terms = st.selectbox(
                "Default Payment Terms",
                ["Due on Receipt", "Net 15", "Net 30", "Net 60"]
            )
            
            late_fee = st.checkbox("Enable Late Fees")
            if late_fee:
                fee_type = st.radio(
                    "Late Fee Type",
                    ["Percentage", "Fixed Amount"]
                )
                if fee_type == "Percentage":
                    fee_amount = st.number_input(
                        "Late Fee Percentage",
                        min_value=0.0,
                        max_value=100.0,
                        value=2.5,
                        step=0.5
                    )
                else:
                    fee_amount = st.number_input(
                        "Late Fee Amount ($)",
                        min_value=0,
                        value=25,
                        step=5
                    )
    
    # Save button at the bottom
    if st.button("Save Settings", type="primary"):
        st.success("Settings saved successfully!")