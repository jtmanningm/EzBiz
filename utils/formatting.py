from datetime import datetime, date, time, timedelta
from typing import Union, Optional, Dict, Tuple
import streamlit as st

def format_currency(amount: float) -> str:
    """Format amount as currency"""
    return f"${amount:,.2f}"

def format_date(date_value: date) -> str:
    """Format date for display"""
    return date_value.strftime("%A, %B %d, %Y")

def format_time(time_value):
    if time_value is None:
        return "Unknown Time"  # Or any default value you'd like
    return time_value.strftime("%I:%M %p")

def format_phone(phone: str) -> str:
    """Format phone number"""
    # Remove any non-numeric characters
    cleaned = ''.join(filter(str.isdigit, phone))
    if len(cleaned) == 10:
        return f"({cleaned[:3]}) {cleaned[3:6]}-{cleaned[6:]}"
    return phone

def add_back_navigation():
    """Display back navigation button"""
    # Use _ to indicate we're intentionally not using the second column
    col1, _ = st.columns([1, 10])
    with col1:
        if st.button("← Home"):
            # Reset both page and settings states
            if 'show_settings' in st.session_state:
                st.session_state.show_settings = False
            if 'settings_page' in st.session_state:
                st.session_state.settings_page = 'business'  # Reset to default settings page
            st.session_state.page = None  # Reset to main menu
            st.rerun()
    st.markdown("---")

def format_receipt(data: dict) -> str:
    """Format receipt for printing/display"""
    receipt = f"""
    EZ Biz Service Receipt
    ----------------------
    Customer: {data["customer_name"]}
    Service Date: {format_date(data["service_date"])}

    Services:
    {chr(10).join(f"- {service}" for service in data["services"])}

    Payment Details:
    ----------------
    Total Cost: {format_currency(data["total_cost"])}
    Deposit: {format_currency(data["deposit"])}
    """
    
    if data.get("payment1", 0) > 0:
        receipt += f"Payment 1: {format_currency(data['payment1'])} ({data['payment1_method']})\n"
    
    if data.get("payment2", 0) > 0:
        receipt += f"Payment 2: {format_currency(data['payment2'])} ({data['payment2_method']})\n"
    
    receipt += f"""
    Final Total Received: {format_currency(data["final_total_received"])}
    Remaining Balance: {format_currency(data["remaining_balance"])}

    Notes:
    {data.get("notes", "")}
    """
    return receipt

def render_date_range_picker(page_prefix: str) -> Tuple[date, date]:
    """
    Render date range picker with quick options for any page.
    
    Args:
        page_prefix: Unique prefix for session state keys (e.g., 'scheduled', 'completed')
        
    Returns:
        Tuple of (start_date, end_date)
    """
    st.subheader("Filter by Date Range")
    
    # Quick date range options
    col1, col2, col3, col4 = st.columns(4)
    today = datetime.now().date()
    
    with col1:
        if st.button("📅 Today", use_container_width=True, key=f"{page_prefix}_today"):
            st.session_state[f'{page_prefix}_start_date'] = today
            st.session_state[f'{page_prefix}_end_date'] = today
            st.rerun()
    
    with col2:
        if st.button("📆 This Week", use_container_width=True, key=f"{page_prefix}_week"):
            # Monday of current week
            start_of_week = today - timedelta(days=today.weekday())
            end_of_week = start_of_week + timedelta(days=6)
            st.session_state[f'{page_prefix}_start_date'] = start_of_week
            st.session_state[f'{page_prefix}_end_date'] = end_of_week
            st.rerun()
    
    with col3:
        if st.button("🗓️ This Month", use_container_width=True, key=f"{page_prefix}_month"):
            start_of_month = today.replace(day=1)
            # Last day of current month
            if today.month == 12:
                end_of_month = today.replace(year=today.year + 1, month=1, day=1) - timedelta(days=1)
            else:
                end_of_month = today.replace(month=today.month + 1, day=1) - timedelta(days=1)
            st.session_state[f'{page_prefix}_start_date'] = start_of_month
            st.session_state[f'{page_prefix}_end_date'] = end_of_month
            st.rerun()
    
    with col4:
        # Different default for scheduled vs completed
        if page_prefix == 'scheduled':
            button_text = "📋 Next 30 Days"
            default_start = today
            default_end = today + timedelta(days=30)
        else:  # completed
            button_text = "📋 Last 30 Days" 
            default_start = today - timedelta(days=30)
            default_end = today
            
        if st.button(button_text, use_container_width=True, key=f"{page_prefix}_30days"):
            st.session_state[f'{page_prefix}_start_date'] = default_start
            st.session_state[f'{page_prefix}_end_date'] = default_end
            st.rerun()
    
    # Custom date range inputs
    col1, col2 = st.columns(2)
    
    # Set defaults based on page type
    if page_prefix == 'scheduled':
        default_start = today
        default_end = today + timedelta(days=30)
    else:  # completed
        default_start = today - timedelta(days=30)
        default_end = today
    
    with col1:
        start_date = st.date_input(
            "Start Date", 
            value=st.session_state.get(f'{page_prefix}_start_date', default_start),
            key=f"{page_prefix}_start_input"
        )
        st.session_state[f'{page_prefix}_start_date'] = start_date
    
    with col2:
        end_date = st.date_input(
            "End Date", 
            value=st.session_state.get(f'{page_prefix}_end_date', default_end),
            key=f"{page_prefix}_end_input"
        )
        st.session_state[f'{page_prefix}_end_date'] = end_date
    
    return start_date, end_date