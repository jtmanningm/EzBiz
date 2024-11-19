from datetime import datetime, date, time
from typing import Union, Optional

def format_currency(amount: float) -> str:
    """Format amount as currency"""
    return f"${amount:,.2f}"

def format_date(date_value: date) -> str:
    """Format date for display"""
    return date_value.strftime("%A, %B %d, %Y")

def format_time(time_value: time) -> str:
    """Format time for display"""
    return time_value.strftime("%I:%M %p")

def format_phone(phone: str) -> str:
    """Format phone number"""
    # Remove any non-numeric characters
    cleaned = ''.join(filter(str.isdigit, phone))
    if len(cleaned) == 10:
        return f"({cleaned[:3]}) {cleaned[3:6]}-{cleaned[6:]}"
    return phone

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