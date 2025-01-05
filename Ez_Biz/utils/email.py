import os
import sys
from datetime import time, datetime
import streamlit as st
import traceback
import re
from typing import Optional, Dict, Any, Tuple
import requests
from dataclasses import dataclass
from models.customer import fetch_customer
from pages.settings.business import fetch_business_info
from database.connection import snowflake_conn

@dataclass
class EmailStatus:
    success: bool
    message: str
    email_id: Optional[str] = None

def validate_email(email: str) -> bool:
    """
    Validate email format using regex pattern.
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return bool(re.match(pattern, email))

def log_email(
    to_email: str,
    subject: str,
    status: bool,
    error_message: Optional[str] = None
) -> None:
    """
    Log email sending attempts to database.
    """
    try:
        query = """
        INSERT INTO OPERATIONAL.CARPET.EMAIL_LOGS (
            RECIPIENT_EMAIL,
            SUBJECT,
            SEND_STATUS,
            ERROR_MESSAGE,
            SEND_TIMESTAMP
        ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP())
        """
        snowflake_conn.execute_query(
            query,
            [to_email, subject, status, error_message]
        )
    except Exception as e:
        if st.session_state.get('debug_mode'):
            st.error(f"Failed to log email: {str(e)}")

def send_email(
    to_email: str,
    subject: str,
    content: str,
    business_info: Dict
) -> EmailStatus:
    """
    Send email using Mailgun with improved error handling and logging.
    """
    try:
        # Validate configuration
        if not st.secrets.get("mailgun", {}).get("api_key"):
            return EmailStatus(False, "Mailgun API key missing", None)
        
        if not st.secrets.get("mailgun", {}).get("domain"):
            return EmailStatus(False, "Mailgun domain missing", None)
            
        # Validate email
        if not to_email or not validate_email(to_email):
            error_msg = "Invalid recipient email address"
            log_email(to_email, subject, False, error_msg)
            return EmailStatus(False, error_msg, None)

        # Set correct sender format
        sender = "EZ Biz <noreply@joinezbiz.com>"

        # Debug logging
        if st.session_state.get('debug_mode'):
            print(f"Sending email to: {to_email}")
            print(f"Subject: {subject}")
            print(f"From: {sender}")

        # Send email using Mailgun
        response = requests.post(
            f"https://api.mailgun.net/v3/joinezbiz.com/messages",  # Using main domain
            auth=("api", st.secrets.mailgun.api_key),
            data={
                "from": sender,
                "to": [to_email],
                "subject": subject,
                "text": content,
                "h:Reply-To": business_info.get('EMAIL_ADDRESS', 'noreply@joinezbiz.com')
            }
        )
        
        if response.status_code == 200:
            email_id = response.json().get('id')
            log_email(to_email, subject, True)
            return EmailStatus(True, "Email sent successfully", email_id)
        else:
            error_msg = f"Failed to send email. Status code: {response.status_code}. Response: {response.text}"
            log_email(to_email, subject, False, error_msg)
            return EmailStatus(False, error_msg, None)
            
    except Exception as e:
        error_msg = f"Error sending email: {str(e)}"
        log_email(to_email, subject, False, error_msg)
        return EmailStatus(False, error_msg, None)  

def send_completion_email(transaction_data: dict, selected_service: dict) -> bool:
    """
    Send completion email for a transaction
    Returns True if email was sent successfully, False otherwise
    """
    try:
        # Debug logging
        print("Transaction Data:", transaction_data)
        print("Selected Service:", selected_service)

        # Get customer info
        customer = fetch_customer(transaction_data['customer_id'])
        if not customer:
            print("Failed to fetch customer")
            return False
        if not customer.email_address:
            print("No email address for customer")
            return False

        print("Customer Info:", customer.to_dict())

        # Get business info
        business_info = fetch_business_info()
        if not business_info:
            print("No business info available")
            return False

        print("Business Info:", business_info)

        # Prepare service details
        service_details = {
            'customer_name': customer.full_name,
            'customer_email': customer.email_address,
            'service_type': selected_service['SERVICE_NAME'],
            'date': selected_service['SERVICE_DATE'].strftime('%Y-%m-%d'),
            'time': selected_service['SERVICE_TIME'].strftime('%I:%M %p'),
            'total_cost': float(transaction_data['final_amount']),
            'deposit_amount': float(transaction_data['deposit']),
            'amount_received': float(transaction_data['amount_received']),
            'notes': transaction_data['notes']
        }

        print("Service Details for Email:", service_details)

        # Send email
        email_status = generate_service_completed_email(service_details, business_info)
        if email_status and email_status.success:
            print(f"Email sent successfully to {customer.email_address}")
            return True
        else:
            print(f"Failed to send email: {getattr(email_status, 'message', 'Unknown error')}")
            return False

    except Exception as e:
        print(f"Error sending completion email: {str(e)}")
        print(traceback.format_exc())
        return False

def generate_service_scheduled_email(service_details: Dict[str, Any], business_info: Dict[str, Any]) -> EmailStatus:
    """
    Generate and send service scheduled confirmation email with improved error handling.
    """
    try:
        if not service_details.get('customer_email'):
            return EmailStatus(False, "No customer email provided", None)

        if not validate_email(service_details['customer_email']):
            return EmailStatus(False, "Invalid customer email address", None)

        # Debug output if enabled
        if st.session_state.get('debug_mode'):
            st.write("Business Info:", business_info)
            st.write("Service Details:", service_details)

        # Generate email content with required fields
        email_content = f"""
From: {business_info.get('BUSINESS_NAME', 'Your Business')}
{business_info.get('STREET_ADDRESS', '')}
{business_info.get('CITY', '')}, {business_info.get('STATE', '')} {business_info.get('ZIP_CODE', '')}

Dear {service_details.get('customer_name', 'Valued Customer')},

Thank you for choosing {business_info.get('BUSINESS_NAME', 'us')}. Your service has been scheduled:

Service: {service_details.get('service_type', 'Service')}
Date: {service_details.get('date', '')}
Time: {service_details.get('time', '')}"""

        if service_details.get('deposit_required'):
            email_content += f"""

Deposit Required: ${service_details.get('deposit_amount', 0):.2f}
Deposit Status: {'Paid' if service_details.get('deposit_paid') else 'Pending'}"""

        if service_details.get('notes'):
            email_content += f"\n\nService Notes: {service_details['notes']}"

        email_content += f"""

If you need to make any changes to your appointment, please contact us:
Phone: {business_info.get('PHONE_NUMBER', '')}
Email: {business_info.get('EMAIL_ADDRESS', '')}

Thank you for your business!

Best regards,
{business_info.get('BUSINESS_NAME', 'Your Business')}"""

        if business_info.get('WEBSITE'):
            email_content += f"\n{business_info['WEBSITE']}"

        # Send the email
        return send_email(
            to_email=service_details['customer_email'],
            subject=f"Service Scheduled - {service_details.get('service_type', 'Service')}",
            content=email_content,
            business_info=business_info
        )
    except Exception as e:
        error_msg = f"Error generating service scheduled email: {str(e)}"
        if st.session_state.get('debug_mode'):
            st.error(error_msg)
            st.error(traceback.format_exc())
        return EmailStatus(False, error_msg, None)

def generate_service_completed_email(service_details: Dict[str, Any], business_info: Dict[str, Any]) -> EmailStatus:
    """
    Generate and send service completion confirmation email.
    """
    try:
        if not service_details.get('customer_email'):
            return EmailStatus(False, "No customer email provided", None)

        if not validate_email(service_details['customer_email']):
            return EmailStatus(False, "Invalid customer email address", None)

        # Generate email content
        email_content = f"""
From: {business_info.get('BUSINESS_NAME', 'Your Business')}
{business_info.get('STREET_ADDRESS', '')}
{business_info.get('CITY', '')}, {business_info.get('STATE', '')} {business_info.get('ZIP_CODE', '')}

Dear {service_details.get('customer_name', 'Valued Customer')},

Thank you for choosing {business_info.get('BUSINESS_NAME', 'us')}. Your service has been completed:

Service: {service_details.get('service_type', 'Service')}
Date: {service_details.get('date', '')}
Time: {service_details.get('time', '')}

Payment Summary:
Total Cost: ${service_details.get('total_cost', 0):.2f}
Amount Paid: ${service_details.get('amount_received', 0):.2f}"""

        if service_details.get('notes'):
            email_content += f"\n\nService Notes: {service_details['notes']}"

        email_content += f"""

If you have any questions about your service, please contact us:
Phone: {business_info.get('PHONE_NUMBER', '')}
Email: {business_info.get('EMAIL_ADDRESS', '')}

Thank you for your business! We appreciate your trust in our services.

Best regards,
{business_info.get('BUSINESS_NAME', 'Your Business')}"""

        if business_info.get('WEBSITE'):
            email_content += f"\n{business_info['WEBSITE']}"

        # Send the email
        return send_email(
            to_email=service_details['customer_email'],
            subject=f"Service Completed - {service_details.get('service_type', 'Service')}",
            content=email_content,
            business_info=business_info
        )
    except Exception as e:
        error_msg = f"Error generating service completed email: {str(e)}"
        if st.session_state.get('debug_mode'):
            st.error(error_msg)
            st.error(traceback.format_exc())
        return EmailStatus(False, error_msg, None)

# Aliases for backward compatibility
send_service_scheduled_email = generate_service_scheduled_email
send_service_completed_email = generate_service_completed_email

# SQL to create email logs table if needed:
"""
CREATE TABLE IF NOT EXISTS OPERATIONAL.CARPET.EMAIL_LOGS (
    LOG_ID NUMBER IDENTITY(1,1),
    RECIPIENT_EMAIL VARCHAR(255) NOT NULL,
    SUBJECT VARCHAR(255),
    SEND_STATUS BOOLEAN,
    ERROR_MESSAGE VARCHAR(1000),
    SEND_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EMAIL_ID VARCHAR(255),
    PRIMARY KEY (LOG_ID)
);
"""