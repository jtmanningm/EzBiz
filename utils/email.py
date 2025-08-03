# In utils/email.py
import os
import sys
from datetime import time, datetime
import streamlit as st
import traceback
import re
from typing import Optional, Dict, Any, Tuple
import requests
from dataclasses import dataclass
from database.connection import snowflake_conn
from utils.business.info import fetch_business_info



def debug_print(msg: str) -> None:
    """Helper function for debug logging."""
    if 'debug_mode' in st.session_state and st.session_state['debug_mode']:
        print(f"DEBUG: {msg}")
        st.write(f"DEBUG: {msg}")

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
            EMAIL_TO,
            EMAIL_SUBJECT,
            EMAIL_TYPE,
            STATUS,
            ERROR_MESSAGE
        ) VALUES (?, ?, ?, ?, ?)
        """
        status_str = "SUCCESS" if status else "FAILED"
        email_type = "NOTIFICATION"  # Default email type
        
        snowflake_conn.execute_query(
            query,
            [to_email, subject, email_type, status_str, error_message]
        )
    except Exception as e:
        # Just print the error and continue - don't let this crash the application
        print(f"Failed to log email (non-critical error): {str(e)}")
        if st.session_state.get('debug_mode'):
            print(f"Debug - Email log error details: {type(e).__name__}: {str(e)}")

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

        # Import here to avoid circular dependency
        from models.customer import fetch_customer
        
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
            'time': selected_service['START_TIME'].strftime('%I:%M %p'),
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
    Generate and send service scheduled confirmation email.
    Maps to SERVICE_TRANSACTION table schema.
    """
    try:
        if not service_details.get('customer_email'):
            return EmailStatus(False, "No customer email provided", None)

        if not validate_email(service_details['customer_email']):
            return EmailStatus(False, "Invalid customer email address", None)

        # Format datetime objects
        service_date = service_details.get('date')
        start_time = service_details.get('time')
        formatted_date = service_date if isinstance(service_date, str) else service_date.strftime('%B %d, %Y')
        formatted_time = start_time if isinstance(start_time, str) else start_time.strftime('%I:%M %p')

        # Calculate amounts
        total_amount = float(service_details.get('total_cost', 0))
        deposit_amount = float(service_details.get('deposit_amount', 0))
        deposit_paid = service_details.get('deposit_paid', False)

        # Generate email content
        email_content = f"""
From: {business_info.get('BUSINESS_NAME', 'Your Business')}
{business_info.get('STREET_ADDRESS', '')}
{business_info.get('CITY', '')}, {business_info.get('STATE', '')} {business_info.get('ZIP_CODE', '')}

Dear {service_details.get('customer_name', 'Valued Customer')},

Thank you for choosing {business_info.get('BUSINESS_NAME', 'us')}. Your service has been scheduled:

Service: {service_details.get('service_type', 'Service')}
Date: {formatted_date}
Time: {formatted_time}
Total Cost: ${total_amount:.2f}"""

        # Add deposit information if there is a deposit required
        if deposit_amount > 0:
            email_content += f"""
Deposit Required: ${deposit_amount:.2f}
Deposit Status: {"Paid" if deposit_paid else "Pending"}"""
            
            if deposit_paid:
                email_content += f"\nDeposit Payment Method: {service_details.get('DEPOSIT_PAYMENT_METHOD', 'Not specified')}"

        # Add service comments if any
        if comments := service_details.get('notes'):
            email_content += f"\n\nService Notes: {comments}"

        # Status-specific messages
        status = service_details.get('STATUS', 'PENDING')
        if status == 'PENDING' and deposit_amount > 0 and not deposit_paid:
            email_content += "\n\nIMPORTANT: Please note that your appointment will be confirmed once the deposit has been received."

        email_content += f"""

If you need to make any changes to your appointment, please contact us:
Phone: {business_info.get('PHONE_NUMBER', '')}
Email: {business_info.get('EMAIL_ADDRESS', '')}

Thank you for your business!

Best regards,
{business_info.get('BUSINESS_NAME', 'Your Business')}"""

        if business_info.get('WEBSITE'):
            email_content += f"\n{business_info['WEBSITE']}"

        # Add recurring service information if applicable
        if service_details.get('is_recurring'):
            recurrence = service_details.get('recurrence_pattern', 'regular')
            email_content += f"\n\nThis is a recurring service scheduled on a {recurrence} basis."

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

def generate_verification_email(
    email: str,
    first_name: str,
    verification_url: str,
    business_info: Dict[str, Any]
) -> EmailStatus:
    """
    Generate and send email verification email.
    """
    try:
        if not validate_email(email):
            return EmailStatus(False, "Invalid email address", None)
            
        # Generate email content
        email_content = f"""
From: {business_info.get('BUSINESS_NAME', 'Your Business')}
{business_info.get('STREET_ADDRESS', '')}
{business_info.get('CITY', '')}, {business_info.get('STATE', '')} {business_info.get('ZIP_CODE', '')}

Dear {first_name},

Thank you for creating an account with {business_info.get('BUSINESS_NAME', 'us')}. Please verify your email address by clicking the link below:

{verification_url}

This link will expire in 24 hours. If you didn't create an account, please ignore this email.

If you have any questions, please contact us:
Phone: {business_info.get('PHONE_NUMBER', '')}
Email: {business_info.get('EMAIL_ADDRESS', '')}

Best regards,
{business_info.get('BUSINESS_NAME', 'Your Business')}"""

        if business_info.get('WEBSITE'):
            email_content += f"\n{business_info['WEBSITE']}"

        # Send the email
        return send_email(
            to_email=email,
            subject="Verify Your Email Address",
            content=email_content,
            business_info=business_info
        )

    except Exception as e:
        error_msg = f"Error generating verification email: {str(e)}"
        if st.session_state.get('debug_mode'):
            st.error(error_msg)
            st.error(traceback.format_exc())
        return EmailStatus(False, error_msg, None)

def generate_password_reset_email(
    email: str,
    first_name: str,
    reset_url: str,
    business_info: Dict[str, Any]
) -> EmailStatus:
    """
    Generate and send password reset email.
    """
    try:
        if not validate_email(email):
            return EmailStatus(False, "Invalid email address", None)
            
        # Generate email content
        email_content = f"""
From: {business_info.get('BUSINESS_NAME', 'Your Business')}
{business_info.get('STREET_ADDRESS', '')}
{business_info.get('CITY', '')}, {business_info.get('STATE', '')} {business_info.get('ZIP_CODE', '')}

Dear {first_name},

We received a request to reset your password for your {business_info.get('BUSINESS_NAME', 'our')} account. To reset your password, click the link below:

{reset_url}

This link will expire in 1 hour. If you didn't request a password reset, please ignore this email.

For your security:
- The link can only be used once
- If it expires, you can request a new one from the login page
- If you didn't request this reset, please contact us immediately

If you have any questions or concerns, please contact us:
Phone: {business_info.get('PHONE_NUMBER', '')}
Email: {business_info.get('EMAIL_ADDRESS', '')}

Best regards,
{business_info.get('BUSINESS_NAME', 'Your Business')}"""

        if business_info.get('WEBSITE'):
            email_content += f"\n{business_info['WEBSITE']}"

        # Send the email
        return send_email(
            to_email=email,
            subject="Password Reset Request",
            content=email_content,
            business_info=business_info
        )

    except Exception as e:
        error_msg = f"Error generating password reset email: {str(e)}"
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