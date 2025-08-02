"""
SMS messaging utilities using Twilio
"""
import streamlit as st
from typing import Optional, Dict, Any
from dataclasses import dataclass
import re
from twilio.rest import Client
from twilio.base.exceptions import TwilioException


@dataclass
class SMSResult:
    """Result of SMS sending operation"""
    success: bool
    message: str
    message_sid: Optional[str] = None
    cost: Optional[str] = None


def get_twilio_client() -> Optional[Client]:
    """Initialize Twilio client from secrets"""
    try:
        account_sid = st.secrets.get("twilio", {}).get("account_sid")
        auth_token = st.secrets.get("twilio", {}).get("auth_token")
        
        if not account_sid or not auth_token:
            return None
            
        return Client(account_sid, auth_token)
    except Exception as e:
        print(f"Error initializing Twilio client: {str(e)}")
        return None


def format_phone_for_sms(phone: str) -> Optional[str]:
    """
    Format phone number for SMS (E.164 format)
    
    Args:
        phone: Phone number in various formats
        
    Returns:
        Formatted phone number (+1XXXXXXXXXX) or None if invalid
    """
    if not phone:
        return None
        
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    # Handle US numbers
    if len(digits) == 10:
        return f"+1{digits}"
    elif len(digits) == 11 and digits.startswith('1'):
        return f"+{digits}"
    else:
        return None


def send_sms(to_phone: str, message: str, from_phone: Optional[str] = None) -> SMSResult:
    """
    Send SMS message using Twilio
    
    Args:
        to_phone: Recipient phone number
        message: Message content
        from_phone: Sender phone number (optional, uses default from secrets)
        
    Returns:
        SMSResult with success status and details
    """
    try:
        client = get_twilio_client()
        if not client:
            return SMSResult(
                success=False,
                message="Twilio client not configured. Please check credentials."
            )
        
        # Format phone number
        formatted_phone = format_phone_for_sms(to_phone)
        if not formatted_phone:
            return SMSResult(
                success=False,
                message=f"Invalid phone number format: {to_phone}"
            )
        
        # Get sender phone from secrets if not provided
        if not from_phone:
            from_phone = st.secrets.get("twilio", {}).get("from_phone")
            if not from_phone:
                return SMSResult(
                    success=False,
                    message="No sender phone number configured"
                )
        
        # Send message
        message_obj = client.messages.create(
            body=message,
            from_=from_phone,
            to=formatted_phone
        )
        
        return SMSResult(
            success=True,
            message="SMS sent successfully",
            message_sid=message_obj.sid,
            cost=getattr(message_obj, 'price', None)
        )
        
    except TwilioException as e:
        return SMSResult(
            success=False,
            message=f"Twilio error: {str(e)}"
        )
    except Exception as e:
        return SMSResult(
            success=False,
            message=f"Unexpected error: {str(e)}"
        )


def generate_service_scheduled_sms(service_details: Dict[str, Any], business_info: Dict[str, Any]) -> str:
    """
    Generate SMS message for service scheduling confirmation
    
    Args:
        service_details: Dictionary containing service information
        business_info: Dictionary containing business information
        
    Returns:
        Formatted SMS message string
    """
    business_name = business_info.get('BUSINESS_NAME', 'Ez Biz')
    business_phone = business_info.get('PHONE_NUMBER', '')
    
    message = f"🏠 {business_name}\n\n"
    message += f"Service Scheduled!\n"
    message += f"📅 {service_details['date']} at {service_details['time']}\n"
    message += f"🔧 {service_details['service_type']}\n"
    
    if service_details.get('deposit_required') and not service_details.get('deposit_paid'):
        message += f"💰 Deposit: ${service_details['deposit_amount']:.2f} (required)\n"
    
    if service_details.get('notes'):
        message += f"📝 Notes: {service_details['notes']}\n"
    
    message += f"\nQuestions? Call {business_phone}"
    
    # SMS character limit is 160 for single message, 1600 for concatenated
    if len(message) > 1600:
        # Truncate if too long
        message = message[:1590] + "..."
    
    return message


def generate_service_reminder_sms(service_details: Dict[str, Any], business_info: Dict[str, Any]) -> str:
    """
    Generate SMS message for service reminder
    
    Args:
        service_details: Dictionary containing service information
        business_info: Dictionary containing business information
        
    Returns:
        Formatted SMS message string
    """
    business_name = business_info.get('BUSINESS_NAME', 'Ez Biz')
    business_phone = business_info.get('PHONE_NUMBER', '')
    
    message = f"🔔 {business_name} Reminder\n\n"
    message += f"Service tomorrow:\n"
    message += f"📅 {service_details['date']} at {service_details['time']}\n"
    message += f"🔧 {service_details['service_type']}\n"
    
    if service_details.get('deposit_required') and not service_details.get('deposit_paid'):
        message += f"💰 Please have ${service_details['deposit_amount']:.2f} deposit ready\n"
    
    message += f"\nQuestions? Call {business_phone}"
    
    return message


def generate_service_completed_sms(service_details: Dict[str, Any], business_info: Dict[str, Any]) -> str:
    """
    Generate SMS message for service completion
    
    Args:
        service_details: Dictionary containing service information
        business_info: Dictionary containing business information
        
    Returns:
        Formatted SMS message string
    """
    business_name = business_info.get('BUSINESS_NAME', 'Ez Biz')
    business_phone = business_info.get('PHONE_NUMBER', '')
    
    message = f"✅ {business_name}\n\n"
    message += f"Service completed!\n"
    message += f"🔧 {service_details['service_type']}\n"
    message += f"💵 Total: ${service_details['total_cost']:.2f}\n"
    
    if service_details.get('balance_due', 0) > 0:
        message += f"💰 Balance due: ${service_details['balance_due']:.2f}\n"
    
    message += f"\nThank you for choosing {business_name}!\n"
    message += f"Questions? Call {business_phone}"
    
    return message


def send_service_notification_sms(
    customer_phone: str,
    service_details: Dict[str, Any],
    business_info: Dict[str, Any],
    notification_type: str = "scheduled"
) -> SMSResult:
    """
    Send service notification SMS based on type
    
    Args:
        customer_phone: Customer's phone number
        service_details: Service information
        business_info: Business information
        notification_type: Type of notification ('scheduled', 'reminder', 'completed')
        
    Returns:
        SMSResult with success status
    """
    try:
        if notification_type == "scheduled":
            message = generate_service_scheduled_sms(service_details, business_info)
        elif notification_type == "reminder":
            message = generate_service_reminder_sms(service_details, business_info)
        elif notification_type == "completed":
            message = generate_service_completed_sms(service_details, business_info)
        else:
            return SMSResult(
                success=False,
                message=f"Unknown notification type: {notification_type}"
            )
        
        return send_sms(customer_phone, message)
        
    except Exception as e:
        return SMSResult(
            success=False,
            message=f"Error generating SMS: {str(e)}"
        )


def validate_sms_setup() -> Dict[str, Any]:
    """
    Validate SMS/Twilio configuration
    
    Returns:
        Dictionary with validation results
    """
    results = {
        'configured': False,
        'errors': [],
        'warnings': []
    }
    
    try:
        # Check secrets
        twilio_config = st.secrets.get("twilio", {})
        
        if not twilio_config.get("account_sid"):
            results['errors'].append("Missing Twilio Account SID")
        
        if not twilio_config.get("auth_token"):
            results['errors'].append("Missing Twilio Auth Token")
            
        if not twilio_config.get("from_phone"):
            results['errors'].append("Missing Twilio phone number")
        
        # Test client connection if credentials are present
        if not results['errors']:
            client = get_twilio_client()
            if client:
                # Test connection by fetching account info
                account = client.api.accounts(twilio_config["account_sid"]).fetch()
                if account.status != 'active':
                    results['warnings'].append(f"Twilio account status: {account.status}")
                else:
                    results['configured'] = True
            else:
                results['errors'].append("Failed to initialize Twilio client")
    
    except Exception as e:
        results['errors'].append(f"Configuration validation error: {str(e)}")
    
    return results


# Export functions for use in other modules
__all__ = [
    'SMSResult',
    'send_sms',
    'send_service_notification_sms',
    'generate_service_scheduled_sms',
    'generate_service_reminder_sms', 
    'generate_service_completed_sms',
    'format_phone_for_sms',
    'validate_sms_setup'
]