import re
from typing import Tuple, Optional, Dict, List, Union
from datetime import datetime, timedelta
from dataclasses import dataclass

@dataclass
class ValidationResult:
    """Standardized validation result object"""
    is_valid: bool
    message: Optional[str] = None
    data: Optional[Union[str, datetime, dict]] = None

def validate_email(email: str) -> ValidationResult:
    """
    Validate email format using RFC 5322 standards
    Returns ValidationResult with:
        - is_valid: bool
        - message: error message if invalid
        - data: normalized email if valid
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not email:
        return ValidationResult(False, "Email address is required")
    
    email = email.strip().lower()
    if not re.match(pattern, email):
        return ValidationResult(False, "Invalid email format")
    
    return ValidationResult(True, data=email)

def validate_phone(phone: str) -> ValidationResult:
    """
    Validate and normalize phone number format
    Accepts: (123) 456-7890, 123-456-7890, 1234567890, +11234567890
    Returns ValidationResult with:
        - is_valid: bool
        - message: error message if invalid
        - data: normalized phone number if valid
    """
    if not phone:
        return ValidationResult(False, "Phone number is required")
    
    # Remove all non-digit characters
    digits = re.sub(r'\D', '', phone)
    
    # Validate length (10 digits or 11 digits with country code)
    if len(digits) == 10:
        formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        return ValidationResult(True, data=formatted)
    elif len(digits) == 11 and digits.startswith('1'):
        formatted = f"+{digits[0]} ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        return ValidationResult(True, data=formatted)
    
    return ValidationResult(False, "Invalid phone number format")

def validate_zip_code(zip_code: str) -> ValidationResult:
    """
    Validate ZIP code format (5-digit or ZIP+4)
    Returns ValidationResult with:
        - is_valid: bool
        - message: error message if invalid
        - data: normalized ZIP code if valid
    """
    if not zip_code:
        return ValidationResult(False, "ZIP code is required")
    
    # Remove any whitespace
    zip_code = zip_code.strip()
    
    # Basic 5-digit ZIP code
    if re.match(r'^\d{5}$', zip_code):
        return ValidationResult(True, data=zip_code)
    
    # ZIP+4 format
    if re.match(r'^\d{5}-\d{4}$', zip_code):
        return ValidationResult(True, data=zip_code)
    
    return ValidationResult(False, "Invalid ZIP code format")

def validate_service_date(
    date_str: str,
    min_days: int = 0,
    max_days: int = 180
) -> ValidationResult:
    """
    Validate service date within allowed range
    Returns ValidationResult with:
        - is_valid: bool
        - message: error message if invalid
        - data: parsed datetime.date if valid
    """
    try:
        if not date_str:
            return ValidationResult(False, "Service date is required")
            
        date = datetime.strptime(date_str, '%Y-%m-%d').date()
        today = datetime.now().date()
        
        # Check minimum days ahead
        min_date = today + timedelta(days=min_days)
        if date < min_date:
            return ValidationResult(
                False,
                f"Service date must be at least {min_days} days in advance"
            )
            
        # Check maximum days ahead
        max_date = today + timedelta(days=max_days)
        if date > max_date:
            return ValidationResult(
                False,
                f"Service date cannot be more than {max_days} days in advance"
            )
            
        return ValidationResult(True, data=date)
        
    except ValueError:
        return ValidationResult(False, "Invalid date format (use YYYY-MM-DD)")

def validate_business_hours(
    time_str: str,
    date: datetime.date
) -> ValidationResult:
    """
    Validate service time within business hours
    - Mon-Fri: 8am - 5pm
    - Sat: 9am - 2pm
    - No Sundays
    Returns ValidationResult with:
        - is_valid: bool
        - message: error message if invalid
        - data: parsed datetime.time if valid
    """
    try:
        if not time_str:
            return ValidationResult(False, "Service time is required")
            
        time = datetime.strptime(time_str, '%H:%M').time()
        
        # Check if Sunday
        if date.weekday() == 6:  # Sunday
            return ValidationResult(False, "Services are not available on Sundays")
            
        # Check Saturday hours
        if date.weekday() == 5:  # Saturday
            start = datetime.strptime('09:00', '%H:%M').time()
            end = datetime.strptime('14:00', '%H:%M').time()
            if time < start or time > end:
                return ValidationResult(
                    False,
                    "Saturday service hours are 9:00 AM to 2:00 PM"
                )
                
        # Check weekday hours
        else:
            start = datetime.strptime('08:00', '%H:%M').time()
            end = datetime.strptime('17:00', '%H:%M').time()
            if time < start or time > end:
                return ValidationResult(
                    False,
                    "Weekday service hours are 8:00 AM to 5:00 PM"
                )
                
        return ValidationResult(True, data=time)
        
    except ValueError:
        return ValidationResult(False, "Invalid time format (use HH:MM)")

def validate_state(state: str) -> ValidationResult:
    """
    Validate US state code
    Returns ValidationResult with:
        - is_valid: bool
        - message: error message if invalid
        - data: normalized state code if valid
    """
    if not state:
        return ValidationResult(False, "State is required")
    
    state = state.strip().upper()
    
    # List of valid US state codes
    valid_states = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    }
    
    if state not in valid_states:
        return ValidationResult(False, "Invalid state code")
    
    return ValidationResult(True, data=state)

def validate_customer_data(data: Dict[str, str]) -> Tuple[bool, List[str]]:
    """
    Validate complete customer form data
    Returns:
        - is_valid: bool
        - list of error messages
    """
    errors = []
    
    # Required fields
    required_fields = {
        'first_name': "First Name",
        'last_name': "Last Name",
        'phone_number': "Phone Number",
        'street_address': "Street Address",
        'city': "City",
        'state': "State",
        'zip_code': "ZIP Code"
    }
    
    # Check required fields
    for field, label in required_fields.items():
        if not data.get(field):
            errors.append(f"{label} is required")
            continue
        
        # Additional validation for specific fields
        if field == 'phone_number':
            result = validate_phone(data[field])
            if not result.is_valid:
                errors.append(result.message)
        
        elif field == 'state':
            result = validate_state(data[field])
            if not result.is_valid:
                errors.append(result.message)
        
        elif field == 'zip_code':
            result = validate_zip_code(data[field])
            if not result.is_valid:
                errors.append(result.message)
    
    # Email validation if provided
    if email := data.get('email_address'):
        result = validate_email(email)
        if not result.is_valid:
            errors.append(result.message)
    
    return not bool(errors), errors

def validate_business_data(data: Dict[str, str]) -> Tuple[bool, List[str]]:
    """
    Validate business form data
    Returns:
        - is_valid: bool
        - list of error messages
    """
    errors = []
    
    # Required fields
    required_fields = {
        'business_name': "Business Name",
        'street_address': "Street Address",
        'city': "City",
        'state': "State",
        'zip_code': "ZIP Code",
        'phone_number': "Phone Number",
        'email_address': "Email Address"
    }
    
    # Check required fields
    for field, label in required_fields.items():
        if not data.get(field):
            errors.append(f"{label} is required")
            continue
        
        # Additional validation for specific fields
        if field == 'phone_number':
            result = validate_phone(data[field])
            if not result.is_valid:
                errors.append(result.message)
        
        elif field == 'email_address':
            result = validate_email(data[field])
            if not result.is_valid:
                errors.append(result.message)
        
        elif field == 'state':
            result = validate_state(data[field])
            if not result.is_valid:
                errors.append(result.message)
        
        elif field == 'zip_code':
            result = validate_zip_code(data[field])
            if not result.is_valid:
                errors.append(result.message)
    
    # Website validation if provided
    if website := data.get('website'):
        if not website.startswith(('http://', 'https://')):
            errors.append("Website must begin with http:// or https://")
    
    return not bool(errors), errors