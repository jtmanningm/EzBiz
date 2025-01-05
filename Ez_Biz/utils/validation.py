from typing import Union, Optional
import math

def validate_numeric_value(value: Optional[Union[int, float, str]], default: float = 0.0) -> float:
    """Validate and convert numeric value"""
    try:
        if value is None:
            return default
        float_value = float(value)
        if math.isnan(float_value) or math.isinf(float_value):
            return default
        return max(0.0, float_value)
    except (ValueError, TypeError):
        return default

def validate_phone(phone: str) -> tuple[bool, str]:
    """Validate phone number format"""
    cleaned = ''.join(filter(str.isdigit, phone))
    is_valid = len(cleaned) == 10
    return is_valid, cleaned if is_valid else phone

def validate_email(email: str) -> bool:
    """Basic email format validation"""
    return '@' in email and '.' in email.split('@')[1]

def validate_zip_code(zip_code: str) -> tuple[bool, Optional[int]]:
    """Validate ZIP code"""
    cleaned = ''.join(filter(str.isdigit, zip_code))
    is_valid = len(cleaned) == 5
    return is_valid, int(cleaned) if is_valid else None