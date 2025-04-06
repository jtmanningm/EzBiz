from typing import Union, Optional, Any, Tuple
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

def validate_phone(phone: str) -> Tuple[bool, str]:
    """Validate phone number format"""
    cleaned = ''.join(filter(str.isdigit, phone))
    is_valid = len(cleaned) == 10
    return is_valid, cleaned if is_valid else phone

def validate_email(email: str) -> bool:
    """Basic email format validation"""
    return '@' in email and '.' in email.split('@')[1]

def validate_zip_code(zip_code: any) -> bool:
    """
    Validate if a zip code is in correct format (5 digits).
    
    Args:
        zip_code: Input ZIP code (can be string, int, or None)
        
    Returns:
        bool: True if valid, False if invalid
    """
    if zip_code is None:
        return False
        
    try:
        # Convert to string and remove any whitespace
        zip_str = str(zip_code).strip()
        
        # Remove any non-digit characters
        zip_str = ''.join(filter(str.isdigit, zip_str))
        
        # Check if it's exactly 5 digits and within valid range
        return len(zip_str) == 5 and 0 <= int(zip_str) <= 99999
        
    except (ValueError, TypeError):
        return False

def sanitize_zip_code(zip_code: any) -> Optional[int]:
    """
    Clean and validate ZIP code input. Returns None if invalid.
    
    Args:
        zip_code: Input ZIP code (can be string, int, or None)
        
    Returns:
        Optional[int]: 5-digit integer ZIP code if valid, None if invalid
        
    Examples:
        >>> sanitize_zip_code("12345")
        12345
        >>> sanitize_zip_code(12345)
        12345
        >>> sanitize_zip_code("123456")
        None
        >>> sanitize_zip_code("abcde")
        None
        >>> sanitize_zip_code(None)
        None
    """
    if zip_code is None or (isinstance(zip_code, str) and not zip_code.strip()):
        return None
        
    try:
        # Convert to string and remove any whitespace
        zip_str = str(zip_code).strip()
        
        # Remove any non-digit characters
        zip_str = ''.join(filter(str.isdigit, zip_str))
        
        # Validate length
        if len(zip_str) != 5:
            return None
            
        # Convert to integer
        zip_int = int(zip_str)
        
        # Ensure it's a 5-digit number
        if 0 <= zip_int <= 99999:
            return zip_int
            
        return None
        
    except (ValueError, TypeError):
        return None