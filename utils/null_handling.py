# utils.null_handling
import pandas as pd
import math
from typing import Any, Optional, TypeVar, Callable

T = TypeVar('T')

def safe_get_value(value: Any, default: T, transform: Callable[[Any], T] = lambda x: x) -> T:
    """
    Safely gets a value handling None, NaN and invalid cases.
    
    Args:
        value: The value to check
        default: Default value to return if invalid
        transform: Optional function to transform the value if valid
    
    Returns:
        Transformed value if valid, default otherwise
    """
    try:
        if pd.isnull(value) or (isinstance(value, float) and math.isnan(value)):
            return default
        return transform(value)
    except (ValueError, TypeError, AttributeError):
        return default

def safe_get_float(value: Any, default: float = 0.0) -> float:
    """Safely converts a value to float."""
    return safe_get_value(value, default, float)

def safe_get_int(value: Any, default: int = 0) -> int:
    """Safely converts a value to int."""
    return safe_get_value(value, default, int)

def safe_get_string(value: Any, default: str = "") -> str:
    """Safely converts a value to string."""
    return safe_get_value(value, default, str)

def safe_get_bool(value: Any, default: bool = False) -> bool:
    """Safely converts a value to boolean."""
    return safe_get_value(value, default, bool)