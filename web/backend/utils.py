#!/usr/bin/env python3
"""
Utility functions for the web application.
"""

from decimal import Decimal
from typing import Optional, Any
from datetime import datetime


def safe_float(value: Optional[Any], default: float = 0.0) -> float:
    """
    Safely convert value to float.
    
    Args:
        value: Value to convert (can be Decimal, int, float, or None).
        default: Default value if conversion fails or value is None.
    
    Returns:
        Float value.
    """
    if value is None:
        return default
    
    if isinstance(value, Decimal):
        return float(value)
    
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value: Optional[Any], default: int = 0) -> int:
    """
    Safely convert value to int.
    
    Args:
        value: Value to convert.
        default: Default value if conversion fails or value is None.
    
    Returns:
        Integer value.
    """
    if value is None:
        return default
    
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def safe_str(value: Optional[Any], default: str = "") -> str:
    """
    Safely convert value to string.
    
    Args:
        value: Value to convert.
        default: Default value if value is None.
    
    Returns:
        String value.
    """
    if value is None:
        return default
    return str(value)


def safe_datetime_iso(dt: Optional[datetime]) -> Optional[str]:
    """
    Safely convert datetime to ISO format string.
    
    Args:
        dt: Datetime object.
    
    Returns:
        ISO format string or None.
    """
    if dt is None:
        return None
    return dt.isoformat()
