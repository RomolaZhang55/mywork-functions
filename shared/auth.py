"""
Authentication module for API key validation
"""
import os
from typing import Optional


class AuthenticationError(Exception):
    """Raised when authentication fails"""
    pass


def validate_api_key(api_key: Optional[str]) -> bool:
    """
    Validate the provided API key against the configured key
    
    Args:
        api_key: The API key to validate
        
    Returns:
        True if valid, False otherwise
    """
    configured_key = os.getenv('API_KEY')
    
    # If no key is configured, allow all requests (development mode)
    if not configured_key:
        return True
    
    # If no key provided, deny access
    if not api_key:
        return False
    
    return api_key == configured_key


def require_api_key(api_key: Optional[str]) -> None:
    """
    Require a valid API key, raise exception if invalid
    
    Args:
        api_key: The API key to validate
        
    Raises:
        AuthenticationError: If the API key is invalid
    """
    if not validate_api_key(api_key):
        raise AuthenticationError('Invalid or missing API key')

