# /models/portal/__init__.py
"""
Initialize portal models module and expose user management functionality
"""

from .user import (
    PortalUser,
    get_portal_user,
    get_portal_user_by_email,
    update_portal_user,
    create_portal_user,
    update_login_attempt
)

__all__ = [
    'PortalUser',
    'get_portal_user',
    'get_portal_user_by_email',
    'update_portal_user',
    'create_portal_user',
    'update_login_attempt'
]