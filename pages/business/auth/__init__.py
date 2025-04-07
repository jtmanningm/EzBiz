# /pages/business/auth/__init__.py
"""
Initialize business auth pages
"""
from .business_login import business_login_page
from .admin_setup import setup_admin_page

__all__ = [
    'business_login_page',
    'setup_admin_page'
]