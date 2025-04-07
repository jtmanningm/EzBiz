# /pages/business/__init__.py
"""
Initialize business portal pages package
"""
from .auth.business_login import business_login_page
from .auth.admin_setup import setup_admin_page

__all__ = [
    'business_login_page',
    'setup_admin_page'
]
