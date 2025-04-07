# /pages/portal/auth/__init__.py
"""
Initialize customer portal authentication pages
"""

from .login import customer_login_page
from pages.portal.auth.register import register_customer_page
from pages.portal.auth.reset import request_reset_page, reset_password_page
from pages.portal.auth.Verify import verify_email_page

__all__ = [
   'customer_login_page',
   'register_customer_page',
   'request_reset_page',
   'reset_password_page',
   'verify_email_page'
]