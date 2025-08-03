# /pages/portal/auth/__init__.py
"""
Initialize customer portal authentication pages
"""

from pages.portal.auth.register import register_customer_page
from pages.portal.auth.Verify import verify_email_page

__all__ = [
   'register_customer_page',
   'verify_email_page'
]