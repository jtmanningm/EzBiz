# /pages/business/__init__.py
"""
Initialize business portal pages package
"""
from .auth.admin_setup import setup_admin_page

__all__ = [
    'setup_admin_page'
]
