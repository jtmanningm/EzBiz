# /pages/portal/services/__init__.py
"""
Initialize customer portal services pages
"""

from .book import book_service_page
from .history import service_history_page
from .upcoming import upcoming_services_page

__all__ = [
    'book_service_page',
    'service_history_page',
    'upcoming_services_page'
]
