# pages/__init__.py
"""
Initialize pages module and expose page functions
"""

from .new_service import new_service_page
from .scheduled import scheduled_services_page
from .completed import completed_services_page
from .transaction_details import transaction_details_page

__all__ = [
    'new_service_page',
    'scheduled_services_page',
    'completed_services_page',
    'transaction_details_page',
]

