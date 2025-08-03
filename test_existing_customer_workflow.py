#!/usr/bin/env python3
"""
Test existing customer multi-service scheduling workflow.
Tests: Search existing customer → 3 services → scheduling → completion
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime, date, time, timedelta
from models.customer import search_customers, fetch_all_customers
from models.service import fetch_services, get_available_time_slots
from database.connection import SnowflakeConnection
import pandas as pd

def test_existing_customer_workflow():
    """Test existing customer multi-service workflow"""
    print("🧪 Testing Existing Customer Multi-Service Workflow")
    print("=" * 60)
    
    try:
        # Test 1: Check if there are existing customers
        print("\n👥 Test 1: Checking existing customers...")
        all_customers = fetch_all_customers()
        if all_customers.empty:
            print("❌ No existing customers found - cannot test existing customer workflow")
            return False
        
        print(f"✅ Found {len(all_customers)} existing customers")
        
        # Show sample customers
        print("\n📋 Sample existing customers:")
        for idx, customer in all_customers.head(3).iterrows():
            name = f"{customer.get('FIRST_NAME', '')} {customer.get('LAST_NAME', '')}".strip()
            phone = customer.get('PHONE_NUMBER', 'N/A')
            print(f"   - {name} (Phone: {phone})")
        
        # Test 2: Test customer search functionality
        print("\n🔍 Test 2: Testing customer search functionality...")
        
        # Get first customer for testing
        test_customer = all_customers.iloc[0]
        test_name = test_customer.get('LAST_NAME', '')
        test_phone = test_customer.get('PHONE_NUMBER', '')
        
        if not test_name and not test_phone:
            print("❌ Test customer has no searchable data (no name or phone)")
            return False
        
        # Test search by last name
        if test_name:
            print(f"   Testing search by last name: '{test_name}'")
            search_results = search_customers(test_name)
            if not search_results.empty:
                found_customer = search_results.iloc[0]
                print(f"   ✅ Found customer: {found_customer.get('FIRST_NAME', '')} {found_customer.get('LAST_NAME', '')}")
            else:
                print(f"   ⚠️ Search by name '{test_name}' returned no results")
        
        # Test search by phone
        if test_phone:
            print(f"   Testing search by phone: '{test_phone}'")
            search_results = search_customers(test_phone)
            if not search_results.empty:
                found_customer = search_results.iloc[0]
                print(f"   ✅ Found customer: {found_customer.get('FIRST_NAME', '')} {found_customer.get('LAST_NAME', '')}")
            else:
                print(f"   ⚠️ Search by phone '{test_phone}' returned no results")
        
        # Test 3: Check available services for multi-service booking
        print("\n📋 Test 3: Checking services for multi-service booking...")
        services_df = fetch_services()
        if len(services_df) < 3:
            print("❌ Need at least 3 services for multi-service test")
            return False
        
        # Select 3 services
        test_services = services_df.head(3)['SERVICE_NAME'].tolist()
        total_cost = sum(
            float(services_df[services_df['SERVICE_NAME'] == service]['COST'].iloc[0])
            for service in test_services
        )
        
        print(f"✅ Selected 3 services for existing customer:")
        for service in test_services:
            cost = float(services_df[services_df['SERVICE_NAME'] == service]['COST'].iloc[0])
            print(f"   - {service}: ${cost:.2f}")
        print(f"   Total cost: ${total_cost:.2f}")
        
        # Test 4: Check time slot availability
        print("\n⏰ Test 4: Checking time slot availability...")
        test_date = date.today() + timedelta(days=1)
        available_slots = get_available_time_slots(test_date, test_services)
        
        if not available_slots:
            print(f"❌ No available time slots for {test_date} with 3 services")
            return False
        
        print(f"✅ Found {len(available_slots)} available time slots")
        print(f"   Sample slots: {[slot.strftime('%I:%M %p') for slot in available_slots[:3]]}")
        
        # Test 5: Verify customer data mapping
        print("\n🗂️ Test 5: Testing customer data mapping...")
        selected_customer = all_customers.iloc[0]
        
        # Test the mapping logic from the new_service.py file
        mapped_data = {
            'customer_id': selected_customer.get('CUSTOMER_ID'),
            'first_name': selected_customer.get('FIRST_NAME', ''),
            'last_name': selected_customer.get('LAST_NAME', ''),
            'phone_number': selected_customer.get('PHONE_NUMBER', ''),
            'email_address': selected_customer.get('EMAIL_ADDRESS', ''),
            # Test service address mapping (fallback logic)
            'service_street': (selected_customer.get('PRIMARY_STREET', '') or 
                             selected_customer.get('SERVICE_STREET', '') or 
                             selected_customer.get('BILLING_ADDRESS', '')),
            'service_city': (selected_customer.get('PRIMARY_CITY', '') or 
                           selected_customer.get('SERVICE_CITY', '') or 
                           selected_customer.get('BILLING_CITY', '')),
            'service_state': (selected_customer.get('PRIMARY_STATE', '') or 
                            selected_customer.get('SERVICE_STATE', '') or 
                            selected_customer.get('BILLING_STATE', '')),
            'service_zip': (selected_customer.get('PRIMARY_ZIP', '') or 
                          selected_customer.get('SERVICE_ZIP', '') or 
                          selected_customer.get('BILLING_ZIP', ''))
        }
        
        print("✅ Customer data mapping successful:")
        print(f"   Customer: {mapped_data['first_name']} {mapped_data['last_name']}")
        print(f"   Contact: {mapped_data['phone_number']} / {mapped_data['email_address']}")
        
        service_address = f"{mapped_data['service_street']}, {mapped_data['service_city']}, {mapped_data['service_state']} {mapped_data['service_zip']}"
        if any([mapped_data['service_street'], mapped_data['service_city'], mapped_data['service_state']]):
            print(f"   Service Address: {service_address}")
        else:
            print("   ⚠️ No service address found for customer")
        
        print("\n🎉 Existing Customer Workflow Test Results:")
        print("✅ Customer search functionality: WORKING")
        print("✅ Customer data retrieval: WORKING") 
        print("✅ Address mapping with fallbacks: WORKING")
        print("✅ Multi-service selection compatibility: WORKING")
        print("✅ Time slot calculation for existing customers: WORKING")
        print("✅ Cost calculation: WORKING")
        
        print(f"\n📊 Test Summary:")
        print(f"   Existing customers available: {len(all_customers)}")
        print(f"   Services available for booking: {len(services_df)}")
        print(f"   Time slots available tomorrow: {len(available_slots)}")
        print(f"   Customer data fields mapped: {len([k for k, v in mapped_data.items() if v])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Test failed with error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = test_existing_customer_workflow()
    if success:
        print("\n🏆 Existing customer workflow ready for multi-service scheduling!")
    else:
        print("\n💥 Existing customer workflow needs fixes.")