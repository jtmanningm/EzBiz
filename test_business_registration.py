#!/usr/bin/env python3
"""
Test business user registration
Creates a test business user account through the registration system
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from database.connection import SnowflakeConnection
from utils.auth.auth_utils import hash_password, validate_password, validate_email
from utils.validation import validate_phone, sanitize_zip_code
from pages.auth.business_register import (
    validate_business_registration_data,
    check_existing_business_user,
    create_business_info,
    create_employee_record,
    create_business_portal_user
)
import random
import string

def generate_test_business_data():
    """Generate unique test business data"""
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    random_suffix = ''.join(random.choices(string.ascii_lowercase, k=4))
    
    return {
        'business_name': f'Ez Biz Test Cleaning {random_suffix}',
        'first_name': 'Test',
        'last_name': f'Owner{random_suffix}',
        'email': f'test.business.{timestamp}@ezbiz.com',
        'phone': f'555{random.randint(1000000, 9999999)}',
        'password': 'TestBusiness123!',
        'confirm_password': 'TestBusiness123!',
        'street_address': '456 Business Blvd',
        'city': 'Business City',
        'state': 'NC',
        'zip_code': '54321'
    }

def test_business_registration():
    """Test complete business registration flow"""
    print("🧪 Testing Business User Registration")
    print("=" * 60)
    
    try:
        # Generate test data
        test_data = generate_test_business_data()
        print(f"Generated test business: {test_data['business_name']}")
        print(f"Owner: {test_data['first_name']} {test_data['last_name']}")
        print(f"Email: {test_data['email']}")
        print(f"Phone: {test_data['phone']}")
        
        # Test 1: Validate registration data
        print(f"\n📋 Test 1: Validating registration data...")
        errors = validate_business_registration_data(test_data)
        if errors:
            print(f"❌ Validation failed:")
            for error in errors:
                print(f"   - {error}")
            return False
        print("✅ Registration data validation passed")
        
        # Test 2: Check for existing business user
        print(f"\n👤 Test 2: Checking for existing business user...")
        if check_existing_business_user(test_data['email']):
            print("❌ Business user already exists with this email")
            return False
        print("✅ Email available for business registration")
        
        # Test 3: Create business information
        print(f"\n🏢 Test 3: Creating business information...")
        business_id = create_business_info(test_data)
        if not business_id:
            print("❌ Failed to create business information")
            return False
        print(f"✅ Business information created with ID: {business_id}")
        
        # Test 4: Create employee record
        print(f"\n👔 Test 4: Creating employee record...")
        employee_id = create_employee_record(test_data, business_id)
        if not employee_id:
            print("❌ Failed to create employee record")
            return False
        print(f"✅ Employee record created with ID: {employee_id}")
        
        # Test 5: Create business portal user
        print(f"\n🔐 Test 5: Creating business portal user...")
        portal_success = create_business_portal_user(test_data, employee_id)
        if not portal_success:
            print("❌ Failed to create business portal user")
            return False
        print("✅ Business portal user created successfully")
        
        # Test 6: Verify complete registration
        print(f"\n✅ Test 6: Verifying registration...")
        conn = SnowflakeConnection.get_instance()
        
        verify_query = """
        SELECT 
            bi.BUSINESS_ID,
            bi.BUSINESS_NAME,
            e.EMPLOYEE_ID,
            e.FIRST_NAME,
            e.LAST_NAME,
            e.EMAIL,
            bpu.PORTAL_USER_ID,
            bpu.IS_ADMIN,
            bpu.IS_ACTIVE
        FROM OPERATIONAL.CARPET.BUSINESS_INFO bi,
             OPERATIONAL.CARPET.EMPLOYEE e,
             OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS bpu 
        WHERE e.EMAIL = ?
        AND bpu.EMPLOYEE_ID = e.EMPLOYEE_ID
        AND bi.EMAIL_ADDRESS = e.EMAIL
        """
        
        result = conn.execute_query(verify_query, [test_data['email']])
        if result:
            user_data = result[0]
            print(f"✅ Registration verified:")
            print(f"   Business: {user_data['BUSINESS_NAME']}")
            print(f"   Business ID: {user_data['BUSINESS_ID']}")
            print(f"   Owner: {user_data['FIRST_NAME']} {user_data['LAST_NAME']}")
            print(f"   Employee ID: {user_data['EMPLOYEE_ID']}")
            print(f"   Portal User ID: {user_data['PORTAL_USER_ID']}")
            print(f"   Admin: {user_data['IS_ADMIN']}")
            print(f"   Active: {user_data['IS_ACTIVE']}")
        else:
            print("❌ Could not verify registration")
            return False
        
        print(f"\n🎉 Business Registration Test Results:")
        print("✅ Data validation: WORKING")
        print("✅ Email uniqueness check: WORKING")
        print("✅ Business info creation: WORKING")
        print("✅ Employee record creation: WORKING")
        print("✅ Portal user creation: WORKING")
        print("✅ Registration verification: WORKING")
        
        return {'business_id': business_id, 'employee_id': employee_id, 'test_data': test_data}
        
    except Exception as e:
        print(f"❌ Business registration test failed: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

def test_business_login(registration_data):
    """Test business login with registered user"""
    print(f"\n🧪 Testing Business User Login")
    print("=" * 60)
    
    try:
        test_data = registration_data['test_data']
        
        # Test login query (similar to what would be used in business login)
        print(f"\n🔐 Testing login for: {test_data['email']}")
        
        conn = SnowflakeConnection.get_instance()
        login_query = """
        SELECT 
            bpu.PORTAL_USER_ID,
            bpu.PASSWORD_HASH,
            bpu.IS_ADMIN,
            bpu.IS_ACTIVE,
            e.EMPLOYEE_ID,
            e.FIRST_NAME,
            e.LAST_NAME,
            bi.BUSINESS_ID,
            bi.BUSINESS_NAME
        FROM OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS bpu,
             OPERATIONAL.CARPET.EMPLOYEE e,
             OPERATIONAL.CARPET.BUSINESS_INFO bi
        WHERE bpu.EMAIL = ? 
        AND bpu.IS_ACTIVE = TRUE
        AND bpu.EMPLOYEE_ID = e.EMPLOYEE_ID
        AND bi.EMAIL_ADDRESS = e.EMAIL
        """
        
        result = conn.execute_query(login_query, [test_data['email'].lower()])
        if result:
            user_data = result[0]
            print("✅ Business user found for login:")
            print(f"   Portal User ID: {user_data['PORTAL_USER_ID']}")
            print(f"   Business: {user_data['BUSINESS_NAME']}")
            print(f"   User: {user_data['FIRST_NAME']} {user_data['LAST_NAME']}")
            print(f"   Admin: {user_data['IS_ADMIN']}")
            print(f"   Active: {user_data['IS_ACTIVE']}")
            
            # Test password verification
            from utils.auth.auth_utils import verify_password
            if verify_password(test_data['password'], user_data['PASSWORD_HASH']):
                print("✅ Password verification: WORKING")
            else:
                print("❌ Password verification: FAILED")
                return False
                
        else:
            print("❌ Business user not found for login")
            return False
        
        print(f"\n🎉 Business Login Test: SUCCESS!")
        return True
        
    except Exception as e:
        print(f"❌ Business login test failed: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

def test_complete_business_flow():
    """Test complete business registration and login flow"""
    print("🌟 COMPLETE BUSINESS USER FLOW TEST")
    print("=" * 80)
    
    # Phase 1: Registration
    registration_result = test_business_registration()
    if not registration_result:
        print("\n💥 Business registration failed - cannot continue")
        return False
    
    # Phase 2: Login test
    login_result = test_business_login(registration_result)
    if not login_result:
        print("\n💥 Business login test failed")
        return False
    
    print(f"\n🏆 COMPLETE BUSINESS USER FLOW: SUCCESS!")
    print("✅ Business can register and login through portal")
    
    return True

if __name__ == "__main__":
    success = test_complete_business_flow()
    if success:
        print("\n🎉 Business User Registration System is ready for production use!")
    else:
        print("\n⚠️ Business User Registration System needs fixes before deployment.")