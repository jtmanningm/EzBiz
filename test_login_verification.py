#!/usr/bin/env python3
"""
Test login verification for Jeremy's account
Simulates the exact login process from unified_login.py
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.connection import snowflake_conn
from utils.auth.auth_utils import verify_password
from utils.business.business_auth import create_business_session

def test_login_process():
    """Test the complete login process"""
    print("🧪 Testing Complete Login Process")
    print("=" * 50)
    
    # Test credentials
    email = "jmanning1992@icloud.com"
    password = "test123"
    
    print(f"Testing login for: {email}")
    print(f"With password: {password}")
    
    try:
        # Step 1: Try business login (from unified_login.py)
        print("\n🔍 Step 1: Querying business portal users...")
        query = """
        SELECT 
            PORTAL_USER_ID,
            PASSWORD_HASH,
            IS_ACTIVE
        FROM OPERATIONAL.CARPET.BUSINESS_PORTAL_USERS
        WHERE EMAIL = ?
        """
        
        result = snowflake_conn.execute_query(query, [email.lower()])
        
        if not result or len(result) == 0:
            print("❌ No business user found")
            return False
            
        user = result[0]
        print(f"✅ Business user found: Portal ID {user['PORTAL_USER_ID']}")
        
        # Step 2: Verify password
        print("\n🔐 Step 2: Verifying password...")
        if not verify_password(password, user['PASSWORD_HASH']):
            print("❌ Password verification failed")
            return False
            
        print("✅ Password verification successful")
        
        # Step 3: Check if account is active
        print("\n✅ Step 3: Checking account status...")
        if not user['IS_ACTIVE']:
            print("❌ Account is inactive")
            return False
            
        print("✅ Account is active")
        
        # Step 4: Create business session
        print("\n🎫 Step 4: Creating business session...")
        session_id = create_business_session(
            user['PORTAL_USER_ID'],
            'test-ip',
            'test-agent'
        )
        
        if not session_id:
            print("❌ Session creation failed")
            return False
            
        print(f"✅ Session created: {session_id}")
        
        print("\n🎉 LOGIN TEST RESULTS:")
        print("✅ User lookup: SUCCESS")
        print("✅ Password verification: SUCCESS") 
        print("✅ Account status: ACTIVE")
        print("✅ Session creation: SUCCESS")
        print("\n✅ LOGIN SHOULD WORK!")
        
        return True
        
    except Exception as e:
        print(f"❌ Login test failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_login_process()
    if success:
        print("\n🏆 Login credentials are working correctly!")
        print(f"📧 Email: jmanning1992@icloud.com")
        print(f"🔑 Password: {password}")
        print("\nIf you're still having issues, please check:")
        print("1. Make sure you're using the exact email and password above")
        print("2. Try clearing your browser cache")
        print("3. Make sure you're on the unified login page")
    else:
        print("\n💥 Login test failed - there may be an issue with the system")