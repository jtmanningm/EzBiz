#!/usr/bin/env python3
"""
Email testing utility for debugging email functionality
"""
import streamlit as st
from utils.email import send_email, EmailStatus
from pages.settings.business import fetch_business_info

def test_email_functionality():
    """Test email sending functionality"""
    print("🔍 Testing email functionality...")
    
    # Check if secrets are available
    try:
        mailgun_config = st.secrets.get("mailgun", {})
        if not mailgun_config:
            print("❌ No mailgun configuration in secrets")
            return False
            
        api_key = mailgun_config.get("api_key")
        domain = mailgun_config.get("domain")
        
        print(f"✅ Mailgun API Key: {'Present' if api_key else 'Missing'}")
        print(f"✅ Mailgun Domain: {domain if domain else 'Missing'}")
        
        if not api_key:
            print("❌ Mailgun API key is required")
            return False
            
    except Exception as e:
        print(f"❌ Error checking secrets: {str(e)}")
        return False
    
    # Get business info
    try:
        business_info = fetch_business_info()
        if business_info:
            print(f"✅ Business info loaded: {business_info.get('BUSINESS_NAME', 'Unknown')}")
        else:
            print("⚠️ No business info available")
            business_info = {"BUSINESS_NAME": "Test Business"}
    except Exception as e:
        print(f"⚠️ Error loading business info: {str(e)}")
        business_info = {"BUSINESS_NAME": "Test Business"}
    
    # Test email sending
    test_email = "jmanning1992@icloud.com"
    test_subject = "Test Email from Ez Biz"
    test_content = """
This is a test email to verify email functionality is working.

If you receive this email, the email system is configured correctly.

Best regards,
Ez Biz Support
"""
    
    print(f"📧 Attempting to send test email to {test_email}...")
    
    try:
        result = send_email(
            to_email=test_email,
            subject=test_subject,
            content=test_content,
            business_info=business_info
        )
        
        if result.success:
            print(f"✅ Email sent successfully! Email ID: {result.email_id}")
            return True
        else:
            print(f"❌ Email failed to send: {result.message}")
            return False
            
    except Exception as e:
        print(f"❌ Exception while sending email: {str(e)}")
        return False

if __name__ == "__main__":
    success = test_email_functionality()
    if success:
        print("\n🎉 Email test PASSED")
    else:
        print("\n❌ Email test FAILED")
        print("\nTroubleshooting steps:")
        print("1. Check that Mailgun API key is set in Streamlit secrets")
        print("2. Verify Mailgun domain is configured correctly")
        print("3. Check that the sender domain is authorized in Mailgun")
        print("4. Ensure business info is properly configured in the database")