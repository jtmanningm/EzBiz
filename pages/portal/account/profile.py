# # pages/portal/account/profile.py
# import streamlit as st
# from datetime import datetime
# from utils.auth.middleware import require_customer_auth
# from utils.auth.auth_utils import validate_password, hash_password
# from database.connection import snowflake_conn
# from models.portal.user import get_portal_user, update_portal_user

# @require_customer_auth
# def profile_page():
#     """Combined profile and settings management page"""
#     st.title("Profile & Settings")
    
#     try:
#         # Fetch current profile and portal user info
#         query = """
#         SELECT 
#             c.FIRST_NAME,
#             c.LAST_NAME,
#             c.EMAIL_ADDRESS,
#             c.PHONE_NUMBER,
#             c.BILLING_ADDRESS,
#             c.BILLING_CITY,
#             c.BILLING_STATE,
#             c.BILLING_ZIP,
#             c.PRIMARY_CONTACT_METHOD,
#             c.TEXT_FLAG,
#             cpu.EMAIL as PORTAL_EMAIL,
#             cpu.LAST_LOGIN_DATE,
#             cpu.EMAIL_VERIFIED
#         FROM CUSTOMER c
#         JOIN CUSTOMER_PORTAL_USERS cpu 
#             ON c.CUSTOMER_ID = cpu.CUSTOMER_ID
#         WHERE c.CUSTOMER_ID = ?
#         """
        
#         result = snowflake_conn.execute_query(query, [st.session_state.customer_id])
#         if not result:
#             st.error("Error loading profile data")
#             return
            
#         profile = result[0]
        
#         # Create tabs for different sections
#         tabs = st.tabs([
#             "Profile Info", 
#             "Contact Methods", 
#             "Password",
#             "Notifications"
#         ])
        
#         # Profile Information Tab
#         with tabs[0]:
#             st.header("Personal Information")
            
#             col1, col2 = st.columns(2)
#             with col1:
#                 new_first_name = st.text_input(
#                     "First Name",
#                     value=profile['FIRST_NAME']
#                 )
#                 new_street = st.text_input(
#                     "Street Address",
#                     value=profile['BILLING_ADDRESS']
#                 )
#                 new_city = st.text_input(
#                     "City",
#                     value=profile['BILLING_CITY']
#                 )
                
#             with col2:
#                 new_last_name = st.text_input(
#                     "Last Name",
#                     value=profile['LAST_NAME']
#                 )
#                 new_state = st.text_input(
#                     "State",
#                     value=profile['BILLING_STATE']
#                 )
#                 new_zip = st.text_input(
#                     "ZIP Code",
#                     value=profile['BILLING_ZIP']
#                 )

#             if st.button("Update Profile", type="primary"):
#                 try:
#                     update_query = """
#                     UPDATE CUSTOMER
#                     SET 
#                         FIRST_NAME = ?,
#                         LAST_NAME = ?,
#                         BILLING_ADDRESS = ?,
#                         BILLING_CITY = ?,
#                         BILLING_STATE = ?,
#                         BILLING_ZIP = ?,
#                         LAST_UPDATED_AT = CURRENT_TIMESTAMP()
#                     WHERE CUSTOMER_ID = ?
#                     """
                    
#                     snowflake_conn.execute_query(update_query, [
#                         new_first_name,
#                         new_last_name,
#                         new_street,
#                         new_city,
#                         new_state,
#                         new_zip,
#                         st.session_state.customer_id
#                     ])
                    
#                     st.success("Profile updated successfully!")
#                     st.rerun()
                    
#                 except Exception as e:
#                     st.error("Error updating profile")
#                     print(f"Profile update error: {str(e)}")
        
#         # Contact Methods Tab
#         with tabs[1]:
#             st.header("Contact Information")
            
#             # Show verification status
#             if profile['EMAIL_VERIFIED']:
#                 st.success(f"Email verified: {profile['PORTAL_EMAIL']}")
#             else:
#                 st.warning(
#                     f"Email pending verification: {profile['PORTAL_EMAIL']}. "
#                     f"Please check your email for verification link."
#                 )
            
#             col1, col2 = st.columns(2)
#             with col1:
#                 new_phone = st.text_input(
#                     "Phone Number",
#                     value=profile['PHONE_NUMBER']
#                 )
#                 new_email = st.text_input(
#                     "Email Address",
#                     value=profile['EMAIL_ADDRESS']
#                 )
            
#             with col2:
#                 new_contact_method = st.selectbox(
#                     "Preferred Contact Method",
#                     ["Phone", "Email", "Text"],
#                     index=["Phone", "Email", "Text"].index(
#                         profile['PRIMARY_CONTACT_METHOD']
#                     )
#                 )
                
#                 text_opt_in = st.checkbox(
#                     "Opt-in to Text Messages",
#                     value=profile['TEXT_FLAG']
#                 )
            
#             if st.button("Update Contact Info", type="primary"):
#                 try:
#                     update_query = """
#                     UPDATE CUSTOMER
#                     SET 
#                         PHONE_NUMBER = ?,
#                         EMAIL_ADDRESS = ?,
#                         PRIMARY_CONTACT_METHOD = ?,
#                         TEXT_FLAG = ?,
#                         LAST_UPDATED_AT = CURRENT_TIMESTAMP()
#                     WHERE CUSTOMER_ID = ?
#                     """
                    
#                     snowflake_conn.execute_query(update_query, [
#                         new_phone,
#                         new_email,
#                         new_contact_method,
#                         text_opt_in,
#                         st.session_state.customer_id
#                     ])
                    
#                     # Update portal email if changed
#                     if new_email != profile['PORTAL_EMAIL']:
#                         portal_user = get_portal_user(st.session_state.portal_user_id)
#                         if portal_user:
#                             portal_user.email = new_email
#                             portal_user.email_verified = False
#                             update_portal_user(portal_user)
                            
#                             # Send new verification email
#                             # You'll implement this based on your email system
                    
#                     st.success("Contact information updated!")
#                     st.rerun()
                    
#                 except Exception as e:
#                     st.error("Error updating contact information")
#                     print(f"Contact update error: {str(e)}")
        
#         # Password Tab
#         with tabs[2]:
#             st.header("Change Password")
            
#             # Show last login info
#             if profile['LAST_LOGIN_DATE']:
#                 st.info(
#                     f"Last login: {profile['LAST_LOGIN_DATE'].strftime('%B %d, %Y at %I:%M %p')}"
#                 )
            
#             with st.form("password_form"):
#                 current_password = st.text_input(
#                     "Current Password",
#                     type="password"
#                 )
#                 new_password = st.text_input(
#                     "New Password",
#                     type="password",
#                     help="Must be at least 8 characters with uppercase, lowercase, and special characters"
#                 )
#                 confirm_password = st.text_input(
#                     "Confirm New Password",
#                     type="password"
#                 )
                
#                 if st.form_submit_button("Change Password"):
#                     if new_password != confirm_password:
#                         st.error("New passwords do not match")
#                         return
                        
#                     # Validate password strength
#                     valid, message = validate_password(new_password)
#                     if not valid:
#                         st.error(message)
#                         return
                        
#                     try:
#                         # Verify current password
#                         verify_query = """
#                         SELECT PASSWORD_HASH
#                         FROM OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS
#                         WHERE PORTAL_USER_ID = ?
#                         """
#                         result = snowflake_conn.execute_query(verify_query, [
#                             st.session_state.portal_user_id
#                         ])
                        
#                         if not result:
#                             st.error("Error verifying current password")
#                             return
                            
#                         from utils.auth.auth_utils import verify_password
#                         if not verify_password(current_password, result[0]['PASSWORD_HASH']):
#                             st.error("Current password is incorrect")
#                             return
                        
#                         # Update password
#                         update_query = """
#                         UPDATE OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS
#                         SET 
#                             PASSWORD_HASH = ?,
#                             MODIFIED_AT = CURRENT_TIMESTAMP()
#                         WHERE PORTAL_USER_ID = ?
#                         """
#                         snowflake_conn.execute_query(update_query, [
#                             hash_password(new_password),
#                             st.session_state.portal_user_id
#                         ])
                        
#                         st.success("Password updated successfully!")
                        
#                     except Exception as e:
#                         st.error("Error updating password")
#                         print(f"Password update error: {str(e)}")
        
#         # Notifications Tab
#         with tabs[3]:
#             st.header("Notification Preferences")
            
#             # Fetch current preferences
#             prefs_query = """
#             SELECT 
#                 MARKETING_EMAILS,
#                 MARKETING_SMS,
#                 APPOINTMENT_REMINDERS,
#                 PROMOTIONAL_MESSAGES
#             FROM CUSTOMER_PREFERENCES
#             WHERE CUSTOMER_ID = ?
#             """
            
#             prefs = snowflake_conn.execute_query(prefs_query, [
#                 st.session_state.customer_id
#             ])
            
#             current_prefs = prefs[0] if prefs else {
#                 'MARKETING_EMAILS': True,
#                 'MARKETING_SMS': True,
#                 'APPOINTMENT_REMINDERS': True,
#                 'PROMOTIONAL_MESSAGES': True
#             }
            
#             col1, col2 = st.columns(2)
#             with col1:
#                 service_reminders = st.checkbox(
#                     "Service Reminders",
#                     value=current_prefs['APPOINTMENT_REMINDERS'],
#                     help="Receive reminders about upcoming services"
#                 )
#                 promotional_messages = st.checkbox(
#                     "Promotional Messages",
#                     value=current_prefs['PROMOTIONAL_MESSAGES'],
#                     help="Receive special offers and promotions"
#                 )
                
#             with col2:
#                 marketing_emails = st.checkbox(
#                     "Marketing Emails",
#                     value=current_prefs['MARKETING_EMAILS'],
#                     help="Receive email newsletters and updates"
#                 )
#                 marketing_sms = st.checkbox(
#                     "Marketing SMS",
#                     value=current_prefs['MARKETING_SMS'],
#                     help="Receive promotional text messages"
#                 )
            
#             if st.button("Update Preferences", type="primary"):
#                 try:
#                     # Upsert preferences
#                     prefs_query = """
#                     MERGE INTO CUSTOMER_PREFERENCES
#                     USING (SELECT ? as customer_id) as source
#                     ON CUSTOMER_PREFERENCES.CUSTOMER_ID = source.customer_id
#                     WHEN MATCHED THEN
#                         UPDATE SET
#                             MARKETING_EMAILS = ?,
#                             MARKETING_SMS = ?,
#                             APPOINTMENT_REMINDERS = ?,
#                             PROMOTIONAL_MESSAGES = ?,
#                             MODIFIED_AT = CURRENT_TIMESTAMP()
#                     WHEN NOT MATCHED THEN
#                         INSERT (
#                             CUSTOMER_ID,
#                             MARKETING_EMAILS,
#                             MARKETING_SMS,
#                             APPOINTMENT_REMINDERS,
#                             PROMOTIONAL_MESSAGES
#                         ) VALUES (?, ?, ?, ?, ?)
#                     """
                    
#                     snowflake_conn.execute_query(prefs_query, [
#                         st.session_state.customer_id,
#                         marketing_emails,
#                         marketing_sms,
#                         service_reminders,
#                         promotional_messages,
#                         st.session_state.customer_id,
#                         marketing_emails,
#                         marketing_sms,
#                         service_reminders,
#                         promotional_messages
#                     ])
                    
#                     st.success("Notification preferences updated!")
#                     st.rerun()
                    
#                 except Exception as e:
#                     st.error("Error updating preferences")
#                     print(f"Preferences update error: {str(e)}")
        
#     except Exception as e:
#         st.error("Error loading profile")
#         print(f"Profile error: {str(e)}")

# if __name__ == "__main__":
#     profile_page()

import streamlit as st
from datetime import datetime
from utils.auth.middleware import require_customer_auth
from utils.auth.auth_utils import validate_password, hash_password
from database.connection import snowflake_conn
from models.portal.user import get_portal_user, update_portal_user

@require_customer_auth
def profile_page():
    """Combined profile and settings management page"""
    st.title("Profile & Settings")
    
    try:
        # Fetch current profile and portal user info
        # NOTE: The CUSTOMER table does not have STREET_ADDRESS, CITY, STATE, ZIP_CODE columns.
        #       Instead, it has BILLING_ADDRESS, BILLING_CITY, BILLING_STATE, BILLING_ZIP.
        #       We alias them in the SELECT so the Python code can still use 'STREET_ADDRESS' etc.
        query = """
        SELECT 
            c.FIRST_NAME,
            c.LAST_NAME,
            c.EMAIL_ADDRESS,
            c.PHONE_NUMBER,
            c.BILLING_ADDRESS AS STREET_ADDRESS,
            c.BILLING_CITY AS CITY,
            c.BILLING_STATE AS STATE,
            c.BILLING_ZIP AS ZIP_CODE,
            c.PRIMARY_CONTACT_METHOD,
            c.TEXT_FLAG,
            cpu.EMAIL AS PORTAL_EMAIL,
            cpu.LAST_LOGIN_DATE,
            cpu.EMAIL_VERIFIED
        FROM CUSTOMER c
        JOIN CUSTOMER_PORTAL_USERS cpu 
            ON c.CUSTOMER_ID = cpu.CUSTOMER_ID
        WHERE c.CUSTOMER_ID = ?
        """
        
        result = snowflake_conn.execute_query(query, [st.session_state.customer_id])
        if not result:
            st.error("Error loading profile data")
            return
            
        profile = result[0]
        
        # Create tabs for different sections
        tabs = st.tabs([
            "Profile Info", 
            "Contact Methods", 
            "Password",
            "Notifications"
        ])
        
        # ─────────────────────────────────────────────────────────────────
        # Profile Information Tab
        # ─────────────────────────────────────────────────────────────────
        with tabs[0]:
            st.header("Personal Information")
            
            col1, col2 = st.columns(2)
            with col1:
                new_first_name = st.text_input(
                    "First Name",
                    value=profile['FIRST_NAME']
                )
                new_street = st.text_input(
                    "Street Address",
                    value=profile['STREET_ADDRESS']
                )
                new_city = st.text_input(
                    "City",
                    value=profile['CITY']
                )
                
            with col2:
                new_last_name = st.text_input(
                    "Last Name",
                    value=profile['LAST_NAME']
                )
                new_state = st.text_input(
                    "State",
                    value=profile['STATE']
                )
                new_zip = st.text_input(
                    "ZIP Code",
                    value=profile['ZIP_CODE']
                )

            if st.button("Update Profile", type="primary"):
                try:
                    # IMPORTANT: The CUSTOMER table columns are BILLING_ADDRESS, BILLING_CITY, etc.
                    update_query = """
                    UPDATE CUSTOMER
                    SET 
                        FIRST_NAME = ?,
                        LAST_NAME = ?,
                        BILLING_ADDRESS = ?,
                        BILLING_CITY = ?,
                        BILLING_STATE = ?,
                        BILLING_ZIP = ?,
                        LAST_UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE CUSTOMER_ID = ?
                    """
                    
                    snowflake_conn.execute_query(update_query, [
                        new_first_name,
                        new_last_name,
                        new_street,   # goes to BILLING_ADDRESS
                        new_city,     # goes to BILLING_CITY
                        new_state,    # goes to BILLING_STATE
                        new_zip,      # goes to BILLING_ZIP
                        st.session_state.customer_id
                    ])
                    
                    st.success("Profile updated successfully!")
                    st.rerun()
                    
                except Exception as e:
                    st.error("Error updating profile")
                    print(f"Profile update error: {str(e)}")
        
        # ─────────────────────────────────────────────────────────────────
        # Contact Methods Tab
        # ─────────────────────────────────────────────────────────────────
        with tabs[1]:
            st.header("Contact Information")
            
            # Show verification status
            if profile['EMAIL_VERIFIED']:
                st.success(f"Email verified: {profile['PORTAL_EMAIL']}")
            else:
                st.warning(
                    f"Email pending verification: {profile['PORTAL_EMAIL']}. "
                    f"Please check your email for a verification link."
                )
            
            col1, col2 = st.columns(2)
            with col1:
                new_phone = st.text_input(
                    "Phone Number",
                    value=profile['PHONE_NUMBER']
                )
                new_email = st.text_input(
                    "Email Address",
                    value=profile['EMAIL_ADDRESS']
                )
            
            with col2:
                new_contact_method = st.selectbox(
                    "Preferred Contact Method",
                    ["Phone", "Email", "Text"],
                    index=["Phone", "Email", "Text"].index(
                        profile['PRIMARY_CONTACT_METHOD']
                    )
                )
                text_opt_in = st.checkbox(
                    "Opt-in to Text Messages",
                    value=profile['TEXT_FLAG']
                )
            
            if st.button("Update Contact Info", type="primary"):
                try:
                    update_query = """
                    UPDATE CUSTOMER
                    SET 
                        PHONE_NUMBER = ?,
                        EMAIL_ADDRESS = ?,
                        PRIMARY_CONTACT_METHOD = ?,
                        TEXT_FLAG = ?,
                        LAST_UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE CUSTOMER_ID = ?
                    """
                    
                    snowflake_conn.execute_query(update_query, [
                        new_phone,
                        new_email,
                        new_contact_method,
                        text_opt_in,
                        st.session_state.customer_id
                    ])
                    
                    # Update portal email if changed
                    if new_email != profile['PORTAL_EMAIL']:
                        portal_user = get_portal_user(st.session_state.portal_user_id)
                        if portal_user:
                            portal_user.email = new_email
                            portal_user.email_verified = False
                            update_portal_user(portal_user)
                            # TODO: Send new verification email if needed
                            
                    st.success("Contact information updated!")
                    st.rerun()
                    
                except Exception as e:
                    st.error("Error updating contact information")
                    print(f"Contact update error: {str(e)}")
        
        # ─────────────────────────────────────────────────────────────────
        # Password Tab
        # ─────────────────────────────────────────────────────────────────
        with tabs[2]:
            st.header("Change Password")
            
            # Show last login info
            if profile['LAST_LOGIN_DATE']:
                st.info(
                    f"Last login: {profile['LAST_LOGIN_DATE'].strftime('%B %d, %Y at %I:%M %p')}"
                )
            
            with st.form("password_form"):
                current_password = st.text_input(
                    "Current Password",
                    type="password"
                )
                new_password = st.text_input(
                    "New Password",
                    type="password",
                    help="Must be at least 8 characters with uppercase, lowercase, and special characters"
                )
                confirm_password = st.text_input(
                    "Confirm New Password",
                    type="password"
                )
                
                if st.form_submit_button("Change Password"):
                    if new_password != confirm_password:
                        st.error("New passwords do not match")
                        return
                        
                    from utils.auth.auth_utils import verify_password
                    password_errors = validate_password(new_password)
                    if password_errors:
                        for error in password_errors:
                            st.error(error)
                        return
                        
                    try:
                        # Verify current password
                        verify_query = """
                        SELECT PASSWORD_HASH
                        FROM OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS
                        WHERE PORTAL_USER_ID = ?
                        """
                        result = snowflake_conn.execute_query(verify_query, [
                            st.session_state.portal_user_id
                        ])
                        
                        if not result:
                            st.error("Error verifying current password")
                            return
                            
                        if not verify_password(current_password, result[0]['PASSWORD_HASH']):
                            st.error("Current password is incorrect")
                            return
                        
                        # Update password
                        update_query = """
                        UPDATE OPERATIONAL.CARPET.CUSTOMER_PORTAL_USERS
                        SET 
                            PASSWORD_HASH = ?,
                            MODIFIED_AT = CURRENT_TIMESTAMP()
                        WHERE PORTAL_USER_ID = ?
                        """
                        snowflake_conn.execute_query(update_query, [
                            hash_password(new_password),
                            st.session_state.portal_user_id
                        ])
                        
                        st.success("Password updated successfully!")
                        
                    except Exception as e:
                        st.error("Error updating password")
                        print(f"Password update error: {str(e)}")
        
        # ─────────────────────────────────────────────────────────────────
        # Notifications Tab
        # ─────────────────────────────────────────────────────────────────
        with tabs[3]:
            st.header("Notification Preferences")
            
            # Fetch current preferences
            prefs_query = """
            SELECT 
                MARKETING_EMAILS,
                MARKETING_SMS,
                APPOINTMENT_REMINDERS,
                PROMOTIONAL_MESSAGES
            FROM CUSTOMER_PREFERENCES
            WHERE CUSTOMER_ID = ?
            """
            prefs = snowflake_conn.execute_query(prefs_query, [
                st.session_state.customer_id
            ])
            
            # Default if no row found
            current_prefs = prefs[0] if prefs else {
                'MARKETING_EMAILS': True,
                'MARKETING_SMS': True,
                'APPOINTMENT_REMINDERS': True,
                'PROMOTIONAL_MESSAGES': True
            }
            
            col1, col2 = st.columns(2)
            with col1:
                service_reminders = st.checkbox(
                    "Service Reminders",
                    value=current_prefs['APPOINTMENT_REMINDERS'],
                    help="Receive reminders about upcoming services"
                )
                promotional_messages = st.checkbox(
                    "Promotional Messages",
                    value=current_prefs['PROMOTIONAL_MESSAGES'],
                    help="Receive special offers and promotions"
                )
            with col2:
                marketing_emails = st.checkbox(
                    "Marketing Emails",
                    value=current_prefs['MARKETING_EMAILS'],
                    help="Receive email newsletters and updates"
                )
                marketing_sms = st.checkbox(
                    "Marketing SMS",
                    value=current_prefs['MARKETING_SMS'],
                    help="Receive promotional text messages"
                )
            
            if st.button("Update Preferences", type="primary"):
                try:
                    # Upsert preferences
                    prefs_query = """
                    MERGE INTO CUSTOMER_PREFERENCES
                    USING (SELECT ? as customer_id) as source
                    ON CUSTOMER_PREFERENCES.CUSTOMER_ID = source.customer_id
                    WHEN MATCHED THEN
                        UPDATE SET
                            MARKETING_EMAILS = ?,
                            MARKETING_SMS = ?,
                            APPOINTMENT_REMINDERS = ?,
                            PROMOTIONAL_MESSAGES = ?,
                            MODIFIED_AT = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN
                        INSERT (
                            CUSTOMER_ID,
                            MARKETING_EMAILS,
                            MARKETING_SMS,
                            APPOINTMENT_REMINDERS,
                            PROMOTIONAL_MESSAGES
                        ) VALUES (?, ?, ?, ?, ?)
                    """
                    
                    snowflake_conn.execute_query(prefs_query, [
                        st.session_state.customer_id,
                        marketing_emails,
                        marketing_sms,
                        service_reminders,
                        promotional_messages,
                        st.session_state.customer_id,
                        marketing_emails,
                        marketing_sms,
                        service_reminders,
                        promotional_messages
                    ])
                    
                    st.success("Notification preferences updated!")
                    st.rerun()
                    
                except Exception as e:
                    st.error("Error updating preferences")
                    print(f"Preferences update error: {str(e)}")
        
    except Exception as e:
        st.error("Error loading profile")
        print(f"Profile error: {str(e)}")

if __name__ == "__main__":
    profile_page()
