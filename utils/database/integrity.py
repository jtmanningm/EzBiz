# utils/database/integrity.py
"""
Database integrity validation and constraint enforcement utilities.
Ensures proper ID uniqueness and schema compliance.
"""

import streamlit as st
from database.connection import snowflake_conn
from typing import Dict, List, Optional, Tuple

def validate_id_uniqueness() -> Dict[str, bool]:
    """
    Validate that CUSTOMER_ID and ACCOUNT_ID ranges don't overlap.
    Returns dict with validation results.
    """
    results = {
        'customer_ids_unique': False,
        'account_ids_unique': False,
        'no_id_overlap': False,
        'service_addresses_proper': False
    }
    
    try:
        # Check customer ID uniqueness
        customer_query = """
        SELECT COUNT(*) as total_customers,
               COUNT(DISTINCT CUSTOMER_ID) as unique_customer_ids
        FROM OPERATIONAL.CARPET.CUSTOMER
        """
        customer_result = snowflake_conn.execute_query(customer_query)
        if customer_result:
            total = customer_result[0]['TOTAL_CUSTOMERS']
            unique = customer_result[0]['UNIQUE_CUSTOMER_IDS']
            results['customer_ids_unique'] = (total == unique)
        
        # Check account ID uniqueness
        account_query = """
        SELECT COUNT(*) as total_accounts,
               COUNT(DISTINCT ACCOUNT_ID) as unique_account_ids
        FROM OPERATIONAL.CARPET.ACCOUNTS
        """
        account_result = snowflake_conn.execute_query(account_query)
        if account_result:
            total = account_result[0]['TOTAL_ACCOUNTS']
            unique = account_result[0]['UNIQUE_ACCOUNT_IDS']
            results['account_ids_unique'] = (total == unique)
        
        # Check for ID overlap (should be none with proper IDENTITY columns)
        overlap_query = """
        SELECT COUNT(*) as overlap_count
        FROM (
            SELECT CUSTOMER_ID as ID FROM OPERATIONAL.CARPET.CUSTOMER
            INTERSECT
            SELECT ACCOUNT_ID as ID FROM OPERATIONAL.CARPET.ACCOUNTS
        )
        """
        overlap_result = snowflake_conn.execute_query(overlap_query)
        if overlap_result:
            overlap_count = overlap_result[0]['OVERLAP_COUNT']
            results['no_id_overlap'] = (overlap_count == 0)
        
        # Check SERVICE_ADDRESSES table integrity
        addresses_query = """
        SELECT 
            COUNT(*) as total_addresses,
            COUNT(CASE WHEN CUSTOMER_ID IS NOT NULL AND ACCOUNT_ID IS NULL THEN 1 END) as customer_addresses,
            COUNT(CASE WHEN ACCOUNT_ID IS NOT NULL AND CUSTOMER_ID IS NULL THEN 1 END) as account_addresses,
            COUNT(CASE WHEN CUSTOMER_ID IS NOT NULL AND ACCOUNT_ID IS NOT NULL THEN 1 END) as invalid_both_ids,
            COUNT(CASE WHEN CUSTOMER_ID IS NULL AND ACCOUNT_ID IS NULL THEN 1 END) as invalid_no_ids
        FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES
        """
        addresses_result = snowflake_conn.execute_query(addresses_query)
        if addresses_result:
            row = addresses_result[0]
            # Service addresses should have exactly one ID type populated
            invalid_records = row['INVALID_BOTH_IDS'] + row['INVALID_NO_IDS']
            results['service_addresses_proper'] = (invalid_records == 0)
            
            # Store additional info for reporting
            results['addresses_stats'] = {
                'total': row['TOTAL_ADDRESSES'],
                'customer_addresses': row['CUSTOMER_ADDRESSES'],
                'account_addresses': row['ACCOUNT_ADDRESSES'],
                'invalid_both_ids': row['INVALID_BOTH_IDS'],
                'invalid_no_ids': row['INVALID_NO_IDS']
            }
        
    except Exception as e:
        st.error(f"Error validating database integrity: {str(e)}")
        return results
    
    return results

def get_schema_violations() -> List[Dict[str, str]]:
    """
    Identify specific records that violate schema rules.
    Returns list of violations for manual correction.
    """
    violations = []
    
    try:
        # Find SERVICE_ADDRESSES records with both IDs populated
        both_ids_query = """
        SELECT ADDRESS_ID, CUSTOMER_ID, ACCOUNT_ID, STREET_ADDRESS
        FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES
        WHERE CUSTOMER_ID IS NOT NULL AND ACCOUNT_ID IS NOT NULL
        """
        both_ids_result = snowflake_conn.execute_query(both_ids_query)
        
        for record in both_ids_result or []:
            violations.append({
                'type': 'SERVICE_ADDRESSES_BOTH_IDS',
                'table': 'SERVICE_ADDRESSES',
                'address_id': record['ADDRESS_ID'],
                'customer_id': record['CUSTOMER_ID'],
                'account_id': record['ACCOUNT_ID'],
                'address': record['STREET_ADDRESS'],
                'description': 'Address has both CUSTOMER_ID and ACCOUNT_ID populated'
            })
        
        # Find SERVICE_ADDRESSES records with no IDs populated
        no_ids_query = """
        SELECT ADDRESS_ID, STREET_ADDRESS
        FROM OPERATIONAL.CARPET.SERVICE_ADDRESSES
        WHERE CUSTOMER_ID IS NULL AND ACCOUNT_ID IS NULL
        """
        no_ids_result = snowflake_conn.execute_query(no_ids_query)
        
        for record in no_ids_result or []:
            violations.append({
                'type': 'SERVICE_ADDRESSES_NO_IDS',
                'table': 'SERVICE_ADDRESSES',
                'address_id': record['ADDRESS_ID'],
                'address': record['STREET_ADDRESS'],
                'description': 'Address has neither CUSTOMER_ID nor ACCOUNT_ID populated'
            })
        
        # Find SERVICE_TRANSACTION records with both IDs populated
        transaction_both_query = """
        SELECT ID, CUSTOMER_ID, ACCOUNT_ID, SERVICE_NAME, SERVICE_DATE
        FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION
        WHERE CUSTOMER_ID IS NOT NULL AND ACCOUNT_ID IS NOT NULL
        """
        transaction_both_result = snowflake_conn.execute_query(transaction_both_query)
        
        for record in transaction_both_result or []:
            violations.append({
                'type': 'SERVICE_TRANSACTION_BOTH_IDS',
                'table': 'SERVICE_TRANSACTION',
                'transaction_id': record['ID'],
                'customer_id': record['CUSTOMER_ID'],
                'account_id': record['ACCOUNT_ID'],
                'service_name': record['SERVICE_NAME'],
                'service_date': str(record['SERVICE_DATE']),
                'description': 'Transaction has both CUSTOMER_ID and ACCOUNT_ID populated'
            })
        
    except Exception as e:
        st.error(f"Error finding schema violations: {str(e)}")
    
    return violations

def display_integrity_report():
    """Display database integrity validation report in Streamlit."""
    st.subheader("Database Integrity Report")
    
    # Run validation
    results = validate_id_uniqueness()
    violations = get_schema_violations()
    
    # Display overall status
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### ID Uniqueness")
        st.write("✅ Customer IDs Unique:" if results['customer_ids_unique'] else "❌ Customer IDs Unique:", results['customer_ids_unique'])
        st.write("✅ Account IDs Unique:" if results['account_ids_unique'] else "❌ Account IDs Unique:", results['account_ids_unique'])
        st.write("✅ No ID Overlap:" if results['no_id_overlap'] else "❌ No ID Overlap:", results['no_id_overlap'])
    
    with col2:
        st.markdown("### Schema Compliance")
        st.write("✅ Service Addresses Proper:" if results['service_addresses_proper'] else "❌ Service Addresses Proper:", results['service_addresses_proper'])
        
        if 'addresses_stats' in results:
            stats = results['addresses_stats']
            st.write(f"Total Addresses: {stats['total']}")
            st.write(f"Customer Addresses: {stats['customer_addresses']}")
            st.write(f"Account Addresses: {stats['account_addresses']}")
            if stats['invalid_both_ids'] > 0:
                st.error(f"⚠️ {stats['invalid_both_ids']} addresses with both IDs")
            if stats['invalid_no_ids'] > 0:
                st.error(f"⚠️ {stats['invalid_no_ids']} addresses with no IDs")
    
    # Display violations if any
    if violations:
        st.markdown("### Schema Violations")
        st.error(f"Found {len(violations)} schema violations that need attention:")
        
        for violation in violations:
            with st.expander(f"{violation['type']} - {violation['table']}"):
                for key, value in violation.items():
                    if key not in ['type', 'table']:
                        st.write(f"**{key.replace('_', ' ').title()}:** {value}")
    else:
        st.success("✅ No schema violations found!")

if __name__ == "__main__":
    display_integrity_report()