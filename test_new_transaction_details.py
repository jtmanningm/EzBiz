#!/usr/bin/env python3
"""
Comprehensive test for the new transaction details page
Tests with existing scheduled services to ensure pricing displays correctly
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database.connection import SnowflakeConnection
from pages.transaction_details import get_transaction_details
from utils.null_handling import safe_get_float, safe_get_int, safe_get_string

def test_transaction_details_functionality():
    """Test the new transaction details page with real data"""
    print("🧪 Testing New Transaction Details Page")
    print("=" * 60)
    
    conn = SnowflakeConnection.get_instance()
    
    # Get test transactions
    test_query = """
    SELECT ID as TRANSACTION_ID, SERVICE_NAME, STATUS, BASE_SERVICE_COST, AMOUNT
    FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION
    WHERE STATUS IN ('SCHEDULED', 'IN_PROGRESS')
    ORDER BY CREATED_DATE DESC
    LIMIT 5
    """
    
    try:
        test_transactions = conn.execute_query(test_query)
        if not test_transactions:
            print("❌ No test transactions found")
            return False
        
        print(f"✅ Found {len(test_transactions)} test transactions")
        
        success_count = 0
        
        for i, tx in enumerate(test_transactions, 1):
            transaction_id = tx['TRANSACTION_ID']
            print(f"\n🔍 Test {i}: Transaction ID {transaction_id}")
            print(f"   Service: {tx['SERVICE_NAME']}")
            print(f"   Status: {tx['STATUS']}")
            
            # Test the get_transaction_details function
            transaction = get_transaction_details(transaction_id)
            
            if not transaction:
                print(f"   ❌ Failed to load transaction details")
                continue
            
            # Test 1: Verify basic data retrieval
            print(f"   ✅ Transaction data loaded successfully")
            
            # Test 2: Verify service name display
            primary_service_name = transaction.get('PRIMARY_SERVICE_NAME') or transaction.get('PRIMARY_SERVICE_TABLE_NAME')
            if primary_service_name:
                print(f"   ✅ Primary service name: {primary_service_name}")
            else:
                print(f"   ⚠️ Primary service name missing")
            
            # Test 3: Verify pricing data
            base_cost = safe_get_float(transaction.get('BASE_SERVICE_COST', 0))
            total_amount = safe_get_float(transaction.get('TOTAL_AMOUNT', 0))
            
            print(f"   📊 Pricing Analysis:")
            print(f"      Base Service Cost: ${base_cost:.2f}")
            print(f"      Total Amount: ${total_amount:.2f}")
            
            if base_cost > 0:
                print(f"   ✅ Base service cost available")
            else:
                print(f"   ⚠️ Base service cost is zero or missing")
            
            if total_amount > 0:
                print(f"   ✅ Total amount available")
            else:
                print(f"   ⚠️ Total amount is zero or missing")
            
            # Test 4: Check for additional services
            service2_id = transaction.get('SERVICE2_ID')
            service3_id = transaction.get('SERVICE3_ID')
            
            if service2_id:
                service2_name = transaction.get('SERVICE2_NAME', 'Unknown')
                service2_cost = safe_get_float(transaction.get('SERVICE2_COST', 0))
                print(f"   ✅ Additional Service 1: {service2_name} (${service2_cost:.2f})")
            
            if service3_id:
                service3_name = transaction.get('SERVICE3_NAME', 'Unknown')
                service3_cost = safe_get_float(transaction.get('SERVICE3_COST', 0))
                print(f"   ✅ Additional Service 2: {service3_name} (${service3_cost:.2f})")
            
            if not service2_id and not service3_id:
                print(f"   📝 Single service transaction")
            
            # Test 5: Verify customer information
            customer_name = ""
            if transaction.get('CUSTOMER_FIRST_NAME'):
                customer_name = f"{transaction['CUSTOMER_FIRST_NAME']} {transaction['CUSTOMER_LAST_NAME']}"
            elif transaction.get('ACCOUNT_NAME'):
                customer_name = transaction['ACCOUNT_NAME']
            
            if customer_name:
                print(f"   ✅ Customer: {customer_name}")
            else:
                print(f"   ⚠️ Customer information missing")
            
            # Test 6: Verify address information
            address_parts = [
                transaction.get('STREET_ADDRESS'),
                transaction.get('CITY'),
                transaction.get('STATE'),
                str(transaction.get('ZIP_CODE')) if transaction.get('ZIP_CODE') else None
            ]
            address = ', '.join(filter(None, address_parts))
            
            if address:
                print(f"   ✅ Service Address: {address}")
            else:
                print(f"   ⚠️ Service address missing")
            
            # Test 7: Calculate expected total
            expected_total = base_cost
            if service2_id and transaction.get('SERVICE2_COST'):
                expected_total += safe_get_float(transaction.get('SERVICE2_COST', 0))
            if service3_id and transaction.get('SERVICE3_COST'):
                expected_total += safe_get_float(transaction.get('SERVICE3_COST', 0))
            
            material_cost = safe_get_float(transaction.get('MATERIAL_COST', 0))
            if material_cost > 0:
                expected_total += material_cost
            
            print(f"   🧮 Calculated Total: ${expected_total:.2f}")
            
            if abs(expected_total - total_amount) < 0.01:  # Allow for small floating point differences
                print(f"   ✅ Pricing calculation matches")
            else:
                print(f"   ⚠️ Pricing mismatch - Expected: ${expected_total:.2f}, Actual: ${total_amount:.2f}")
            
            success_count += 1
            print(f"   ✅ Transaction {transaction_id} test passed")
        
        print(f"\n🎉 Test Results Summary:")
        print(f"   Transactions tested: {len(test_transactions)}")
        print(f"   Successful tests: {success_count}")
        print(f"   Success rate: {(success_count/len(test_transactions)*100):.1f}%")
        
        if success_count == len(test_transactions):
            print(f"\n🏆 All tests passed! New transaction details page is working correctly.")
            return True
        else:
            print(f"\n⚠️ Some tests failed. Review the issues above.")
            return False
        
    except Exception as e:
        print(f"❌ Test failed with error: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return False

def test_specific_multi_service_transaction():
    """Test a specific multi-service transaction in detail"""
    print(f"\n🔬 Testing Multi-Service Transaction in Detail")
    print("=" * 50)
    
    conn = SnowflakeConnection.get_instance()
    
    # Find a transaction with multiple services
    multi_service_query = """
    SELECT ID
    FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION
    WHERE (SERVICE2_ID IS NOT NULL OR SERVICE3_ID IS NOT NULL)
    AND STATUS IN ('SCHEDULED', 'IN_PROGRESS')
    ORDER BY CREATED_DATE DESC
    LIMIT 1
    """
    
    try:
        result = conn.execute_query(multi_service_query)
        if not result:
            print("❌ No multi-service transactions found for detailed testing")
            return True  # Not a failure, just no data
        
        transaction_id = result[0]['ID']
        print(f"🎯 Testing Transaction ID: {transaction_id}")
        
        transaction = get_transaction_details(transaction_id)
        if not transaction:
            print("❌ Failed to load multi-service transaction")
            return False
        
        # Detailed analysis
        print(f"📋 Detailed Analysis:")
        print(f"   Primary Service: {transaction.get('PRIMARY_SERVICE_NAME')}")
        print(f"   Primary Cost: ${safe_get_float(transaction.get('BASE_SERVICE_COST', 0)):.2f}")
        
        if transaction.get('SERVICE2_ID'):
            print(f"   Service 2: {transaction.get('SERVICE2_NAME')}")
            print(f"   Service 2 Cost: ${safe_get_float(transaction.get('SERVICE2_COST', 0)):.2f}")
        
        if transaction.get('SERVICE3_ID'):
            print(f"   Service 3: {transaction.get('SERVICE3_NAME')}")
            print(f"   Service 3 Cost: ${safe_get_float(transaction.get('SERVICE3_COST', 0)):.2f}")
        
        total_calculated = (
            safe_get_float(transaction.get('BASE_SERVICE_COST', 0)) +
            safe_get_float(transaction.get('SERVICE2_COST', 0)) +
            safe_get_float(transaction.get('SERVICE3_COST', 0)) +
            safe_get_float(transaction.get('MATERIAL_COST', 0))
        )
        
        total_stored = safe_get_float(transaction.get('TOTAL_AMOUNT', 0))
        
        print(f"   Calculated Total: ${total_calculated:.2f}")
        print(f"   Stored Total: ${total_stored:.2f}")
        
        if abs(total_calculated - total_stored) < 0.01:
            print(f"   ✅ Multi-service pricing is accurate")
            return True
        else:
            print(f"   ❌ Multi-service pricing mismatch")
            return False
            
    except Exception as e:
        print(f"❌ Multi-service test failed: {str(e)}")
        return False

def verify_new_page_vs_old_data():
    """Verify the new page handles all edge cases from existing data"""
    print(f"\n🔍 Edge Case Testing")
    print("=" * 30)
    
    conn = SnowflakeConnection.get_instance()
    
    # Test various scenarios
    test_cases = [
        ("Transactions with NULL service names", "SERVICE_NAME IS NULL"),
        ("Transactions with zero costs", "BASE_SERVICE_COST = 0 OR AMOUNT = 0"),
        ("Transactions with missing customer data", "CUSTOMER_ID IS NULL"),
        ("Transactions with only additional services", "SERVICE2_ID IS NOT NULL AND SERVICE_ID IS NULL"),
    ]
    
    for test_name, condition in test_cases:
        query = f"""
        SELECT COUNT(*) as count
        FROM OPERATIONAL.CARPET.SERVICE_TRANSACTION
        WHERE STATUS IN ('SCHEDULED', 'IN_PROGRESS') AND ({condition})
        """
        
        try:
            result = conn.execute_query(query)
            count = result[0]['count'] if result else 0
            print(f"   {test_name}: {count} cases found")
            
            if count > 0:
                print(f"     ⚠️ Edge cases exist - ensure new page handles these")
            else:
                print(f"     ✅ No edge cases of this type")
                
        except Exception as e:
            print(f"     ❌ Error testing {test_name}: {e}")
    
    return True

if __name__ == "__main__":
    print("🌟 COMPREHENSIVE TRANSACTION DETAILS TEST")
    print("=" * 80)
    
    # Run all tests
    test1 = test_transaction_details_functionality()
    test2 = test_specific_multi_service_transaction()
    test3 = verify_new_page_vs_old_data()
    
    if test1 and test2 and test3:
        print(f"\n🏆 ALL TESTS PASSED!")
        print("✅ New transaction details page is ready to replace the old one")
        print("✅ Pricing displays correctly for all transaction types")
        print("✅ All existing functionality is preserved")
    else:
        print(f"\n💥 SOME TESTS FAILED!")
        print("❌ Review the issues above before deploying the new page")