-- Database fixes applied to resolve missing tables and columns

-- Fix 1: Add missing PORTAL_USER_ID column to RATE_LIMIT_LOG
-- Applied: 2025-01-04
-- Issue: SQL compilation error in rate limiting functionality
ALTER TABLE OPERATIONAL.CARPET.RATE_LIMIT_LOG 
ADD COLUMN IF NOT EXISTS PORTAL_USER_ID NUMBER;

-- Fix 2: Create missing SERVICE_ASSIGNMENTS table
-- Applied: 2025-01-04  
-- Issue: Employee assignments functionality failing
CREATE TABLE IF NOT EXISTS OPERATIONAL.CARPET.SERVICE_ASSIGNMENTS (
    ASSIGNMENT_ID NUMBER IDENTITY(1,1) PRIMARY KEY,
    TRANSACTION_ID NUMBER NOT NULL,
    EMPLOYEE_ID NUMBER NOT NULL,
    ASSIGNMENT_DATE TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    ASSIGNMENT_STATUS VARCHAR(20) DEFAULT 'ASSIGNED',
    NOTES VARCHAR(500),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    MODIFIED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- These fixes resolve:
-- 1. "invalid identifier 'PORTAL_USER_ID'" errors in password reset and rate limiting
-- 2. "Object 'SERVICE_ASSIGNMENTS' does not exist" errors in employee assignments