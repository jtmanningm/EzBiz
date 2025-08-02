#!/usr/bin/env python3
"""
Seed script to add initial services to the database.
Run this to populate the SERVICES table with basic carpet cleaning services.
"""

from database.connection import snowflake_conn

def seed_services():
    """Add initial services to the database"""
    
    services = [
        {
            'name': 'Standard Carpet Cleaning',
            'category': 'Carpet Cleaning',
            'description': 'Professional carpet cleaning for residential and commercial spaces',
            'cost': 120.00,
            'duration': 90,
            'customer_bookable': True
        },
        {
            'name': 'Deep Carpet Cleaning',
            'category': 'Deep Cleaning',
            'description': 'Intensive deep cleaning for heavily soiled carpets',
            'cost': 180.00,
            'duration': 120,
            'customer_bookable': True
        },
        {
            'name': 'Pet Odor Treatment',
            'category': 'Pet Odor Treatment',
            'description': 'Specialized treatment to eliminate pet odors and stains',
            'cost': 95.00,
            'duration': 60,
            'customer_bookable': True
        },
        {
            'name': 'Upholstery Cleaning',
            'category': 'Upholstery Cleaning',
            'description': 'Professional cleaning for furniture and upholstery',
            'cost': 85.00,
            'duration': 75,
            'customer_bookable': True
        },
        {
            'name': 'Area Rug Cleaning',
            'category': 'Area Rug Cleaning',
            'description': 'Specialized cleaning for area rugs and delicate fabrics',
            'cost': 65.00,
            'duration': 45,
            'customer_bookable': True
        },
        {
            'name': 'Tile & Grout Cleaning',
            'category': 'Tile & Grout Cleaning',
            'description': 'Deep cleaning for tile floors and grout lines',
            'cost': 150.00,
            'duration': 90,
            'customer_bookable': True
        },
        {
            'name': 'Commercial Carpet Cleaning',
            'category': 'Carpet Cleaning',
            'description': 'Large-scale carpet cleaning for commercial properties',
            'cost': 250.00,
            'duration': 180,
            'customer_bookable': False
        },
        {
            'name': 'Emergency Stain Removal',
            'category': 'Stain Removal',
            'description': 'Urgent stain removal service for immediate needs',
            'cost': 75.00,
            'duration': 30,
            'customer_bookable': False
        }
    ]
    
    # First, check if services already exist
    check_query = "SELECT COUNT(*) as count FROM OPERATIONAL.CARPET.SERVICES"
    result = snowflake_conn.execute_query(check_query)
    
    if result and result[0]['COUNT'] > 0:
        print(f"Services table already has {result[0]['COUNT']} services")
        return
    
    # Insert services
    insert_query = """
    INSERT INTO OPERATIONAL.CARPET.SERVICES (
        SERVICE_NAME,
        SERVICE_CATEGORY,
        SERVICE_DESCRIPTION,
        COST,
        ACTIVE_STATUS,
        SERVICE_DURATION,
        CUSTOMER_BOOKABLE
    ) VALUES (?, ?, ?, ?, TRUE, ?, ?)
    """
    
    inserted_count = 0
    for service in services:
        try:
            snowflake_conn.execute_query(insert_query, [
                service['name'],
                service['category'],
                service['description'],
                service['cost'],
                service['duration'],
                service['customer_bookable']
            ])
            inserted_count += 1
            print(f"✅ Added: {service['name']}")
        except Exception as e:
            print(f"❌ Failed to add {service['name']}: {e}")
    
    print(f"\n🎉 Successfully added {inserted_count} services to the database!")

if __name__ == "__main__":
    print("🌱 Seeding services database...")
    seed_services()