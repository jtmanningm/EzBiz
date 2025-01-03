# models/pricing.py
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import streamlit as st
import json
from database.connection import snowflake_conn

@dataclass
class PricingStrategy:
    strategy_id: int
    name: str
    type: str
    rules: Dict[str, Any]
    active: bool
    created_at: datetime
    modified_at: datetime

def get_active_pricing_strategy() -> Optional[PricingStrategy]:
    """Get the currently active pricing strategy from settings"""
    query = """
    SELECT 
        STRATEGY_ID,
        STRATEGY_NAME,
        STRATEGY_TYPE,
        RULES_JSON,
        ACTIVE_FLAG,
        CREATED_AT,
        MODIFIED_AT
    FROM OPERATIONAL.CARPET.PRICING_STRATEGIES
    WHERE ACTIVE_FLAG = TRUE
    ORDER BY MODIFIED_AT DESC
    LIMIT 1
    """
    
    try:
        result = snowflake_conn.execute_query(query)
        if result:
            strategy = result[0]
            return PricingStrategy(
                strategy_id=int(strategy['STRATEGY_ID']),
                name=strategy['STRATEGY_NAME'],
                type=strategy['STRATEGY_TYPE'],
                rules=json.loads(strategy['RULES_JSON']) if strategy['RULES_JSON'] else {},
                active=bool(strategy['ACTIVE_FLAG']),
                created_at=strategy['CREATED_AT'],
                modified_at=strategy['MODIFIED_AT']
            )
        return None
    except Exception as e:
        print(f"Error fetching pricing strategy: {str(e)}")
        return None

def calculate_final_price(
    base_cost: float,
    strategy: Optional[PricingStrategy],
    labor_details: List[Dict[str, Any]],
    material_cost: float,
    price_adjustment: float = 0.0
) -> Tuple[float, Dict[str, Any]]:
    """
    Calculate final price based on strategy and adjustments.
    
    Args:
        base_cost: Base cost of the service(s)
        strategy: Active pricing strategy
        labor_details: List of dictionaries containing employee labor details
        material_cost: Cost of materials used
        price_adjustment: Price adjustment percentage (-100 to 100)
        
    Returns:
        Tuple containing:
        - final_price: The calculated final price
        - price_details: Dictionary containing breakdown of price components
    """
    # Ensure all inputs are floats
    try:
        base_cost = float(base_cost)
        material_cost = float(material_cost)
        price_adjustment = float(price_adjustment)
    except (TypeError, ValueError) as e:
        print(f"Error converting costs to float: {e}")
        return 0.0, {}
    
    price_details = {
        'base_cost': base_cost,
        'labor_cost': 0.0,
        'material_cost': material_cost,
        'adjustment_amount': 0.0,
        'total_before_adjustment': 0.0
    }
    
    if not strategy:
        total = base_cost + material_cost
        price_details['total_before_adjustment'] = total
        return total, price_details
    
    total_cost = base_cost
    
    if strategy.type == "Cost + Labor":
        # Calculate labor cost with explicit float conversion
        try:
            labor_cost = 0.0
            for detail in labor_details:
                hours = float(detail['hours'])
                rate = float(detail['rate'])
                labor_cost += hours * rate
                
            price_details['labor_cost'] = labor_cost
            total_cost += labor_cost
        except (TypeError, ValueError) as e:
            print(f"Error calculating labor cost: {e}")
            price_details['labor_cost'] = 0.0
        
        # Add material cost if included
        if strategy.rules.get('include_materials', True):
            total_cost += material_cost
            
    elif strategy.type == "Fixed Price":
        # Add material cost if included
        if strategy.rules.get('include_materials', True):
            total_cost += material_cost
    
    price_details['total_before_adjustment'] = total_cost
    
    # Apply price adjustment
    if price_adjustment != 0:
        adjustment_amount = total_cost * (price_adjustment / 100)
        price_details['adjustment_amount'] = adjustment_amount
        total_cost += adjustment_amount
    
    # Ensure final values are floats
    for key in price_details:
        if isinstance(price_details[key], (Decimal, float, int)):
            price_details[key] = float(price_details[key])
    
    return float(total_cost), price_details

def save_pricing_strategy(strategy_data: Dict[str, Any]) -> bool:
    """Save or update pricing strategy"""
    try:
        # First deactivate all other strategies
        deactivate_query = """
        UPDATE OPERATIONAL.CARPET.PRICING_STRATEGIES
        SET 
            ACTIVE_FLAG = FALSE,
            MODIFIED_AT = CURRENT_TIMESTAMP()
        """
        snowflake_conn.execute_query(deactivate_query)

        # Then insert new strategy
        query = """
        INSERT INTO OPERATIONAL.CARPET.PRICING_STRATEGIES (
            STRATEGY_NAME,
            STRATEGY_TYPE,
            RULES_JSON,
            ACTIVE_FLAG,
            CREATED_AT,
            MODIFIED_AT
        ) VALUES (?, ?, ?, TRUE, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """
        params = [
            strategy_data.get('name', 'Default Strategy'),
            strategy_data['type'],
            json.dumps(strategy_data['rules'])
        ]
            
        snowflake_conn.execute_query(query, params)
        return True
    except Exception as e:
        print(f"Error saving pricing strategy: {str(e)}")
        return False