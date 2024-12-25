# models/pricing.py
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
import json
from database.connection import snowflake_conn

@dataclass
class PricingStrategy:
    strategy_id: int
    name: str
    type: str
    rules: Dict[str, Any]

def get_active_pricing_strategy() -> Optional[PricingStrategy]:
    """Get the currently active pricing strategy from settings"""
    query = """
    SELECT 
        STRATEGY_ID,
        STRATEGY_NAME,
        STRATEGY_TYPE,
        RULES_JSON
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
                strategy_id=strategy['STRATEGY_ID'],
                name=strategy['STRATEGY_NAME'],
                type=strategy['STRATEGY_TYPE'],
                rules=json.loads(strategy['RULES_JSON']) if strategy['RULES_JSON'] else {}
            )
        return None
    except Exception as e:
        print(f"Error fetching pricing strategy: {str(e)}")
        return None

def calculate_final_price(
    base_cost: float,
    strategy: Optional[PricingStrategy],
    labor_details: Optional[List[Dict[str, Any]]] = None,
    material_cost: float = 0.0,
    additional_charges: Optional[Dict[str, float]] = None,
    discounts: Optional[Dict[str, float]] = None
) -> tuple[float, Dict[str, Any]]:
    """Calculate final price based on strategy and adjustments"""
    price_details = {"base_cost": base_cost}
    current_price = base_cost
    
    if strategy:
        # Apply strategy-specific calculations
        if strategy.type == "Fixed Price":
            current_price = base_cost
            
        elif strategy.type == "Cost Plus":
            # Calculate total cost
            total_cost = base_cost
            
            # Add labor cost if included in rules
            if strategy.rules.get('include_labor', True) and labor_details:
                total_labor_cost = sum(
                    detail['hours'] * detail['rate']
                    for detail in labor_details
                )
                total_cost += total_labor_cost
                price_details['labor_cost'] = total_labor_cost
            
            # Add material cost if included in rules
            if strategy.rules.get('include_materials', True) and material_cost > 0:
                total_cost += material_cost
                price_details['material_cost'] = material_cost
            
            # Apply markup
            markup_type = strategy.rules.get('markup_type', 'Percentage')
            markup_value = float(strategy.rules.get('markup_value', 20))
            
            if markup_type == "Percentage":
                markup_amount = total_cost * (markup_value / 100)
            else:  # Fixed Amount
                markup_amount = markup_value
                
            current_price = total_cost + markup_amount
            price_details['markup_amount'] = markup_amount
            
        elif strategy.type == "Variable":
            # Apply base adjustment
            base_adjustment = float(strategy.rules.get('base_adjustment', 0))
            adjusted_cost = base_cost * (1 + (base_adjustment / 100))
            current_price = adjusted_cost
            price_details['base_adjustment'] = adjusted_cost - base_cost
    
    # Add additional charges
    if additional_charges:
        total_charges = sum(additional_charges.values())
        price_details['additional_charges'] = additional_charges
        current_price += total_charges
    
    # Apply discounts
    if discounts:
        total_discounts = sum(discounts.values())
        price_details['discounts'] = discounts
        current_price -= total_discounts
    
    price_details['final_price'] = current_price
    return current_price, price_details

def save_pricing_strategy(strategy_data: Dict[str, Any]) -> bool:
    """Save or update pricing strategy"""
    try:
        if strategy_data.get('strategy_id'):
            query = """
            UPDATE OPERATIONAL.CARPET.PRICING_STRATEGIES
            SET 
                STRATEGY_NAME = ?,
                STRATEGY_TYPE = ?,
                RULES_JSON = ?,
                ACTIVE_FLAG = ?,
                MODIFIED_AT = CURRENT_TIMESTAMP()
            WHERE STRATEGY_ID = ?
            """
            params = [
                strategy_data['name'],
                strategy_data['type'],
                json.dumps(strategy_data['rules']),
                strategy_data.get('active', True),
                strategy_data['strategy_id']
            ]
        else:
            query = """
            INSERT INTO OPERATIONAL.CARPET.PRICING_STRATEGIES (
                STRATEGY_NAME,
                STRATEGY_TYPE,
                RULES_JSON,
                ACTIVE_FLAG
            ) VALUES (?, ?, ?, ?)
            """
            params = [
                strategy_data['name'],
                strategy_data['type'],
                json.dumps(strategy_data['rules']),
                strategy_data.get('active', True)
            ]
            
        snowflake_conn.execute_query(query, params)
        return True
    except Exception as e:
        print(f"Error saving pricing strategy: {str(e)}")
        return False