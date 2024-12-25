# pricing_settings.py
import streamlit as st
from database.connection import snowflake_conn
import json
from models.pricing import save_pricing_strategy

def pricing_settings_page():
    """Simple pricing strategy configuration"""
    st.title("Pricing Settings")

    # Simple strategy selection
    strategy_type = st.radio(
        "Select Pricing Strategy",
        ["Fixed Price", "Cost + Labor"],
        help="Fixed Price: Base price with optional materials\nCost + Labor: Base price plus labor hours and materials"
    )

    with st.form("pricing_strategy_form"):
        # Common settings for both strategies
        include_materials = st.checkbox(
            "Include Material Costs",
            value=True,
            help="Allow adding material costs to service price"
        )

        rules = {
            'include_materials': include_materials
        }

        name = f"{strategy_type} Strategy"
        strategy_data = {
            'name': name,
            'type': strategy_type,
            'rules': rules
        }

        if st.form_submit_button("Save Strategy"):
            if save_pricing_strategy(strategy_data):
                st.success("Pricing strategy updated successfully!")
                st.rerun()
            else:
                st.error("Failed to update pricing strategy")

    # Display help text
    st.markdown("### Strategy Details")
    st.markdown("""
    **All pricing strategies include:**
    - Base service cost
    - Optional material costs
    - Price adjustments during transactions
    
    **Cost + Labor additionally includes:**
    - Labor hours tracking
    - Employee rate calculations
    """)

if __name__ == "__main__":
    pricing_settings_page()