import requests
import streamlit as st

def send_simple_message():
    """
    Example function to demonstrate how to use Mailgun API
    Uses streamlit secrets for API key and domain
    """
    # Get values from streamlit secrets
    api_key = st.secrets.get("mailgun", {}).get("api_key", "")
    domain = st.secrets.get("mailgun", {}).get("test_domain", "")
    
    if not api_key or not domain:
        print("Mailgun API key or domain not configured in secrets")
        return None
    
    return requests.post(
        f"https://api.mailgun.net/v3/{domain}/messages",
        auth=("api", api_key),
        data={"from": f"Excited User <mailgun@{domain}>",
            "to": ["recipient@example.com"],
            "subject": "Hello",
            "text": "Testing some Mailgun awesomeness!"}))