# Deployment Guide for Ez Biz

This guide explains how to deploy the Ez Biz application to Streamlit Cloud.

## Prerequisites

1. You already have a GitHub repository at: github.com/jtmanningm/Ez-Biz.git
2. A Streamlit Cloud account (sign up at https://streamlit.io/cloud)

## Pre-Deployment Tasks Completed

- ✅ Moved all credentials to `.streamlit/secrets.toml`
- ✅ Added `.streamlit/secrets.toml` to `.gitignore`
- ✅ Created requirements.txt with all dependencies in root folder
- ✅ Added packages.txt for system dependencies
- ✅ Modified code to handle private key in cloud environment

## Remaining Deployment Steps

### 1. Update and Push Your Code to GitHub

```bash
# Fix authentication for GitHub if needed
git remote set-url origin https://github.com/jtmanningm/Ez-Biz.git

# Add all your changes and untracked files
git add .

# Commit changes
git commit -m "Prepare for deployment"

# Push to GitHub (you'll be prompted for your username and password/token)
git push -u origin main
```

### 2. Deploy to Streamlit Cloud

1. Log in to [Streamlit Cloud](https://streamlit.io/cloud)
2. Click "New app" button
3. Select your repository: Ez-Biz
4. Configure the app:
   - Main file path: main.py
   - Python version: 3.9 (recommended)
   - Advanced settings:
     - Package dependencies: requirements.txt (use the root one, not in requirements/ folder)

### 3. Configure Secrets in Streamlit Cloud

After deploying, add your secrets to Streamlit Cloud with these adjustments:

1. Go to your app in Streamlit Cloud
2. Navigate to "Settings" > "Secrets"
3. Copy the modified contents below (with private key directly in secrets):

```toml
# Snowflake credentials
[snowflake]
account = "uvfnphy-okb79182"
user = "JTMANNINGM"
role = "ACCOUNTADMIN" 
warehouse = "COMPUTE_WH"
database = "OPERATIONAL"
schema = "CARPET"
# Instead of file path, add the actual private key content here
private_key = """-----BEGIN PRIVATE KEY-----
YOUR_ACTUAL_KEY_CONTENT_HERE
-----END PRIVATE KEY-----"""
private_key_passphrase = "Lizard24"

# Mailgun credentials
[mailgun]
api_key = "YOUR_ACTUAL_API_KEY_HERE"
domain = "joinezbiz.com"
test_domain = "sandbox16a2d63b058f4bbc914143c47438384a.mailgun.org"

# Twilio SMS credentials
[twilio]
account_sid = "YOUR_TWILIO_ACCOUNT_SID"
auth_token = "YOUR_TWILIO_AUTH_TOKEN"
from_phone = "YOUR_TWILIO_PHONE_NUMBER"
```

4. Save the secrets

## Important Notes

- For the private key, you need to copy the actual content of your p8 file into the secrets
- To view your key content, run: `cat ~/Documents/Key/rsa_key.p8`
- Make sure to add your actual Mailgun API key

## Debugging the Deployed App

- Check the logs in your Streamlit Cloud dashboard under the app's "Manage app" section
- If you see errors related to Snowflake, check that your private key is correctly formatted
- Remove debugging code (debugpy is already commented out)

## Security Reminders

- Never commit secrets to your repository
- You've already adjusted the code to use Streamlit secrets
- Delete any hardcoded keys after deployment is successful