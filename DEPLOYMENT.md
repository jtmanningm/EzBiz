# Deployment Guide for Ez Biz

This guide explains how to deploy the Ez Biz application to Streamlit Cloud.

## Prerequisites

1. You already have a GitHub repository at: github.com/jtmanningm/Ez-Biz.git
2. A Streamlit Cloud account (sign up at https://streamlit.io/cloud)

## Pre-Deployment Tasks Completed

- ✅ Moved all credentials to `.streamlit/secrets.toml`
- ✅ Added `.streamlit/secrets.toml` to `.gitignore`
- ✅ Created requirements.txt with all dependencies

## Remaining Deployment Steps

### 1. Update and Push Your Code to GitHub

```bash
# Add all your changes and untracked files
git add .

# Commit changes
git commit -m "Prepare for deployment"

# Push to GitHub
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
     - Package dependencies: requirements/requirements.txt

### 3. Configure Secrets in Streamlit Cloud

After deploying, add your secrets to Streamlit Cloud:

1. Go to your app in Streamlit Cloud
2. Navigate to "Settings" > "Secrets"
3. Copy the contents of your local `.streamlit/secrets.toml` file
4. Paste into the secrets manager in Streamlit Cloud
5. Save the secrets

IMPORTANT: Make sure to update any API keys before pasting into Streamlit Cloud!

## Debugging the Deployed App

- Check the logs in your Streamlit Cloud dashboard under the app's "Manage app" section
- Remove debugging code (like debugpy) before final deployment

## Security Reminders

- Never commit secrets to your repository
- You've already adjusted the code to use Streamlit secrets
- Delete any hardcoded keys after deployment is successful