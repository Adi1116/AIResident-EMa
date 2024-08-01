import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime

# Set up user credentials
USERS = {
    "user1@example.com": "password1",
    "user2@example.com": "password2",
}

# Define login function
def login(email, password):
    if email in USERS and USERS[email] == password:
        return True
    return False

# Log user data
def log_user_data(email):
    login_data = {
        "email": email,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        log_df = pd.read_csv('user_log.csv')
    except FileNotFoundError:
        log_df = pd.DataFrame(columns=["email", "timestamp"])
    log_df = log_df.append(login_data, ignore_index=True)
    log_df.to_csv('user_log.csv', index=False)

# Check if the user is logged in
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

# Login page
if not st.session_state.logged_in:
    st.title("Login")
    email = st.text_input("Email")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        if login(email, password):
            st.session_state.logged_in = True
            log_user_data(email)
            st.experimental_rerun()
        else:
            st.error("Email or password is incorrect")
else:
    # Main App Code
    st.title("Email Enrichment App")

    # Upload CSV file
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    if uploaded_file:
        df = pd.read_csv(uploaded_file)

        url_column = 'LinkedIn_Profile_Url'
        url = "https://api.apollo.io/api/v1/people/bulk_match"

        headers = {
            'Cache-Control': 'no-cache',
            'Content-Type': 'application/json',
            'X-Api-Key': 'Qu-V9RSUsbmUHYaFLkflYg'
        }

        def make_api_request(details_batch):
            data = {
                "reveal_personal_emails": True,
                "details": details_batch
            }
            response = requests.request("POST", url, headers=headers, json=data)
            return response.json()

        def enrich_emails(start_index, batch_size=10):
            end_index = min(start_index + batch_size, len(df))
            batch = df[url_column][start_index:end_index]
            details = [{"linkedin_url": url} for url in batch]
            response_data = make_api_request(details)

            emails_data = []
            if response_data.get('status') != 'success':
                st.error("API call failed: " + response_data.get('error_message'))
            else:
                for match in response_data.get('matches', []):
                    if match:
                        email = match.get('email')
                        personal_emails = match.get('personal_emails', [])
                        emails_data._append({
                            'Email': email,
                            'Personal Emails': personal_emails
                        })
                    else:
                        emails_data._append({
                            'Email': None,
                            'Personal Emails': None
                        })

            emails_df = pd.DataFrame(emails_data, index=batch.index)
            df.loc[start_index:end_index-1, 'Email'] = emails_df['Email']
            df.loc[start_index:end_index-1, 'Personal Emails'] = emails_df['Personal Emails']

        def process_batches(batch_size=10, minute_limit=200, hour_limit=400):
            start_index = 0
            minute_count = 0
            hour_count = 0

            while start_index < len(df):
                enrich_emails(start_index, batch_size)
                start_index += batch_size
                minute_count += batch_size
                hour_count += batch_size

                # Wait for 1 second after each batch of 10
                time.sleep(1)

                # Check if we reached the minute limit
                if minute_count >= minute_limit:
                    time.sleep(60 - (minute_count // 10))
                    minute_count = 0

                # Check if we reached the hour limit
                if hour_count >= hour_limit:
                    time.sleep(3600 - (hour_count // 10))
                    hour_count = 0

        if st.button("Enrich Emails"):
            with st.spinner("Enriching emails..."):
                process_batches()
                st.success("Email enrichment completed!")
                st.dataframe(df)
                df.to_csv('OUTPUT.csv', index=False)
                st.download_button(
                    label="Download Enriched CSV",
                    data=open('OUTPUT.csv', 'rb').read(),
                    file_name='OUTPUT.csv',
                    mime='text/csv'
                )
