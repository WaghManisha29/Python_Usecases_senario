import os
from dotenv import load_dotenv
from modules.email_extractor import extract_emails, get_gmail_service
from modules.s3_uploader import upload_attachments
from modules.db_writer import insert_email_data
from google.oauth2.credentials import Credentials

load_dotenv()

print("▶ Starting email processing...")

try:
    creds = Credentials.from_authorized_user_file('gmail_auth/token.json', ['https://www.googleapis.com/auth/gmail.readonly'])
    service = get_gmail_service(creds)
    emails = extract_emails(service)

    for email in emails:
        print(f"⏳ Processing email: {email.get('Subject', '')}")

        attachments = email.get("Attachments", [])
        uploaded_urls = upload_attachments(attachments)

        for i, url in enumerate(uploaded_urls):
            email[f"attachment_{i + 1}_url"] = url

        insert_email_data(email)

except Exception as e:
    print(f"❌ Error occurred: {e}")
