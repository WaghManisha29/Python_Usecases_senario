import base64
from email import message_from_bytes
from datetime import datetime
from googleapiclient.discovery import build
from email.utils import parsedate_to_datetime

def get_gmail_service(creds):
    return build('gmail', 'v1', credentials=creds)

def extract_emails(service, max_results=10):
    results = service.users().messages().list(userId='me', maxResults=max_results).execute()
    messages = results.get('messages', [])
    emails = []

    for msg in messages:
        msg_data = service.users().messages().get(userId='me', id=msg['id'], format='raw').execute()
        raw_data = base64.urlsafe_b64decode(msg_data['raw'].encode('UTF-8'))
        email_msg = message_from_bytes(raw_data)

        sender = email_msg.get('From', '')
        receiver = email_msg.get('To', '')
        cc = email_msg.get('Cc', '')
        subject = email_msg.get('Subject', '')
        date = email_msg.get('Date', '')
        body = ""
        attachments = []

        # Extract body and attachments
        if email_msg.is_multipart():
            for part in email_msg.walk():
                content_type = part.get_content_type()
                filename = part.get_filename()

                if filename:  # Attachment
                    content = part.get_payload(decode=True)
                    attachments.append({
                        "filename": filename,
                        "content": content
                    })
                elif content_type == 'text/plain' and not filename:
                    charset = part.get_content_charset() or 'utf-8'
                    try:
                        body += part.get_payload(decode=True).decode(charset, errors="ignore")
                    except Exception as e:
                        body += "[Error decoding body]"
        else:
            charset = email_msg.get_content_charset() or 'utf-8'
            try:
                body = email_msg.get_payload(decode=True).decode(charset, errors="ignore")
            except:
                body = "[Error decoding body]"

        # Parse the received date
        try:
            received_date = parsedate_to_datetime(date)
        except:
            received_date = datetime.utcnow()

        emails.append({
            "Sender": sender,
            "Receiver": receiver,
            "CC": cc,
            "Subject": subject,
            "Body": body,
            "ReceivedDate": received_date,
            "Attachments": attachments
        })

    return emails
