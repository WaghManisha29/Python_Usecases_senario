import boto3
import os
import uuid
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Load env vars
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
S3_REGION = os.getenv("S3_REGION", "ap-south-1")

# S3 client (no ACL used)
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=S3_REGION
)

def upload_attachments(attachments, folder="current"):
    uploaded_urls = []

    for attachment in attachments:
        filename = attachment["filename"]
        content = attachment["content"]

        # Generate unique file name
        unique_id = str(uuid.uuid4()).replace('-', '')[:10]
        extension = os.path.splitext(filename)[1]
        today = datetime.now()
        s3_key = f"{folder}/{today.strftime('%Y/%m/%d')}/{unique_id}{extension}"

        try:
            # Upload file (no ACL since bucket-level public access is enabled)
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=content,
                ContentType="application/octet-stream"
            )

            # Construct public URL
            public_url = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com/{s3_key}"
            uploaded_urls.append(public_url)

        except Exception as e:
            print(f"❌ Upload failed: {e}")
            uploaded_urls.append(None)

    return uploaded_urls
