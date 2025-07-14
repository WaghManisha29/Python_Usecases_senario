import os
import boto3
import configparser

from extract.s3_downloader import download_resume_from_s3
from transform.pdf_to_text import extract_text_from_pdf
from transform.parse_resume import parse_resume_text
from load.load_to_sqlserver import insert_resume_to_db
from archive.s3_archiver import archive_file

# Load config
config = configparser.ConfigParser()
config.read("config/config.ini")

aws_access_key_id = config['AWS']['aws_access_key_id']
aws_secret_access_key = config['AWS']['aws_secret_access_key']
bucket_name = config['AWS']['bucket_name']
resumes_prefix = config['AWS']['resumes_prefix']
archive_prefix = config['AWS']['archive_prefix']

# Ensure downloads/ folder exists
os.makedirs("downloads", exist_ok=True)

# S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

def list_resume_files():
    """
    List all .pdf files under resumes/ but NOT under resumes/archive/
    """
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=resumes_prefix)
    if 'Contents' not in response:
        return []

    files = [
        obj['Key']
        for obj in response['Contents']
        if obj['Key'].endswith('.pdf') and not obj['Key'].startswith(archive_prefix)
    ]
    return files

def process_resume(s3_key):
    """
    Download, parse, load to DB, and archive the resume.
    """
    filename = os.path.basename(s3_key)
    local_path = f"downloads/{filename}"

    print(f"\n📄 Processing: {filename}")

    # 1. Download from S3
    download_resume_from_s3(filename)

    # 2. Extract text
    text = extract_text_from_pdf(local_path)
    if not text:
        print("❌ Skipping: No text extracted")
        return

    # 3. Parse resume
    parsed_data = parse_resume_text(text)

    # 4. Insert into DB
    insert_resume_to_db(parsed_data, filename)

    # 5. Archive on S3
    archive_file(filename)

if __name__ == "__main__":
    resume_keys = list_resume_files()

    if not resume_keys:
        print("📭 No resumes found in S3.")
    else:
        print(f"📂 Found {len(resume_keys)} resume(s):")
        for key in resume_keys:
            print(f" - {key}")

        for key in resume_keys:
            process_resume(key)

        print("\n✅ All resumes processed.")
