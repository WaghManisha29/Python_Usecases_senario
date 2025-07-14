import boto3
import configparser

# Load config
config = configparser.ConfigParser()
config.read('config/config.ini')

aws_access_key_id = config['AWS']['aws_access_key_id']
aws_secret_access_key = config['AWS']['aws_secret_access_key']
bucket_name = config['AWS']['bucket_name']
resumes_prefix = config['AWS']['resumes_prefix']
archive_prefix = config['AWS']['archive_prefix']

# Create boto3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

def archive_file(filename):
    """
    Moves file from resumes/ to resumes/archive/ in the same bucket.
    """
    print(f"📦 Archiving: {filename}")  # Debug print

    source_key = f"{resumes_prefix}{filename}"
    dest_key = f"{archive_prefix}{filename}"

    print(f"🔑 Source Key: {source_key}")  # Debug
    print(f"📁 Dest Key:   {dest_key}")    # Debug

    try:
        # Copy the file to archive
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': source_key},
            Key=dest_key
        )
        print(f"✅ Copied {source_key} to {dest_key}")

        # Delete the original file
        s3.delete_object(Bucket=bucket_name, Key=source_key)
        print(f"🗑️ Deleted original: {source_key}")

    except Exception as e:
        print(f"❌ Error archiving {filename}: {e}")

# Test run
if __name__ == "__main__":
    # Try either of these (exact spelling and spaces required)
    archive_file("John resume.pdf")
    # archive_file("resume.pdf")
