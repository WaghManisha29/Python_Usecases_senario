import boto3
import configparser

# Load config
config = configparser.ConfigParser()
config.read('config/config.ini')

aws_access_key_id = config['AWS']['aws_access_key_id']
aws_secret_access_key = config['AWS']['aws_secret_access_key']
bucket_name = config['AWS']['bucket_name']
resumes_prefix = config['AWS']['resumes_prefix']

# Create S3 client
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)

def download_resume_from_s3(filename, local_path=None):
    """
    Downloads a PDF resume from the S3 resumes folder to the local system.
    
    Args:
        filename (str): The name of the file in the S3 resumes folder.
        local_path (str, optional): The local path to save the file. If None, saves to 'downloads/'.
    """
    if local_path is None:
        local_path = f"downloads/{filename}"

    s3_key = f"{resumes_prefix}{filename}"
    
    try:
        s3.download_file(bucket_name, s3_key, local_path)
        print(f"✅ Downloaded {filename} from S3")
    except Exception as e:
        print(f"❌ Error downloading {filename}: {e}")
