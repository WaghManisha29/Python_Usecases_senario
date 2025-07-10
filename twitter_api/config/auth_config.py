

import os
import tweepy
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

def get_twitter_client():
    bearer_token = os.getenv("BEARER_TOKEN")
    if not bearer_token:
        raise ValueError("❌ BEARER_TOKEN not found. Check your .env file.")
    return tweepy.Client(bearer_token=bearer_token)
