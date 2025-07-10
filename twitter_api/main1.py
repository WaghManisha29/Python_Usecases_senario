import json
import logging
import os
from datetime import datetime

from config.auth_config import get_twitter_client
from scripts.collection.tweet_collector import collect_tweets
from scripts.preprocessing.clean_tweets import clean_raw_tweets
from scripts.preprocessing.filter_tweets import filter_cleaned_tweets
from scripts.analysis.sentiment_analysis import analyze_sentiment
from scripts.load.load_to_db import load_to_all_databases

# Logging setup
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/processing_log.txt',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    try:
        logging.info("🚀 Starting Twitter Sentiment Pipeline")

        # Load filter config
        with open("config/filters_config.json") as f:
            config = json.load(f)

        query = " OR ".join(config["keywords"])
        start_time = config["start_time"]
        end_time = config["end_time"]
        language = config["language"]
        max_results = config["max_results"]

        # Step 1: Collect tweets
        logging.info("🔍 Collecting tweets from Twitter API...")
        collect_tweets(query, start_time, end_time, language, max_results)
        logging.info("✅ Tweet collection completed.")

        # Step 2: Clean tweets
        logging.info("🧹 Cleaning raw tweets...")
        clean_raw_tweets()
        logging.info("✅ Tweet cleaning completed.")

        # Step 3: Filter tweets
        logging.info("🔎 Filtering tweets by keyword...")
        filter_cleaned_tweets(config["keywords"])
        logging.info("✅ Tweet filtering completed.")

        # Step 4: Sentiment analysis
        logging.info("🧠 Performing sentiment analysis...")
        analyze_sentiment()
        logging.info("✅ Sentiment analysis completed.")

        # Step 5: Load to databases
        logging.info("🗃️ Loading data into MySQL and SQL Server...")
        load_to_all_databases()
        logging.info("✅ Data loaded into both databases.")

        logging.info("🎉 Pipeline executed successfully!")

    except Exception as e:
        logging.error("❌ Pipeline failed: %s", str(e))
        with open("logs/error_log.txt", "a") as err_log:
            err_log.write(f"{datetime.utcnow()} - ERROR: {str(e)}\n")
        print("Pipeline failed. Check logs/error_log.txt for details.")

if __name__ == "__main__":
    main()
