import boto3
import yfinance as yf
import json

from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple
from queue import Queue
import logging

logger = logging.getLogger(__name__)

class YahooScraper:
    def __init__(
        self, tickers: List[str],
        start_date: datetime,
        end_date: datetime, 
        timedelta: timedelta,
        bucket_name: str = None,
        lambda_name: str = "yahoo_scraper_lambda"
    ):
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.timedelta = timedelta
        self.bucket_name = bucket_name
        self.lambda_name = lambda_name
        self.bucket_tmp_dir = "yahoo_tmp"
        self.s3_client = boto3.client('s3')
        self.lambda_client = boto3.client('lambda', region_name='us-east-2')
        self.batch_size = 500
        self.files_contents_queue = Queue()

    def fetch_data(self, ticker: str) -> List[Dict]:
        """
        Fetch news data from Yahoo Finance for a given ticker.
        """
        stock = yf.Ticker(ticker)
        news_items = stock.news 
        
        results = []
        for item in news_items:
            content = item.get("content", {})

            # Extract the publication date (fallback to displayTime if pubDate is missing)
            date = content.get("pubDate") or content.get("displayTime")
            if date:
                date = datetime.fromisoformat(date).replace(tzinfo=timezone.utc).isoformat()
            else:
                continue  # Skip the entry if no valid date is found

            # Extract the news text (use 'title' as the main text)
            text = content.get("title", "")

            results.append({"date": date, "text": text})

        return results
            
    def scrape_tickers(self):
        """
        Scrape stock data for all tickers.
        """
        results = []
        for ticker in self.tickers:
            results.append(self.fetch_data(ticker))
        return [record for result in results for record in result]

    def save_to_s3(self, data: List[Dict]):
        """
        Save fetched data to S3 as JSON.
        """
        file_name = f"yahoo_finance_{self.start_date.strftime('%Y-%m-%d')}_{self.end_date.strftime('%Y-%m-%d')}.json"
        file_content = json.dumps(data, indent=4, ensure_ascii=False)
        s3_key = f"{self.start_date.strftime('%Y/%m/%d')}/{file_name}"
        self.s3_client.put_object(Bucket=self.bucket_name, Key=s3_key, Body=file_content)
        logger.info(f"Uploaded file to S3: {s3_key}")

    def save_to_json(self, data: List[Dict], file_path: str):
        """
        Save fetched data to a local JSON file for testing.
        """
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)
        logger.info(f"Saved file locally: {file_path}")

    def run(self, local_test: bool = False, test_file_path: str = "yahoo_finance_data.json"):
        logger.info("Scraping stock data started")
        all_data = self.scrape_tickers()
        logger.info(f"Scraping completed. Retrieved {len(all_data)} records.")
        
        if local_test:
            self.save_to_json(all_data, test_file_path)
        else:
            self.save_to_s3(all_data)
        
        logger.info("Data saved successfully")
