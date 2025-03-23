import requests
import json
import boto3
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

ALPHA_VANTAGE_API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
S3_RAW_FOLDER = "raw/"
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def fetch_stock_data(symbol: str) -> dict:
    """Fetch stock data from Alpha Vantage API."""
    api_key = ALPHA_VANTAGE_API_KEY
    url = f"{ALPHA_VANTAGE_BASE_URL}?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTP error for bad responses
        data = response.json()
        logging.info(f"Fetched {symbol} data from Alpha Vantage")
        if "Time Series (Daily)" not in data:
            logging.error(f"No time series data found for symbol {symbol}")
            return {}
        return data
    except Exception as e:
        logging.error(f"Error fetching data for symbol {symbol}: {e}")
        return {}


def save_data_locally(data: dict, symbol: str) -> str:
    """Save the fetched data to a local file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"data/{symbol}_raw_{timestamp}.json"
    try:
        with open(filename, "w") as f:
            json.dump(data, f)
        logging.info(f"Saved data locally to {filename}")
        return filename
    except Exception as e:
        logging.error(f"Error saving data locally: {e}")
        return ""


def upload_to_s3(filename: str, bucket_name: str, s3_folder: str) -> None:
    """Uploads the local file to S3."""
    s3 = boto3.client("s3")
    s3_key = f"{s3_folder}{filename}"
    try:
        s3.upload_file(filename, bucket_name, s3_key)
        logging.info(f"Uploaded {filename} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        logging.error(f"Error uploading {filename} to S3: {e}")


def main(symbols: list[str]):
    # Ensure data directory exists
    if not os.path.exists("data"):
        os.makedirs("data")
        logging.info("Created data directory")

    for symbol in symbols:
        data = fetch_stock_data(symbol)
        if data:
            local_filename = save_data_locally(data, symbol)
            if local_filename:
                upload_to_s3(local_filename, S3_BUCKET_NAME, S3_RAW_FOLDER)
                os.remove(local_filename)


if __name__ == "__main__":
    main(["AAPL", "GOOGL", "MSFT", "TSLA"])
