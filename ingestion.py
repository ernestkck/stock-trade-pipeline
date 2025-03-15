import requests
import json
import boto3
from datetime import datetime

# Alpha Vantage API details
API_KEY = "YOUR_API_KEY"  # Replace with your Alpha Vantage key
SYMBOL = "AAPL"  # Stock symbol (Apple as an example)
URL = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={SYMBOL}&apikey={API_KEY}"

# AWS S3 details
BUCKET_NAME = "stock-trade-pipeline-bucket"  # Unique name for your bucket
S3_FOLDER = "raw/"  # Folder in S3 to organize raw data

def fetch_stock_data():
    # Fetch data from Alpha Vantage
    try:
        response = requests.get(URL)
        response.raise_for_status()  # Check for HTTP errors
        data = response.json()
        
        # Check if API returned valid data
        if "Time Series (Daily)" not in data:
            raise ValueError("Invalid response from Alpha Vantage. Check API key or rate limits.")
        
        # Save to local file with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{SYMBOL}_raw_{timestamp}.json"
        with open(filename, "w") as f:
            json.dump(data, f)
        print(f"Saved data locally to {filename}")
        return filename
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def upload_to_s3(filename):
    # Initialize S3 client
    s3 = boto3.client("s3")
    
    # Create bucket if it doesn’t exist (run once manually if needed)
    try:
        s3.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "ap-southeast-2"}  # Sydney region
        )
        print(f"Created S3 bucket: {BUCKET_NAME}")
    except s3.exceptions.BucketAlreadyExists:
        print(f"Bucket {BUCKET_NAME} already exists")
    except Exception as e:
        print(f"Error creating bucket: {e}")
        return
    
    # Upload file to S3
    try:
        s3_key = f"{S3_FOLDER}{filename}"
        s3.upload_file(filename, BUCKET_NAME, s3_key)
        print(f"Uploaded {filename} to s3://{BUCKET_NAME}/{s3_key}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def main():
    # Fetch and upload
    filename = fetch_stock_data()
    if filename:
        upload_to_s3(filename)

if __name__ == "__main__":
    main()