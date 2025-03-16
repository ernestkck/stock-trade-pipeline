# stock-trade-pipeline
A pipeline to ingest, process, and analyse real-time stock trade data


## fetch_stock_data.py
Fetches stock data for a specified symbol from the Alpha Vantage API,
saves the data locally, and uploads it to an S3 bucket.