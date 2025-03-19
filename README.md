# stock-trade-pipeline
A pipeline to ingest, process, and analyse real-time stock trade data


## fetch_stock_data.py
Fetches stock data for a specified symbol from the Alpha Vantage API,
saves the data locally, and uploads it to an S3 bucket.

## databricks_s3_catalog.ipynb
Databricks is used to transform the data. Since serverless warehouse doesn't allow configurations of fs.s3a properties via spark.conf.set and dbutils.fs.mount, Unity Catalog is used to access S3 in Databricks. An external location is added after adding the required IAM role and policies in AWS (see https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/storage-credentials#step-1-create-an-iam-role). Then we can read the JSON files and write write parquet files back to S3.