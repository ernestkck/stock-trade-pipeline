# stock-trade-pipeline
This project automates the process of fetching stock trade data, transforming it, and storing it for further analysis. The pipeline is designed to be modular and scalable, using cloud services and modern data engineering tools such as AWS S3, Databricks and Airflow.

## Components

- `src/fetch_stock_data.py`: Fetches stock data for specified symbols from the Alpha Vantage API and uploads it to an S3 bucket.
- `databricks_notebooks/transform_stock_data.ipynb`: Transforms the raw stock data using Databricks and PySpark. \
**Notes**:
  - Unity Catalog is used to access S3 in Databricks (Trial) due to serverless warehouse limitations.
  - IAM roles and policies must be configured in AWS to enable access (see [Databricks Unity Catalog Documentation](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-storage/storage-credentials#step-1-create-an-iam-role)).
- `dags/stock_pipeline_dag.py`: Orchestrates the pipeline using Apache Airflow.