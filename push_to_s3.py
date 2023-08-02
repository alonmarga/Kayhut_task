import os
import pandas as pd
import boto3
import logging
from dotenv import load_dotenv
from io import BytesIO

load_dotenv()

logging.basicConfig(filename='data_upload_log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

aws_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

if aws_access_key_id is None or aws_secret_access_key is None:
    logging.error("AWS credentials are missing.")
    raise ValueError("AWS credentials are missing.")

endpoint_url = "https://finance-stock-interview.s3.ca-central-1.amazonaws.com"

feature_engineered_file = 'feature_engineered_data.csv'

s3 = boto3.client('s3', endpoint_url=endpoint_url, aws_access_key_id=aws_access_key_id,
                  aws_secret_access_key=aws_secret_access_key)


def stream_upload_to_s3_parquet(data, date):
    partitioned_file_name = f"{date.strftime('%Y-%m-%d')}/stock_data.parquet"

    # Create an in-memory binary stream
    temp_buffer = BytesIO()
    data.to_parquet(temp_buffer, engine="pyarrow")
    temp_buffer.seek(0)

    s3.upload_fileobj(temp_buffer, "finance-stock-interview", partitioned_file_name)
    logging.info(f"Uploaded {partitioned_file_name} to S3")

# Parquet files offer better terms of storage efficiency, compression, and query performance, especially when dealing with large datasets.
# Parquet is a columnar storage format that is optimized for analytics workloads.

logging.info("Data upload to S3 in Parquet format started.")


try:
    with open(feature_engineered_file, 'rb') as file:
        df_reader = pd.read_csv(file, chunksize=10000)
        for chunk in df_reader:
            chunk['date'] = pd.to_datetime(chunk['date'])
            grouped = chunk.groupby(chunk['date'].dt.date)
            for date, group in grouped:
                stream_upload_to_s3_parquet(group, date)

    logging.info("Data upload to S3 in Parquet format completed.")
except Exception as ex:
    print(f"An error occurred: {ex}")
    logging.error(f"An error occurred: {ex}")
