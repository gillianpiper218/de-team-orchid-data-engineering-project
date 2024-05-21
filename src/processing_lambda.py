import os
import pandas as pd
from datetime import datetime
import logging
import json
from pprint import pprint
import boto3


logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")
current_time = datetime.now()

INGESTION_S3_BUCKET_NAME = "de-team-orchid-totesys-ingestion"
PROCESSED_S3_BUCKET_NAME = "de-team-orchid-totesys-processed"


def get_object_key(
    table_name: str, prefix: str = None, bucket=INGESTION_S3_BUCKET_NAME
) -> str:
    # look in the given bucket with the prefix and get the object
    # response = s3.list_objects_v2(**kwargs)
    pass


def remove_created_at_and_last_updated():
    # remove created_at and last_updated keys function
    # Assuming df is your DataFrame
    # df.drop('column_name', axis=1, inplace=True)
    pass


def process_fact_sales_order():
    # split created at date into created_date and created_time keys
    # split last updated into last_updated_date and last_updated_time keys
    remove_created_at_and_last_updated()
    pass


def process_dim_counterparty():
    # remove commercial_contact and delivery_contact keys
    remove_created_at_and_last_updated()
    pass


def process_dim_currency():
    # create currency_name column
    remove_created_at_and_last_updated()
    pass


def process_dim_date():
    # create from each unique date
    # - year
    # - month
    # - day
    # - day_of_week (int type where Monday = 1 and Sunday = 7)
    # - day_name
    # - month_name
    # - quarter
    pass


def process_dim_design():
    remove_created_at_and_last_updated()
    pass


def process_dim_location():
    # change address_id key into location_id
    # Or rename the existing DataFrame (rather than creating a copy)
    # df.rename(columns={'oldName1': 'newName1', 'oldName2': 'newName2'}, inplace=True)
    remove_created_at_and_last_updated()
    pass


def process_dim_staff():
    remove_created_at_and_last_updated()
    pass


def convert_json_to_parquet():
    # convert file format from json to parquet
    # Read JSON into DataFrame
    # json_data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]
    # df = pd.DataFrame(json_data)

    # # Convert DataFrame to Arrow Table
    # table = pa.Table.from_pandas(df)

    # # Write Arrow Table to Parquet file
    # pq.write_table(table, 'output.parquet')
    pass