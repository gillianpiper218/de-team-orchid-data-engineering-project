import os
import pandas as pd
from datetime import datetime
import logging
import json
from pprint import pprint
import boto3


logger = logging.getLogger(__name__)
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
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    all_objects = response.get("Contents", [])
    table_files = []
    for obj in all_objects:
        if table_name in obj["Key"]:
            table_files.append(obj["Key"])

    if not table_files:
        logger.error(f"No files found for table {table_name}")
        raise AttributeError

    return table_files[0]


def remove_created_at_and_last_updated(df):
    # remove created_at and last_updated keys function
    # Assuming df is your DataFrame
    df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    return df


def process_fact_sales_order(bucket=INGESTION_S3_BUCKET_NAME):
    # split created at date into created_date and created_time keys
    # split last updated into last_updated_date and last_updated_time keys
    key = get_object_key(table_name="sales_order", prefix="updated/", bucket=bucket)

    obj = s3.get_object(Bucket=bucket, Key=key)
    sales_order_json = obj["Body"].read().decode("utf-8")
    sales_order_list = json.loads(sales_order_json)
    for dictionary in sales_order_list:
        dictionary["created_date"] = dictionary["created_at"][:10]
        dictionary["created_time"] = dictionary["created_at"][11:]
        dictionary["last_updated_date"] = dictionary["last_updated"][:10]
        dictionary["last_updated_time"] = dictionary["last_updated"][11:]
    fact_sales_order_df = pd.DataFrame(sales_order_list)
    fact_sales_order_df = remove_created_at_and_last_updated(fact_sales_order_df)
    return fact_sales_order_df


def process_dim_counterparty():
    # remove commercial_contact and delivery_contact keys
    remove_created_at_and_last_updated()
    pass


def process_dim_currency(bucket=INGESTION_S3_BUCKET_NAME):
    # create currency_name column
    # remove_created_at_and_last_updated()
    key = get_object_key(table_name="currency", prefix="baseline/", bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    currency_json = obj["Body"].read().decode("utf-8")
    currency_list = json.loads(currency_json)
    dim_currency_df = pd.DataFrame(currency_list)
    remove_created_at_and_last_updated(dim_currency_df)
    currency_names = {
        'GDP': 'British Pound',
        'USD': 'US Dollar',
        'EUR': 'Euro'
    }
    dim_currency_df["currency_name"] = dim_currency_df["currency_code"].map(currency_names)
    return dim_currency_df


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


def convert_dataframe_to_parquet():
    # convert file format from json to parquet
    # Read JSON into DataFrame
    # json_data = [{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]
    # df = pd.DataFrame(json_data)

    # # Convert DataFrame to Arrow Table
    # table = pa.Table.from_pandas(df)

    # # Write Arrow Table to Parquet file
    # pq.write_table(table, 'output.parquet')
    pass


# if __name__ == "__main__":

    #process_fact_sales_order(df)
