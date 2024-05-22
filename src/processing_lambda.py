import os
import pandas as pd
from datetime import datetime
import logging
import json
from pprint import pprint
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO


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
    sales_order_list = json.loads(sales_order_json)["sales_order"]
    for dictionary in sales_order_list:
        dictionary["created_date"] = dictionary["created_at"][:10]
        dictionary["created_time"] = dictionary["created_at"][11:]
        dictionary["last_updated_date"] = dictionary["last_updated"][:10]
        dictionary["last_updated_time"] = dictionary["last_updated"][11:]
    df = pd.DataFrame(sales_order_list)
    return_df = remove_created_at_and_last_updated(df)
    return return_df


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


def process_dim_design(bucket=INGESTION_S3_BUCKET_NAME):
    key = get_object_key(table_name="design", prefix="baseline/", bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    design_json = obj["Body"].read().decode("utf-8")
    design_list = json.loads(design_json)["design"]
    df = pd.DataFrame(design_list)
    return_df = remove_created_at_and_last_updated(df)
    return return_df


def process_dim_location(bucket=INGESTION_S3_BUCKET_NAME):
    # change address_id key into location_id
    key = get_object_key(table_name="address", prefix="baseline/", bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    location_json = obj["Body"].read().decode("utf-8")
    location_list = json.loads(location_json)["address"]
    for location_dict in location_list:
        location_dict["location_id"] = location_dict["address_id"]
        del location_dict["address_id"]
    df = pd.DataFrame(location_list)
    return_df = remove_created_at_and_last_updated(df)
    return return_df


def process_dim_staff(bucket=INGESTION_S3_BUCKET_NAME):
    key = get_object_key(table_name="staff", prefix="baseline/", bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    staff_json = obj["Body"].read().decode("utf-8")
    staff_list = json.loads(staff_json)["staff"]
    df = pd.DataFrame(staff_list)
    return_df = remove_created_at_and_last_updated(df)
    return return_df


def convert_to_parquet_put_in_s3(s3, df, key, bucket=PROCESSED_S3_BUCKET_NAME):
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=out_buffer.getvalue())


# if __name__ == "__main__":

#     process_fact_sales_order(df)
