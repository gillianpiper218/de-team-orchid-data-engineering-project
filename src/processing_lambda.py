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
        raise FileNotFoundError(f"No files found for table {table_name}")

    return table_files[0]


def remove_created_at_and_last_updated(df):
    # remove created_at and last_updated keys function
    df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    return df


def process_fact_sales_order(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    # split created at date into created_date and created_time keys
    # split last updated into last_updated_date and last_updated_time keys
    key = get_object_key(table_name="sales_order", prefix=prefix, bucket=bucket)

    obj = s3.get_object(Bucket=bucket, Key=key)
    sales_order_json = obj["Body"].read().decode("utf-8")
    sales_order_list = json.loads(sales_order_json)

    for dictionary in sales_order_list:
        if "staff_id" in dictionary:
            dictionary["sales_staff_id"] = dictionary.pop("staff_id")

        unix_ts_s_created_at = (dictionary["created_at"]) / 1000
        unix_ts_s_last_updated = (dictionary["last_updated"]) / 1000

        formatted_time_c = datetime.fromtimestamp(unix_ts_s_created_at)
        formatted_time_l = datetime.fromtimestamp(unix_ts_s_last_updated)

        dictionary["created_date"] = formatted_time_c.date()
        dictionary["created_time"] = formatted_time_c.time()
        dictionary["last_updated_date"] = formatted_time_l.date()
        dictionary["last_updated_time"] = formatted_time_l.time()

    fact_sales_order_df = pd.DataFrame(sales_order_list)
    fact_sales_order_df = remove_created_at_and_last_updated(fact_sales_order_df)
    return fact_sales_order_df


def process_dim_counterparty(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):

    key = get_object_key(table_name="counterparty", prefix=prefix, bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    counterparty_json = obj["Body"].read().decode("utf-8")
    counterparty_list = json.loads(counterparty_json)

    key = get_object_key(table_name="address", prefix=prefix, bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    address_json = obj["Body"].read().decode("utf-8")
    address_list = json.loads(address_json)

    for counterparty_dict in counterparty_list:
        for address_dict in address_list:
            if address_dict["address_id"] == counterparty_dict["legal_address_id"]:
                counterparty_dict["counterparty_legal_address_line_1"] = address_dict[
                    "address_line_1"
                ]
                counterparty_dict["counterparty_legal_address_line_2"] = address_dict[
                    "address_line_2"
                ]
                counterparty_dict["counterparty_legal_district"] = address_dict[
                    "district"
                ]
                counterparty_dict["counterparty_legal_city"] = address_dict["city"]
                counterparty_dict["counterparty_legal_postal_code"] = address_dict[
                    "postal_code"
                ]
                counterparty_dict["counterparty_legal_country"] = address_dict[
                    "country"
                ]
                counterparty_dict["counterparty_legal_phone_number"] = address_dict[
                    "phone"
                ]

    dim_counterparty_df = pd.DataFrame(counterparty_list)
    dim_counterparty_df = remove_created_at_and_last_updated(dim_counterparty_df)
    dim_counterparty_df.drop(
        ["commercial_contact", "delivery_contact", "legal_address_id"],
        axis=1,
        inplace=True,
    )
    return dim_counterparty_df


def process_dim_currency(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    # create currency_name column
    # remove_created_at_and_last_updated()
    key = get_object_key(table_name="currency", prefix=prefix, bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    currency_json = obj["Body"].read().decode("utf-8")
    currency_list = json.loads(currency_json)
    dim_currency_df = pd.DataFrame(currency_list)
    remove_created_at_and_last_updated(dim_currency_df)
    currency_names = {"GDP": "British Pound", "USD": "US Dollar", "EUR": "Euro"}
    dim_currency_df["currency_name"] = dim_currency_df["currency_code"].map(
        currency_names
    )
    return dim_currency_df


def process_dim_date(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    # create from each unique date
    # - year
    # - month
    # - day
    # - day_of_week (int type where Monday = 1 and Sunday = 7)
    # - day_name
    # - month_name
    # - quarter

    fso_df = process_fact_sales_order(bucket=bucket, prefix=prefix)
    fso_dicts = fso_df.to_dict(orient="records")

    dim_date = []
    date_columns = [
        "created_date",
        "last_updated_date",
        "agreed_payment_date",
        "agreed_delivery_date",
    ]
    for sales_order_dict in fso_dicts:
        for col in date_columns:
            date_string = str(sales_order_dict[col])

            date_format = "%Y-%m-%d"
            dt_obj = datetime.strptime(date_string, date_format)

            dim_date_item = {
                "date_id": dt_obj.date(),
                "year": dt_obj.year,
                "month": dt_obj.month,
                "day": dt_obj.day,
                "day_of_week": dt_obj.isoweekday(),
                "day_name": dt_obj.strftime("%A"),
                "month_name": dt_obj.strftime("%B"),
                "quarter": (dt_obj.month - 1) // 3 + 1,
            }
            dim_date.append(dim_date_item)
    dim_date_df = pd.DataFrame(dim_date)
    dim_date_df = dim_date_df.drop_duplicates(subset=["date_id"])

    # print(fso_df)
    # print(fso_dicts)
    # print(dim_date_df)
    return dim_date_df


def process_dim_design(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    key = get_object_key(table_name="design", prefix=prefix, bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    design_json = obj["Body"].read().decode("utf-8")
    design_list = json.loads(design_json)["design"]
    df = pd.DataFrame(design_list)
    return_df = remove_created_at_and_last_updated(df)
    return return_df


def process_dim_location(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    # change address_id key into location_id
    key = get_object_key(table_name="address", prefix=prefix, bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    location_json = obj["Body"].read().decode("utf-8")
    location_list = json.loads(location_json)["address"]
    for location_dict in location_list:
        location_dict["location_id"] = location_dict["address_id"]
        del location_dict["address_id"]
    df = pd.DataFrame(location_list)
    return_df = remove_created_at_and_last_updated(df)
    return return_df


def process_dim_staff(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    key = get_object_key(table_name="staff", prefix=prefix, bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    staff_json = obj["Body"].read().decode("utf-8")
    staff_list = json.loads(staff_json)["staff"]
    df = pd.DataFrame(staff_list)
    return_df = remove_created_at_and_last_updated(df)
    key = "dimension/staff.parquet"
    return return_df, key


def convert_to_parquet_put_in_s3(s3, df, key, bucket=PROCESSED_S3_BUCKET_NAME):
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=out_buffer.getvalue())


# if __name__ == "__main__":
#     process_dim_date(bucket=INGESTION_S3_BUCKET_NAME)
