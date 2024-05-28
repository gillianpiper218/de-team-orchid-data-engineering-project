import pandas as pd
from botocore.exceptions import ClientError
from datetime import datetime
import pg8000.exceptions
import pg8000.native
import logging
import json
import boto3
import pyarrow.parquet as pq
import io
from time import sleep
import pyarrow as pa


# timestamp for now
current_time = datetime.now()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# S3 processed bucket
S3_PROCESSED_BUCKET_NAME = "de-team-orchid-totesys-processed"
# S3 client
s3_client = boto3.client("s3")
# secret manager client
secret_manager_client = boto3.client("secretsmanager")


def retrieve_secret_credentials(secret_name="dw_environment"):
    response = secret_manager_client.get_secret_value(
        SecretId=secret_name,
    )

    secret_string = json.loads(response["SecretString"])
    DW_HOST = secret_string["host"]
    DW_PORT = secret_string["port"]
    DW_NAME = secret_string["dbname"]
    DW_USER = secret_string["username"]
    DW_PASSWORD = secret_string["password"]
    DW_SCHEMA = secret_string["dwschema"]
    return DW_HOST, DW_PASSWORD, DW_NAME, DW_PORT, DW_USER, DW_SCHEMA


def connect_to_dw(credentials=retrieve_secret_credentials()):
    DW_HOST = credentials[0]
    DW_PORT = credentials[3]
    DW_NAME = credentials[2]
    DW_USER = credentials[4]
    DW_PASSWORD = credentials[1]
    DW_SCHEMA = credentials[5]

    try:
        logger.info(f"Connecting to the database {DW_NAME}")
        conn = pg8000.connect(
            host=DW_HOST,
            port=DW_PORT,
            database=DW_NAME,
            user=DW_USER,
            password=DW_PASSWORD
        )
        logger.info("Connected to the database successfully")

        return conn
    except pg8000.DatabaseError as e:
        logger.error(f"Error connecting to database: {e}")
        raise

    except pg8000.exceptions.InterfaceError as e:
        logger.error(f"Error connecting to the database: {e}")
        raise


def get_latest_parquet_file_key(prefix, bucket=S3_PROCESSED_BUCKET_NAME):
    try:
        s3_client = boto3.client("s3")
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if "Contents" not in response:
            raise FileNotFoundError(
                f"No files have been found from {bucket} for prefix: {prefix}"
            )
        else:
            process_content_keys_list = []
            for content in response["Contents"]:
                if content["Key"].endswith(".parquet"):
                    process_content_keys_list.append(content["Key"])
        return process_content_keys_list[-1]
    except FileNotFoundError as fnfe:
        logger.error(
            f"No files have been found from {bucket} for prefix: {prefix}: {fnfe}"
        )
        raise
    except Exception as e:
        logger.error(
            f"Error getting the latest parquet file from bucket {bucket} for prefix {prefix}: {e}"
        )
        raise


def read_parquet_from_s3(key, bucket=S3_PROCESSED_BUCKET_NAME):
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        # read into bytesio object first
        body_io = io.BytesIO(response["Body"].read())
        p_table = pq.read_table(body_io)
        return p_table
    except ClientError as ce:
        if ce.response["Error"]["Code"] == "NoSuchKey":
            logger.error(f"Key: {key} does not exist: {ce}")
            raise
    except Exception as e:
        logger.error(
            f"Error reading parquet file from bucket {bucket} with key {key}: {e}"
        )
        raise


"""pseudocode for def load_dim_tables(): Accepts an argument, bucket, default value is the processing s3 bucket
   
    create a list called dim_tables with names: dim_date, dim_staff, dim_counterparty, dim_currency, dim_design, dim_location.
    For each table_name in the dim_tables list:
        Create a variable prefix for 'f-stringing' with "dimension/" with the table_name.- CHECK IF THIS IS THE CORRECT PREFIX FOR PROCESSING BUCKET!
        Get the 'latest Parquet file key' using this prefix and the bucket name.
        'Read the Parquet file from S3' assign to variable called p_dim_table.
        'Load the p_dim_table into the data warehouse', called with  and p_table data and 2nd arg table_name"""


def load_dim_tables(bucket=S3_PROCESSED_BUCKET_NAME):

    dimension_tables = [
        "dim_date",
        "dim_staff",
        "dim_counterparty",
        "dim_currency",
        "dim_design",
        "dim_location",
    ]

    for dim_table_name in dimension_tables:
        dim_prefix = (
            f"dimension/{dim_table_name[4:]}"  # confirm s3 processing bucket keys
        )
        dim_key = get_latest_parquet_file_key(dim_prefix, bucket=bucket)
        p_dim_table_data = read_parquet_from_s3(dim_key, bucket=bucket)
        load_to_data_warehouse(p_dim_table_data, dim_table_name)


def load_fact_table(bucket=S3_PROCESSED_BUCKET_NAME):
    fact_table_name = "fact_sales_order"
    fact_prefix = "fact/sales_order"  # confirm s3 processing bucket keys
    fact_key = get_latest_parquet_file_key(fact_prefix, bucket=bucket)
    p_fact_table_data = read_parquet_from_s3(fact_key, bucket=bucket)
    load_to_data_warehouse(p_fact_table_data, fact_table_name)


def load_to_data_warehouse(table_data, table_name):
    """
    loads data into the data warehouse table.
    - table_data: pyarrow table containing the data to load.
    - table_name: name of the target table in the data warehouse.
    """
    try:
        conn = connect_to_dw()
        cursor = conn.cursor()
        try:
            with io.BytesIO() as buffer:
                pq.write_table(table_data, buffer)
                buffer.seek(0)
                sql_copy_query = f"COPY project_team_1.{table_name} FROM STDIN WITH (FORMAT 'parquet')"
                cursor.execute(sql_copy_query, stream=buffer)
                sleep(2.0)
                conn.commit()
                logger.info(f" Successfully loaded data into {table_name}")
        except Exception as e:
            logger.error(f"Error during loading {table_name}: {e}")
            conn.rollback()
            raise
        finally:
            cursor.close()
            conn.close()
    except Exception as ex:
        logger.error(f"Failed to connect to dw and load into {table_name}: {ex}")
        raise


def lambda_handler(event, context):
    try:
        load_dim_tables()
        load_fact_table()
    except Exception as e:
        logger.error(f"Error in loading lambda execution: {e}")
        raise
