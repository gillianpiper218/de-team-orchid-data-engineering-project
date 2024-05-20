import pprint
import os
import pandas as pd
from datetime import datetime
import pg8000.exceptions
from dotenv import load_dotenv
import pg8000.native
import logging
from pg8000.native import literal
import json
from pprint import pprint
import boto3
import re

load_dotenv()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

current_time = datetime.now()


S3_BUCKET_NAME = "de-team-orchid-totesys-ingestion"


DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")


def connect_to_db():
    try:
        logger.info(f"Connecting to the database {DB_NAME}")
        conn = pg8000.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        logger.info("Connected to the database successfully")

        return conn
    except pg8000.DatabaseError as e:
        logger.error(f"Error connecting to database: {e}")
        raise
    except pg8000.exceptions.InterfaceError as e:
        logger.error(f"Error connecting to the database: {e}")
        raise


def get_table_names():
    db = None
    try:
        db = connect_to_db()
        table_names = db.run(
            """SELECT table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_type = 'BASE TABLE' 
        AND table_name NOT LIKE 'pg_%'
        AND table_name NOT LIKE 'sql_%'
        AND table_name NOT LIKE '_prisma_migrations%';"""
        )
        table_names = list(table_names)
        table_names = sorted(table_names)
        return table_names

    except pg8000.exceptions.DatabaseError as e:
        logger.error(f"Error connecting to database: {e}")
        raise
    except pg8000.exceptions.InterfaceError as e:
        logger.error(f"Error connecting to the database: {e}")
    finally:
        if db:
            db.close()


s3 = boto3.client("s3")


def select_all_tables_for_baseline(
    bucket_name=S3_BUCKET_NAME,
    name_of_tables=get_table_names(),
    db=connect_to_db(),
    query_limit="",
    **kwargs,
):

    cursor = db.cursor()
    if not query_limit == "":
        query_limit = "LIMIT 2"

    for table_name in name_of_tables:
        cursor.execute(f"SELECT * FROM {table_name[0]} {query_limit};")
        result = cursor.fetchall()
        col_names = [elt[0] for elt in cursor.description]
        df = pd.DataFrame(result, columns=col_names)
        json_data = df.to_json(orient="records")

        data = json.dumps(json.loads(json_data))

        s3_bucket_key = f"baseline/{table_name[0]}-{current_time}.json"
        s3.put_object(Body=data, Bucket=bucket_name, Key=s3_bucket_key)
        logger.info({"Result": f"Uploaded file to {s3_bucket_key}"})


def select_and_write_updated_data(
    db=connect_to_db(),
    name_of_tables=get_table_names(),
    bucket_name=S3_BUCKET_NAME,
    **kwargs,
):
    cursor = db.cursor()
    for table_name in name_of_tables:
        cursor.execute(
            f"""SELECT * FROM {table_name[0]} WHERE last_updated
                       > NOW() - interval '20 minutes';
                       """
        )
        result = cursor.fetchall()
        col_names = [elt[0] for elt in cursor.description]
        df = pd.DataFrame(result, columns=col_names)
        json_data = df.to_json(orient="records")

        data = json.dumps(json.loads(json_data))
        file_path = f"updated/{table_name[0]}-{current_time}.json"
        s3.put_object(Body=data, Bucket=bucket_name, Key=file_path)

        logger.info({"Result": f"update to file at {file_path}"})


# def delete_empty_s3_files():
#     try:
#         response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="updated/")
#         if "Contents" in response:
#             print("There are objects in the 'staging/' folder.")
#             for obj in response["Contents"]:
#                 obj_size = obj["Size"]
#                 file_path = ""
#                 if obj_size == 0:
#                     s3.delete_object(Bucket=S3_BUCKET_NAME, Key=file_path)
#                     logger.info(f"Delete empty s3 file: {file_path}")

#     except:
#         logger.error(f"Error deleting empty files")


def check_baseline_exists():
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="baseline/")
    return "Contents" in response


def lambda_handler(event, context):
    try:
        if not check_baseline_exists():
            logger.info("Baseline does not exist. Running baseline data extraction.")
            select_all_tables_for_baseline()
        else:
            logger.info("Baseline exists. Running updated data extraction.")

        select_and_write_updated_data()

        # delete_empty_s3_files()
    except Exception as e:
        logger.error(f"Error in Lambda execution: {e}")
        raise
