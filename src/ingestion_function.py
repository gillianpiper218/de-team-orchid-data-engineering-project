# connect to the database - need to log an error for if connecting to database fails - get credentials from . env file , close conn afterwards
import pprint
import os
import pandas as pd

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

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Database connection
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT = os.environ["DB_PORT"]

# S3 ingestion bucket
S3_BUCKET_NAME = "de-team-orchid-totesys-ingestion"

# S3 client
s3 = boto3.client("s3")


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

        s3_bucket_key = f"baseline/{table_name[0]}.json"
        s3.put_object(Body=data, Bucket=bucket_name, Key=s3_bucket_key)
        logger.info({"Result": f"Uploaded file to {s3_bucket_key}"})


def initial_data_for_latest(table_names=get_table_names(), bucket_name=S3_BUCKET_NAME):
    for table in table_names:
        s3.copy_object(
            Bucket=bucket_name,
            CopySource=f"{bucket_name}/baseline/{table[0]}.json",
            Key=f"latest/{table[0]}.json",
        )


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
                       > NOW() - interval '50 minutes';
                       """
        )
        result = cursor.fetchall()
        col_names = [elt[0] for elt in cursor.description]
        df = pd.DataFrame(result, columns=col_names)
        json_data = df.to_json(orient="records")

        data = json.dumps(json.loads(json_data))
        file_path = f"staging/{table_name[0]}.json"
        s3.put_object(Body=data, Bucket=bucket_name, Key=file_path)

        logger.info({"Result": f"update to file at {file_path}"})


def delete_empty_s3_files():
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="staging/")
        if "Contents" in response:
            print("There are objects in the 'staging/' folder.")
            for obj in response["Contents"]:
                obj_size = obj["Size"]
                file_path = ""
                if obj_size == 0:
                    s3.delete_object(Bucket=S3_BUCKET_NAME, Key=file_path)
                    logger.info(f"Delete empty s3 file: {file_path}")

    except:
        logger.error(f"Error deleting empty files")


def get_s3_object_data(key):
    # try:
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        data = json.loads(response["Body"].read().decode("utf-8"))
        return data
    #except:



def update_latest_with_new_record():
    staging_response = s3.list_objects_v2(
        Bucket=S3_BUCKET_NAME, Prefix="staging/")
    list_of_staging_files = []
    for s_item in staging_response["Contents"]:
        if s_item["Size"] > 2:
            list_of_staging_files.append(s_item["Key"][8:])
        # pprint.pp(list_of_staging_files)

    if list_of_staging_files == []:
        logger.info("No new files")
        # print("No new files")
        # pprint.pp(list_of_staging_files)
    else:
        latest_response = s3.list_objects_v2(
            Bucket=S3_BUCKET_NAME, Prefix="latest/")
        list_of_latest_files = []
        for l_item in latest_response["Contents"]:
            if l_item["Key"][7:] in list_of_staging_files:
                list_of_latest_files.append(l_item["Key"][7:])

        for item in list_of_staging_files:
            staging_data = get_s3_object_data(f"staging/{item}")
            latest_data = get_s3_object_data(f"latest/{item}")

            col_id_name = re.sub(r"\.json", "_id", item)
            biggest_id_dict = max(latest_data, key=lambda x: x[col_id_name])

            # pprint.pp(biggest_id_dict)
            # print(">>>>>>>>>>>>>>>>>>>>>>>>>>")
            for el in staging_data:
                pprint.pp(el)
                if el[col_id_name] > biggest_id_dict[col_id_name]:
                    latest_data.append(el)
                    logger.info("new record added to latest")
            data = json.dumps(latest_data)
            file_path = f"latest/{item}"
            s3.put_object(Body=data, Bucket=S3_BUCKET_NAME, Key=file_path)






def delete_empty_s3_files():
    try:
        response=s3.list_objects_v2(Bucket=S3_BUCKET_NAME,Prefix="staging/")
        
        if 'Contents' in response:
            print(f"there are objects in the files")
            for obj in response['Contents']:
                obj_size=obj['Size']
                file_path = obj['Key']
                if obj_size == 0:
                    s3.delete_object(Bucket=S3_BUCKET_NAME , Key=file_path)
                    logger.info(f"Delete empty s3 file: {file_path}")

    except Exception as e:
        logger.error(f"Error deleting empty files: {e}")

















if __name__ == "__main__":
    # Test database connection

    #     db = connect_to_db()
    #     # select_all_tables_for_baseline()
    #     select_all_updated_rows()
    db = connect_to_db()
    # select_all_tables_for_baseline()
    # get_table_columns()
    select_all_tables_for_baseline()
    # select_all_updated_rows()

    db = connect_to_db()

    delete_empty_s3_files()
    # select_all_tables_for_baseline()
    delete_empty_s3_files()
    # initial_data_for_latest()
    # select_and_write_updated_data()

# need a fetch tables function - log error if cant fetch the data - SELECT * FROM {table_name}" - stop injection
#  need an upload to s3 function - need boto.client put object into s3 object - need to decide structure, log error if cant upload to s3 bucket, log if successful
