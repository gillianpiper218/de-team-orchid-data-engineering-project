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
import pprint
import boto3

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Database connection
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT = os.environ["DB_PORT"]

# S3 injestion bucket
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
        return table_names

    except pg8000.exceptions.DatabaseError as e:
        logger.error(f"Error connecting to database: {e}")
        raise
    except pg8000.exceptions.InterfaceError as e:
        logger.error(f"Error connecting to the database: {e}")
    finally:
        if db:
            db.close()


def select_all_tables_for_baseline():
    db = connect_to_db()
    cursor = db.cursor()
    name_of_tables = get_table_names()

    for table_name in name_of_tables:
        cursor.execute(f"SELECT * FROM {table_name[0]};")
        result = cursor.fetchall()
        col_names = [elt[0] for elt in cursor.description]
        df = pd.DataFrame(result, columns=col_names)
        json_data = df.to_json(orient="records")

        data = json.dumps(json.loads(json_data))
        file_path = f"baseline/{table_name[0]}.json"
        s3.put_object(Body=data, Bucket=S3_BUCKET_NAME, Key=file_path)
        logger.info({"Result": f"Uploaded file to {file_path}"})


def initial_data_for_latest():
    table_names = get_table_names()
    for table in table_names:
        s3.copy_object(
            Bucket=S3_BUCKET_NAME,
            CopySource=f"{S3_BUCKET_NAME}/baseline/{table[0]}.json",
            Key=f"latest/{table[0]}.json",
        )


def select_and_write_updated_data():
    db = connect_to_db()
    cursor = db.cursor()
    name_of_tables = get_table_names()
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
        file_path = f"staging/{table_name[0]}.json"
        s3.put_object(Body=data, Bucket=S3_BUCKET_NAME, Key=file_path)
        logger.info({"Result": f"update to file at {file_path}"})


def get_s3_object_data(key):
    # try:
    response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
    data = json.loads(response["Body"].read().decode("utf-8"))
    return data
    # except s3.exceptions.NoSuchKey:
     


def update_latest_with_new_record():
    staging_response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="staging/")
    list_of_staging_files = []
    for item in staging_response["Contents"]:
        if item["Size"] > 2:
            list_of_staging_files.append(item["Key"][8:])
        pprint.pp(list_of_staging_files)
    if list_of_staging_files == []:
        logger.info("No new files")
        print("No new files")
        pprint.pp(list_of_staging_files)
    else:
        latest_response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="latest/")
        list_of_latest_files = []
        for l_item in latest_response["Contents"]:
            if l_item["Key"][7:] in list_of_staging_files:
                list_of_latest_files.append(l_item["Key"][7:])
        #pprint.pp(list_of_latest_files)

        for item in list_of_staging_files:
            staging_data = get_s3_object_data(f'staging/{item}')
            latest_data = get_s3_object_data(f'latest/{item}')

            
            biggest_latest_id = max(latest_data.values())
            pprint.pp(biggest_latest_id)

            # for key, value in staging_data.items():
            #     if key == f"{item}_id":
                    
            
        #pprint.pp(staging_data)
        #pprint.pp(latest_data)
   
#inside staging data and latest data, find column where column name = f"{item}_id"
#find largest id inside latest data, compare this with all of staging data records
#if the staging data id is larger than the largest latest data id, append/copy (no deletion) this staging record to latest data
    

    
# for each file in staging that is not empty
# fetch the biggest ID number from the equivalent file in latest
# if the id number in staging is bigger than the biggest id number in latest
# append the row of data for that id into the latest file


if __name__ == "__main__":
    # Test database connection

    db = connect_to_db()
    # select_all_tables_for_baseline()
    # initial_data_for_latest()
    select_and_write_updated_data()
    update_latest_with_new_record()

# need a fetch tables function - log error if cant fetch the data - SELECT * FROM {table_name}" - stop injection
#  need an upload to s3 function - need boto.client put object into s3 object - need to decide structure, log error if cant upload to s3 bucket, log if successful
