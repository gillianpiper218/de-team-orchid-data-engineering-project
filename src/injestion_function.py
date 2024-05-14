# connect to the database - need to log an error for if connecting to database fails - get credentials from . env file , close conn afterwards
import pprint
import os

import pg8000.exceptions
from dotenv import load_dotenv
import pg8000.native
import logging
from pg8000.native import literal
import json
import pprint
load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Database connection
DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_PORT = os.environ['DB_PORT']

# S3 injestion bucket
S3_BUCKET_NAME = "de-team-orchid-totesys-ingestion"


def connect_to_db():
    try:
        logger.info(f"Connecting to the database {DB_NAME}")
        conn = pg8000.connect(host=DB_HOST, port=DB_PORT,
                              database=DB_NAME, user=DB_USER,
                              password=DB_PASSWORD)
        logger.info("Connected to the database successfully")

        return conn
    except pg8000.DatabaseError as e:
        logger.error(f"Error connecting to database: {e}")
        raise
    except pg8000.exceptions.InterfaceError as e:
        logger.error(f"Error connecting to the database: {e}")


def get_table_names():
    db = None
    try:
        db = connect_to_db()
        table_names = db.run(
            """SELECT table_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE table_type = 'BASE TABLE' 
        AND table_name NOT LIKE 'pg_%'
        AND table_name NOT LIKE 'sql_%';"""
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
    data_dictionary = {}
    for table_name in name_of_tables:
        cursor.execute(f"SELECT * FROM {table_name[0]};")
        rows = cursor.fetchall()

        data_dictionary[table_name[0]] = rows
    return data_dictionary


if __name__ == "__main__":
    # Test database connection

    db = connect_to_db()
    if db:
        print("Database connection successful")
    else:
        print("Database connection failed")
    select_all_tables_for_baseline()


# need a fetch tables function - log error if cant fetch the data - SELECT * FROM {table_name}" - stop injection
#  need an upload to s3 function - need boto.client put object into s3 object - need to decide structure, log error if cant upload to s3 bucket, log if successful
