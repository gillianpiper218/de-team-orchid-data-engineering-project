import pandas as pd
from botocore.exceptions import ClientError
from datetime import datetime
import pg8000.exceptions
import pg8000.native
import logging
import json
import boto3

# timestamp for now
current_time = datetime.now()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# S3 ingestion bucket
S3_BUCKET_NAME = "de-team-orchid-totesys-ingestion"
# S3 client
s3 = boto3.client("s3")
# secret manager client
secret_manager_client = boto3.client("secretsmanager")


def retrieve_secret_credentials(secret_name="totesys_environment"):
    response = secret_manager_client.get_secret_value(
        SecretId=secret_name,
    )

    secret_string = json.loads(response["SecretString"])
    DB_HOST = secret_string["host"]
    DB_PORT = secret_string["port"]
    DB_NAME = secret_string["dbname"]
    DB_USER = secret_string["username"]
    DB_PASSWORD = secret_string["password"]
    return DB_HOST, DB_PASSWORD, DB_NAME, DB_PORT, DB_USER


def connect_to_db():
    credentials = retrieve_secret_credentials()
    DB_HOST = credentials[0]
    DB_PORT = credentials[3]
    DB_NAME = credentials[2]
    DB_USER = credentials[4]
    DB_PASSWORD = credentials[1]

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


def select_all_tables_for_baseline(
    bucket_name=S3_BUCKET_NAME,
    name_of_tables=get_table_names(),
    db=connect_to_db(),
    query_limit="",
    **kwargs,
):
    try:
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
            logger.info(f'{"Result": "Uploaded file to {s3_bucket_key}"}')
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchBucket":
            logger.info("No bucket found")
            raise


def select_and_write_updated_data(
    db=connect_to_db(),
    name_of_tables=get_table_names(),
    bucket_name=S3_BUCKET_NAME,
    **kwargs,
):
    try:
        cursor = db.cursor()
        for table_name in name_of_tables:
            cursor.execute(
                f"""SELECT * FROM {table_name[0]} WHERE last_updated
                        > NOW() - interval '20 minutes';
                        """
            )
            result = cursor.fetchall()
            if len(result) == 0:
                logger.info("No new data found")
            else:
                col_names = [elt[0] for elt in cursor.description]
                df = pd.DataFrame(result, columns=col_names)
                json_data = df.to_json(orient="records")
                data = json.dumps(json.loads(json_data))
                file_path = f"updated/{table_name[0]}-{current_time}.json"
                s3.put_object(Body=data, Bucket=bucket_name, Key=file_path)
                logger.info("New data added to updated")
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchBucket":
            logger.info("No bucket found")
            raise


def delete_empty_s3_files(bucket_name=S3_BUCKET_NAME):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix="updated/")
        if "Contents" in response:
            for obj in response["Contents"]:
                obj_size = obj["Size"]
                file_path = obj["Key"]
                if obj_size <= 2:
                    s3.delete_object(Bucket=bucket_name, Key=file_path)
                    logger.info(f"Delete empty s3 file: {file_path}")
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchBucket":
            logger.info("No bucket found")
            raise


def lambda_handler(event, context):
    try:
        conn = connect_to_db()
        if not check_baseline_exists():
            logger.info(
                "Baseline does not exist. Running baseline data extraction.")
            select_all_tables_for_baseline()
        else:
            logger.info("Baseline exists. Running updated data extraction.")
            select_and_write_updated_data()
            delete_empty_s3_files()

    except Exception as e:
        logger.error(f"Error in Lambda execution: {e}")
        raise
    finally:
        if conn:
            conn.close()


def check_baseline_exists():
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="baseline/")
    return "Contents" in response
