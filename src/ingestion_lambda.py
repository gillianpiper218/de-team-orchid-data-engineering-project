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
    '''Uses the boto3 module with the AWS secrets manager to store and
      retrieve AWS credentials securely.

        Parameters:
                secret_name (str): 
                    keyword argument with the database environment as a default value.

        Returns:
			DB_HOST, DB_PASSWORD, DB_NAME, DB_PORT, DB_USER (Tuple): Credentials needed for AWS.
    
    '''
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


def connect_to_db(credentials=retrieve_secret_credentials()):
    '''Retrieves credentials from retrieve_secret_credentials(),
    stores as new constants with the same variable names as within retrieve_secret_credentials(),
    connects to the database,
    logs the outcome of connection to the database when successfully or unsuccessfully connected.

        Returns:
			conn (Connection): Connection to the database.

        Errors:
			DatabaseError: pg8000 specific error which is related to the database itself, such as incorrect SQL or constraint violations. Logs failure in logger.
			InterfaceError: pg8000 specific error when connection failure with the database occurs. Logs failure in logger.
    '''
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
        raise
    except pg8000.exceptions.InterfaceError as e:
        logger.error(f"Error connecting to the database: {e}")
        raise


def get_table_names():
    '''Opens a connection to the database and runs an SQL query to get all relevant table names, 
        list them and then sort them alphabetically. 
        Ensures that a connection to the database is closed.

        Returns:
			table_names (list): Sorted list of table names from database.

        Errors:
			DatabaseError: pg8000 specific error which is related to the database itself, such as incorrect SQL or constraint violations. Logs failure in logger.
			InterfaceError: pg8000 specific error when connection failure with the database occurs. Logs failure in logger.
    '''
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
    '''Sets up the baseline database and uploads to s3 bucket:
    
    Tries to set up a cursor, 
    if successful it loops through each table to extract all rows,
    extracts all columnn names,
    creates a dataframe,
    converts that to json string format,
    names the key based on the table name and the time of upload and
    puts the json string into the s3 object,
    logs result to the logger.

    Parameters:
        bucket_name (str): 
            keyword argument - s3 bucket name.
		name_of_tables (list): 
            keyword argument - list of the names of tables.
		db (Connection): 
            keyword argument - connection to the database.
		query_limit (str): 
            keyword argument - query limit for the query.

    Errors:
        ClientError:
            returns error if no s3 bucket exists in the given name.
            Logs to the logger.
    '''
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
            logger.info({"Result": f"Uploaded file to {s3_bucket_key}"})
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
    '''Tries to set up a cursor, 
    if successful loops through every table,
    runs a query that selects all entries in the last 20 minutes from time of execution,
    same method as select_all_tables_for_baseline(),
    *[could we abstract this away into a function rather than repeating code?],

    Parameters:
        db (Connection):
            keyword argument - connection to database.
        name_of_tables (list):
            keyword argument - list of tables.
        bucket_name (str):
            keyword argument - name of the s3 bucket.

    Errors:
        ClientError:
            returns error if bucket doesnt exist.
            Logs to the logger.
    '''
    try:
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


def check_baseline_exists():
    response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix="baseline/")
    return "Contents" in response
