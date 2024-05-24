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
S3_BUCKET_NAME = "de-team-orchid-totesys-processed"
# S3 client
s3 = boto3.client("s3")
# secret manager client
secret_manager_client = boto3.client("secretsmanager")


def retrieve_secret_credentials(secret_name="totesys_environment"):
    response = secret_manager_client.get_secret_value(
        SecretId=secret_name,
    )

    secret_string = json.loads(response["SecretString"])
    DW_HOST = secret_string["host"]
    DW_PORT = secret_string["port"]
    DW_NAME = secret_string["dbname"]
    DW_USER = secret_string["username"]
    DW_PASSWORD = secret_string["password"]
    return DW_HOST, DW_PASSWORD, DW_NAME, DW_PORT, DW_USER


def connect_to_dw(credentials=retrieve_secret_credentials()):
    """Retrieves credentials from retrieve_secret_credentials(),
    stores as new constants with the same variable names as within retrieve_secret_credentials(),
    connects to the database,
    logs the outcome of connection to the database when successfully or unsuccessfully connected.

        Returns:
                        conn (Connection): Connection to the database.

        Errors:
                        DatabaseError: pg8000 specific error which is related to the database itself, such as incorrect SQL or constraint violations. Logs failure in logger.
                        InterfaceError: pg8000 specific error when connection failure with the database occurs. Logs failure in logger.
    """
    DW_HOST = credentials[0]
    DW_PORT = credentials[3]
    DW_NAME = credentials[2]
    DW_USER = credentials[4]
    DW_PASSWORD = credentials[1]

    try:
        logger.info(f"Connecting to the database {DW_NAME}")
        conn = pg8000.connect(
            host=DW_HOST,
            port=DW_PORT,
            database=DW_NAME,
            user=DW_USER,
            password=DW_PASSWORD,
        )
        logger.info("Connected to the database successfully")

        return conn
    except pg8000.DatabaseError as e:
        logger.error(f"Error connecting to database: {e}")
        raise

    except pg8000.exceptions.InterfaceError as e:
        logger.error(f"Error connecting to the database: {e}")
        raise
