from pprint import pprint
import pandas as pd
from datetime import datetime
import logging
import json
from pprint import pprint
import boto3
from io import BytesIO
# from src.ingestion_lambda import get_table_names
from botocore.exceptions import ClientError
import re
import pg8000

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")
current_time = datetime.now()
secret_manager_client = boto3.client("secretsmanager")

INGESTION_S3_BUCKET_NAME = "de-team-orchid-totesys-ingestion"
PROCESSED_S3_BUCKET_NAME = "de-team-orchid-totesys-processed"

def retrieve_secret_credentials(secret_name="totesys_environment"):
    """Uses the boto3 module with the AWS secrets manager to store and
    retrieve AWS credentials securely.

      Parameters:
              secret_name (str):
                  keyword argument with the database environment as a default value.

      Returns:
                      DB_HOST, DB_PASSWORD, DB_NAME, DB_PORT, DB_USER (Tuple): Credentials needed for AWS.

    """
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
    """Opens a connection to the database and runs an SQL query to get all relevant table names,
    list them and then sort them alphabetically.
    Ensures that a connection to the database is closed.

    Returns:
                    table_names (list): Sorted list of table names from database.

    Errors:
                    DatabaseError: pg8000 specific error which is related to the database itself, such as incorrect SQL or constraint violations. Logs failure in logger.
                    InterfaceError: pg8000 specific error when connection failure with the database occurs. Logs failure in logger.
    """
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
        print(table_names)
        return table_names
    except pg8000.exceptions.DatabaseError as e:
        logger.error(f"Error connecting to database: {e}")
        raise
    except pg8000.exceptions.InterfaceError as e:
        logger.error(f"Error connecting to the database: {e}")
    finally:
        if db:
            db.close()




def get_object_key(
    table_name: str,  bucket=INGESTION_S3_BUCKET_NAME
) -> str:
    """Retrieves the s3 object key for the specifed prefix table name and bucket
    Parameters:
        table_name(str): The name of the table to search for inside the s3 bucket.
        prefix(str): The prefix path to use when listing the objects in the s3 bucket if not specified no prefix is used.
        bucket(str):The name of the s3 bucket to get the objects from the default value is INGESTION_S3_BUCKET_NAME.
    Returns:
        (str): The key of the latest s3 object that matches the table name.
    Errors:
        FileNotFoundError: If no objects are found that matches the table name.
    """
    response = s3.list_objects_v2(Bucket=bucket, Prefix="updated/")
    # print(response)
    table_files = []
    number_of_files = response['KeyCount']
    for i in range(number_of_files):
        key = response['Contents'][i]['Key']
        table_files.append(key)
    
    
    # for obj in all_objects:
    #     if table_name in obj["Key"]:
    #         table_files.append(obj["Key"])
        
    if len(table_files) > 0:
        return table_files[-1]

    else:
        logger.info(f"No files found for table {table_name}")

    
    # raise FileNotFoundError(f"No files found for table {table_name}")
    # print(table_files)
    


def remove_created_at_and_last_updated(df):
    """Removes the created_at and last_updated columns from a DataFrame.
    Parameters:
        df(pandas.DataFrame): The DataFrame from which to remove the columns.
    Returns:
        (pandas.DataFrame): The DataFrame without created_at and last_updated columns.
    """
    df.drop(["created_at", "last_updated"], axis=1, inplace=True)
    return df


def process_fact_sales_order(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    """This function processes the latest sales_order data from the s3 bucket
    and split the created_at and last_updated columns into separate date and time columns , renames staff_id to
    sales_staff_id
    and returns the DataFrame with the created_at and last_updated columns removed.
    Parameters:
        bucket(str): The name of the s3 bucket to retrieve the sales_order table.
        The default value is INGESTION_S3_BUCKET_NAME.
        prefix(str): The file path of the s3 bucket, default value is None.
    Returns:
        (pandas.DataFrame): The DataFrame for the processed sales_order table.
        (str): The key for the processed data in the s3 bucket.
    """
    key = get_object_key(table_name="sales_order",
                          bucket=bucket)
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
    fact_sales_order_df = remove_created_at_and_last_updated(
        fact_sales_order_df)
    key = f"fact/sales_order-{current_time}.parquet"
    return fact_sales_order_df, key


def process_dim_counterparty(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    """
    Process the counterparty data with the address data from an s3 bucket and
    create the required columns for counterparty by renaming existing columns
    from the address table convert to a DataFrame, then remove columns,
    "created_at", "last_updated", "commercial_contact", "delivery_contact" and
    "legal_address_id".

    Parameter:
        bucket(str): The name of the s3 bucket where the address data is stored,
        the default value is INGESTION_S3_BUCKET_NAME.
        prefix(str): The file path of the s3 bucket, default value is None.

    Return:
        (pandas.DataFrame): A DataFrame containing processed counterparty data,
        with all required columns.
        (str): The key for the processed data in the s3 bucket.
    """

    key = get_object_key(table_name="counterparty",
                          bucket=bucket)
    print(key, "key")
    obj = s3.get_object(Bucket=bucket, Key=key)
    print(obj, "obj")
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
    dim_counterparty_df = remove_created_at_and_last_updated(
        dim_counterparty_df)
    dim_counterparty_df.drop(
        ["commercial_contact", "delivery_contact", "legal_address_id"],
        axis=1,
        inplace=True,
    )
    key = f"dimension/counterparty-{current_time}.parquet"
    return dim_counterparty_df, key


def process_dim_currency(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    """Process the currency table from the s3 bucket by adding a new column currency_name
    and converting into DataFrame then removing created_at and last_updated columns.
    Parameters:
        bucket(str): The name of the s3 bucket to retrieve the currency table.
        The default value is INGESTION_S3_BUCKET_NAME.
        prefix(str): The file path of the s3 bucket, default value is None.
    Returns:
        (pandas.DataFrame): The DataFrame for the processed currency data.
        (str): The key for the processed data in the s3 bucket.
    """
    key = get_object_key(table_name="currency", bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    currency_json = obj["Body"].read().decode("utf-8")
    currency_list = json.loads(currency_json)
   
    dim_currency_df = pd.DataFrame(currency_list)
   
    remove_created_at_and_last_updated(dim_currency_df)
    currency_names = {"GDP": "British Pound",
                      "USD": "US Dollar", "EUR": "Euro"}
    dim_currency_df["currency_name"] = dim_currency_df["currency_code"].map(
        currency_names
    )

    key = f"dimension/currency-{current_time}.parquet"

    return dim_currency_df, key


def process_dim_date(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    """
    Process the sales order data to create a dim date DataFrame.
    It extracts unique dates and creates additional columns for year, month,
    day, day_of_week, day_name, month_name and quarter.
    Parameters:
        bucket(str): The name of the bucket to retrieve the sales order data from.
        The default value is INGESTION_S3_BUCKET_NAME.
        prefix(str): The file path of the s3 bucket, default value is None.
    Returns:
        pandas.DataFrame: The DataFrame containing unique dates and the corresponding date-related columns.
        (str): The key for the processed data in the s3 bucket.
    """
    fso_df, key = process_fact_sales_order(bucket=bucket, prefix=prefix)
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
    key = f"dimension/date-{current_time}.parquet"
    return dim_date_df, key


def process_dim_design(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    """Process the design table from s3 bucket convert it into a DataFrame ,removes created_at and last_updated columns
    Parameters:
        bucket(str): The name of the s3 bucket to retrieve the design table.
        The default value is INGESTION_S3_BUCKET_NAME.
        prefix(str): The file path of the s3 bucket, default value is None.
    Returns:
        (pandas.DataFrame): The DataFrame for the processed design data.
        (str): The key for the processed data in the s3 bucket.
    """
    key = get_object_key(table_name="design", bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    design_json = obj["Body"].read().decode("utf-8")
    design_list = json.loads(design_json)
    df = pd.DataFrame(design_list)
    return_df = remove_created_at_and_last_updated(df)
    key = f"dimension/design-{current_time}.parquet"
    return return_df, key


def process_dim_location(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    """Process the address table from s3 bucket converts to json ,renames address_id column to
    location_id ,then converts to DataFrame , removes the created_at and last_updated columns.
    Parameters:
        bucket(str): The name of the s3 bucket to retrieve the address table.
        The default value is INGESTION_S3_BUCKET_NAME.
        prefix(str): The file path of the s3 bucket, default value is None.
    Returns:
        (pandas.DataFrame): The DataFrame for the processed address data as the location data.
        (str): The key for the processed data in the s3 bucket.
    """

    key = get_object_key(table_name="address", bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    location_json = obj["Body"].read().decode("utf-8")
    location_list = json.loads(location_json)
    for location_dict in location_list:
        location_dict["location_id"] = location_dict["address_id"]
        del location_dict["address_id"]
    df = pd.DataFrame(location_list)
    return_df = remove_created_at_and_last_updated(df)
    key = f"dimension/location-{current_time}.parquet"
    return return_df, key


def process_dim_staff(bucket=INGESTION_S3_BUCKET_NAME, prefix=None):
    """Process the staff table from s3 bucket convert it into a DataFrame ,removes created_at and last_updated columns
    Parameters:
        bucket(str): The name of the s3 bucket to retrieve the staff table.
        The default value is INGESTION_S3_BUCKET_NAME.
        prefix(str): The file path of the s3 bucket, default value is None.
    Returns:
        (pandas.DataFrame): The DataFrame for the processed staff data.
        (str): The key for the processed data in the s3 bucket.
    """

    key = get_object_key(table_name="staff", bucket=bucket)
    obj = s3.get_object(Bucket=bucket, Key=key)
    staff_json = obj["Body"].read().decode("utf-8")
    staff_list = json.loads(staff_json)
    df = pd.DataFrame(staff_list)
    return_df = remove_created_at_and_last_updated(df)
    key = f"dimension/staff-{current_time}.parquet"
    return return_df, key


def convert_to_parquet_put_in_s3(s3, df, key, bucket=PROCESSED_S3_BUCKET_NAME):
    """Converts a DataFrame into parquet and uploads it into s3 bucket by specified key .
    Parameters:
        s3(boto3.client): The boto3 client to interact with s3.
        df(pandas.DataFrame): The DataFrame  to convert into parquet.
        key(str): The s3 path to store the parquet file into s3 bucket.
        bucket(str): The name of the s3 bucket to upload the parquet file.
        The default value is PROCESSED_S3_BUCKET_NAME.
    Returns:
        None.
    """
    out_buffer = BytesIO()
    df.to_parquet(out_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=out_buffer.getvalue())


def delete_duplicates(bucket=INGESTION_S3_BUCKET_NAME):
    table_names = get_table_names()
    list_obj_response = s3.list_objects_v2(Bucket=bucket, Prefix="updated/")
    if not list_obj_response:
        logger.info("no files found")
    else:
        dict_list = list_obj_response["Contents"]
        dict_list_len = len(dict_list)
        keys_to_be_deleted = []
        sizes_and_date_dict = {}
        for table in table_names:
            sizes_and_date_dict = {}
            for i in range(dict_list_len - 1, -1, -1):
                key = dict_list[i]["Key"]
                response = s3.get_object(Bucket=bucket, Key=key)
                response_json = response["Body"].read().decode("utf-8")
                response_list = json.loads(response_json)
                if response_list:
                    last_updated_date = response_list[-1]["last_updated"]
                    if dict_list[i]["Key"][8 : 8 + len(table[0])] == table[0]:
                        if dict_list[i]["Size"] not in sizes_and_date_dict:
                            sizes_and_date_dict[dict_list[i]["Size"]] = [last_updated_date]
                        elif dict_list[i]["Size"] in sizes_and_date_dict:
                            if last_updated_date not in sizes_and_date_dict[dict_list[i]["Size"]]:
                                sizes_and_date_dict[dict_list[i]["Size"]].append(last_updated_date)
                            else:
                                keys_to_be_deleted.append(key)
        for obj_key in keys_to_be_deleted:
            s3.delete_object(Bucket=bucket, Key=obj_key)

        
def move_processed_ingestion_data(s3, bucket=INGESTION_S3_BUCKET_NAME):
    try:
        list_of_files = s3.list_objects_v2(Bucket=bucket, Prefix='updated')
        number_of_files = list_of_files['KeyCount']
        if number_of_files > 0:
            for i in range(number_of_files):
                file = list_of_files['Contents'][i]['Key']
                s3.copy_object(
                    Bucket=bucket,
                    CopySource={'Bucket': bucket, 'Key': file},
                    Key=f'processed_updated/{file[8:]}'
                )
        else:
            logger.info('No files were found in updated')

    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchBucket":
            logger.info("No bucket found")
            raise


def delete_files_from_updated_after_handling(s3, bucket_name=INGESTION_S3_BUCKET_NAME):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix="updated/")
        if "Contents" in response:
            for obj in response["Contents"]:
                file_path = obj["Key"]
                s3.delete_object(Bucket=bucket_name, Key=file_path)
            logger.info(f"moved {response['KeyCount']} files")
        else:
            logger.info("No files to be moved")
    except ClientError as ex:
        if ex.response["Error"]["Code"] == "NoSuchBucket":
            logger.info("No bucket found")
            raise


def lambda_handler(event, context, bucket_name=INGESTION_S3_BUCKET_NAME):
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix='updated/')
    # print(response)
  
    if response['KeyCount'] == 0:
        print('hi')
        logger.info("No new updated data to process")
    if response['KeyCount'] >= 1:
        print('processing')
        # try:

        # delete_duplicates()
        print('dealt with duplicates')
        logger.info('The delete function ran successfully')
        list_of_files = s3.list_objects_v2(
            Bucket=bucket_name, Prefix='updated/')
        
        number_of_files = list_of_files['KeyCount']
        for i in range(number_of_files):
            key_name = list_of_files['Contents'][i]['Key'][8:]
            pattern = re.compile(r'^[A-Za-z]+')
            match = pattern.findall(key_name)
            
            if match == ['sales']:
                df, key = process_fact_sales_order(
                    bucket=INGESTION_S3_BUCKET_NAME)
                convert_to_parquet_put_in_s3(
                    s3, df, key, bucket=PROCESSED_S3_BUCKET_NAME)
                logger.info("sales_order data processed")
                print("processed sales_order")
            if match == ['counterparty']:
                df, key = process_dim_counterparty(
                    bucket=INGESTION_S3_BUCKET_NAME)
                convert_to_parquet_put_in_s3(
                    s3, df, key, bucket=PROCESSED_S3_BUCKET_NAME)
                logger.info("counterparty data processed")
                print("processed counterparty")
            if match == ['currency']:
                
                df, key = process_dim_currency(
                    bucket=INGESTION_S3_BUCKET_NAME)
                convert_to_parquet_put_in_s3(
                    s3, df, key, bucket=PROCESSED_S3_BUCKET_NAME)
                logger.info("currency data processed")
                print("processed currency")
            if match == ['date']:
                df, key = process_dim_date(
                    bucket=INGESTION_S3_BUCKET_NAME)
                convert_to_parquet_put_in_s3(
                    s3, df, key, bucket=PROCESSED_S3_BUCKET_NAME)
                logger.info("date data processed")
                print("processed date")
            if match == ['design']:
                print('a match for design')
                df, key = process_dim_design(
                    bucket=INGESTION_S3_BUCKET_NAME)
                convert_to_parquet_put_in_s3(
                    s3, df, key, bucket=PROCESSED_S3_BUCKET_NAME)
                logger.info("design data processed")
                print("processed design")
            if match == ['location']:
                df, key = process_dim_location(
                    bucket=INGESTION_S3_BUCKET_NAME)
                convert_to_parquet_put_in_s3(
                    s3, df, key,bucket=PROCESSED_S3_BUCKET_NAME)
                logger.info("location data processed")
                print("processed location")
            if match == ['staff']:
                df, key = process_dim_staff(
                    bucket=INGESTION_S3_BUCKET_NAME)
                convert_to_parquet_put_in_s3(
                    s3, df, key, bucket=PROCESSED_S3_BUCKET_NAME)
                logger.info("staff data processed")
                print("processed staff")
            move_processed_ingestion_data(s3, bucket=INGESTION_S3_BUCKET_NAME)
            print("moved data to processed")
            delete_files_from_updated_after_handling(s3, bucket_name=INGESTION_S3_BUCKET_NAME)
        # except Exception as e:
            
        #     logger.error(f"Error in Lambda execution: {e}")
    else:
        logger.info('No files to be processed')


# if __name__ == "__main__":
    # df, key = process_dim_design(bucket=INGESTION_S3_BUCKET_NAME, prefix='updated/')
    # convert_to_parquet_put_in_s3(s3, df, key, bucket=PROCESSED_S3_BUCKET_NAME)
    # event = {}
    # context = {}
    # lambda_handler(event, context, bucket_name=INGESTION_S3_BUCKET_NAME)
    # df, key = process_dim_design(
    #                     bucket=INGESTION_S3_BUCKET_NAME, prefix='updated/')
    # convert_to_parquet_put_in_s3(
    #                     s3, key, df, bucket=PROCESSED_S3_BUCKET_NAME)

# if __name__ == "__main__":
#     process_dim_design(prefix='baseline/')

get_table_names()
get_object_key(table_name="counterparty",
                         bucket=INGESTION_S3_BUCKET_NAME)