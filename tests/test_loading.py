# import unittest
# from unittest import mock
# import pytest
# from unittest.mock import patch, Mock, MagicMock
# from moto import mock_aws
# import os
# import boto3
# from botocore.exceptions import ClientError
# from pg8000 import DatabaseError, InterfaceError
# from datetime import datetime
# import logging
# import json
# import pandas as pd
# import pyarrow.parquet as pq
# import pyarrow as pa

# from src.loading_lambda import (
#     connect_to_dw,
#     retrieve_secret_credentials,
#     get_latest_parquet_file_key,
#     read_parquet_from_s3,
#     load_dim_tables,
#     load_fact_table,
#     load_to_data_warehouse,
#     lambda_handler,
# )


# LOGGER = logging.getLogger(__name__)


# class DummyContext:
#     pass


# @pytest.fixture(scope="function")
# def aws_credentials():
#     """Mocked AWS Credentials for moto."""
#     os.environ["AWS_ACCESS_KEY_ID"] = "test"
#     os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
#     os.environ["AWS_SECURITY_TOKEN"] = "test"
#     os.environ["AWS_SESSION_TOKEN"] = "test"
#     os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


# @pytest.fixture(scope="function")
# def mock_s3(aws_credentials):
#     with mock_aws():
#         yield boto3.client("s3", region_name="eu-west-2")


# @pytest.fixture(scope="function")
# def secrets_manager_client(aws_credentials):
#     with mock_aws():
#         yield boto3.client("secretsmanager")


# # adding mock parquet file to put into a mock test bucket in mock aws/s3
# test_bucket = "test_bucket"


# @pytest.fixture(scope="function")
# def mock_s3_bucket(mock_s3):
#     mock_s3.create_bucket(
#         Bucket=test_bucket,
#         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#     )

#     data = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]}).to_parquet()
#     mock_s3.put_object(
#         Bucket=test_bucket,
#         Key="fact/sales_order-2024-05-24 14:35:22.parquet",
#         Body=data,
#     )
#     mock_s3.put_object(
#         Bucket=test_bucket, Key="dimension/date-2024-05-24 14:35:22.parquet", Body=data
#     )

#     yield mock_s3


# class TestRetrieveSecretCredentials:
#     @pytest.mark.it("unit test: check function retrieves secret")
#     def test_retieve_secret(self, secrets_manager_client):
#         secrets_manager_client.create_secret(
#             Name="test_retrieve",
#             SecretString='{"host": "test_host", "password": "test_password", "dbname": "test_db", "port": "test_port", "username": "test_username", "dwschema": "test_schema"}'
#         )
#         expected = (
#             "test_host",
#             "test_password",
#             "test_db",
#             "test_port",
#             "test_username",
#             "test_schema"
#         )

#         with mock.patch("builtins.input", return_value="test_retrieve"):
#             result = retrieve_secret_credentials(secret_name="test_retrieve")

#         assert result == expected


# class TestConnectToDatabase:
#     @pytest.mark.it("unit test: check connection to database")
#     def test_connect_to_datebase(self, caplog):
#         LOGGER.info("Testing now")
#         connect_to_dw()
#         assert "Connected to the database successfully" in caplog.text

#     @pytest.mark.it("unit test: check DatabaseError exception")
#     def test_database_error_exception(self, caplog):
#         LOGGER.info("Testing now")
#         with patch("pg8000.connect") as mock_connection:
#             mock_connection.side_effect = DatabaseError("Connection timed out")
#             with pytest.raises(DatabaseError):
#                 connect_to_dw()
#         assert "Error connecting to database: Connection timed out" in caplog.text

#     @pytest.mark.it("unit test: check InterfaceError exception")
#     def test_interface_error_exception(self, caplog):
#         LOGGER.info("Testing now")
#         with patch("pg8000.connect") as mock_connection:
#             mock_connection.side_effect = InterfaceError("Connection timed out")
#             with pytest.raises(InterfaceError):
#                 connect_to_dw()
#         assert "Error connecting to the database: Connection timed out" in caplog.text


# class TestGetLatestParquetFileKeyWithPatch:
#     # using decorator patch on boto3 client
#     @patch("src.loading_lambda.boto3.client")
#     def test_get_latest_file_key_with_patch(self, mock_boto_3_client):
#         prefix = "dimension/"
#         mock_response = {
#             "IsTruncated": False,
#             "Contents": [
#                 {
#                     "Key": "dimension/file1-2024-05-24 14:35:22.parquet",
#                     "LastModified": datetime(2024, 5, 24, 14, 35, 22),
#                     "ETag": "string",
#                 },
#                 {
#                     "Key": "dimension/file2-2024-05-25 16:35:22.parquet",
#                     "LastModified": datetime(2024, 5, 25, 16, 35, 22),
#                     "ETag": "string",
#                 },
#             ],
#         }
#         # two return values needed, mock boto client is a func call, then for list_obj method
#         mock_boto_3_client.return_value.list_objects_v2.return_value = mock_response
#         result = get_latest_parquet_file_key(prefix, bucket=test_bucket)
#         expected = "dimension/file2-2024-05-25 16:35:22.parquet"
#         assert result == expected

#         mock_boto_3_client.return_value.list_objects_v2.assert_called_once_with(
#             Bucket=test_bucket, Prefix=prefix
#         )

#     @patch("src.loading_lambda.boto3.client")
#     def test_cw_logging_of_exception_error(self, mock_boto_3_client, caplog):
#         prefix = "exceptiontest/"
#         caplog.set_level(logging.ERROR)
#         mock_boto_3_client.return_value.list_objects_v2.side_effect = Exception(
#             "Test Exception"
#         )

#         with pytest.raises(Exception, match="Test Exception"):
#             get_latest_parquet_file_key(prefix=prefix, bucket=test_bucket)
#             assert (
#                 "Error getting the latest parquet file from bucket test_bucket for prefix exceptiontest/"
#                 in caplog.text
#             )


# class TestGetLatestParquetFileKeyWithMockAws:
#     # using mock aws environment
#     def test_get_latest_file_key_with_mock_aws(self, mock_s3_bucket):
#         prefix = "dimension/"
#         latest_file_key = get_latest_parquet_file_key(bucket=test_bucket, prefix=prefix)
#         assert latest_file_key == "dimension/date-2024-05-24 14:35:22.parquet"

#     def test_no_files_found_error(self, mock_s3_bucket):
#         prefix = "nonexistent/"
#         with pytest.raises(FileNotFoundError) as f_exc_info:
#             get_latest_parquet_file_key(prefix=prefix, bucket=test_bucket)
#             assert (
#                 str(f_exc_info)
#                 == "No files have been found from test_bucket for prefix: nonexistent/"
#             )

#     def test_cw_logging_of_no_file_error(self, mock_s3_bucket, caplog):
#         prefix = "nonexistent/"
#         caplog.set_level(logging.ERROR)
#         with pytest.raises(FileNotFoundError):
#             get_latest_parquet_file_key(prefix=prefix, bucket=test_bucket)
#             assert (
#                 "No files have been found from test_bucket for prefix: nonexistent/"
#                 in caplog.text
#             )

#     def test_get_latest_file_key_with_new_file_added_to_bucket(
#         self, mock_s3, mock_s3_bucket
#     ):
#         prefix = "dimension/"
#         mock_s3.put_object(
#             Bucket=test_bucket,
#             Key="dimension/date-2024-05-24 20:05:22.parquet",
#             Body="{}",
#         )
#         result = get_latest_parquet_file_key(prefix, bucket=test_bucket)
#         expected = "dimension/date-2024-05-24 20:05:22.parquet"
#         assert result == expected


# class TestReadParquetFromS3:
#     @pytest.mark.it("unit test: check type of file in s3 is parquet")
#     def test_correct_file_type_of_objects_in_s3(self, mock_s3, mock_s3_bucket):
#         key = "fact/sales_order-2024-05-24 14:35:22.parquet"
#         # get obj metadata
#         response = mock_s3.head_object(Bucket=test_bucket, Key=key)
#         content_type = response["ContentType"]
#         assert content_type == "binary/octet-stream"

#     @pytest.mark.it("unit test: check correct file type after reading pq file from s3")
#     def test_pq_file_can_be_read_from_s3(self, mock_s3_bucket):
#         key = "fact/sales_order-2024-05-24 14:35:22.parquet"
#         result = read_parquet_from_s3(key, bucket=test_bucket)
#         assert isinstance(result, pa.Table)

#     @pytest.mark.it("unit test: test object not found error using wrong key")
#     def test_obj_not_found_wrong_key(self, mock_s3_bucket):
#         key = "wrong_key_fact/sales_order-2024-05-24 14:35:22.parquet"
#         with pytest.raises(ClientError) as ce:
#             read_parquet_from_s3(key, bucket=test_bucket)
#             assert (
#                 "Key: wrong_key_fact/sales_order-2024-05-24 14:35:22.parquet does not exist"
#                 in str(ce.value)
#             )

#     @pytest.mark.it("unit test: check read parquet from s3 func general exception")
#     def test_obj_not_found_general_exc_wrong_bucket_name(self, caplog, mock_s3_bucket):
#         key = "key_fact/sales_order-2024-05-24 14:35:22.parquet"
#         with pytest.raises(Exception):
#             read_parquet_from_s3(key, bucket="should be test_bucket")
#             assert (
#                 "Error reading parquet file from bucket should be test_bucket with key key_fact/sales_order-2024-05-24 14:35:22.parquet"
#                 in caplog.text
#             )


# class TestLoadDimTables:
#     @pytest.mark.it("use patches/mocking to check func is loading six dim tables")
#     @patch("src.loading_lambda.load_to_data_warehouse")
#     @patch("src.loading_lambda.read_parquet_from_s3")
#     @patch(
#         "src.loading_lambda.get_latest_parquet_file_key",
#         return_value="dimension/dim_date-2024-05-24 14:35:22.parquet",
#     )
#     def test_load_dim_tables(
#         self,
#         mock_get_latest_parquet_file_key,
#         mock_read_parquet_from_s3,
#         mock_load_to_data_warehouse,
#     ):

#         load_dim_tables(bucket=test_bucket)

#         # checking funcs called the expected no of times
#         assert mock_get_latest_parquet_file_key.call_count == 6
#         assert mock_read_parquet_from_s3.call_count == 6
#         assert mock_load_to_data_warehouse.call_count == 6


# class TestLoadFactTable:
#     @pytest.mark.it("use patches/mocking to check func is loading one fact tables")
#     @patch("src.loading_lambda.load_to_data_warehouse")
#     @patch("src.loading_lambda.read_parquet_from_s3")
#     @patch(
#         "src.loading_lambda.get_latest_parquet_file_key",
#         return_value="dimension/dim_design-2025-05-24 14:35:22.parquet",
#     )
#     def test_load_fact_table(
#         self,
#         mock_get_latest_parquet_file_key,
#         mock_read_parquet_from_s3,
#         mock_load_to_data_warehouse,
#     ):

#         load_fact_table(bucket=test_bucket)

#         # checking funcs called the expected no of times
#         assert mock_get_latest_parquet_file_key.call_count == 1
#         assert mock_read_parquet_from_s3.call_count == 1
#         assert mock_load_to_data_warehouse.call_count == 1


# class TestLoadToDataWarehouse:
#     @pytest.mark.it("use patch to verify function calling")
#     @patch("src.loading_lambda.connect_to_dw")
#     def test_load_to_data_warehouse_func_calls(self, mock_connect_to_dw):
#         # create mocks for conn and cursor
#         mock_conn = MagicMock()
#         mock_cursor = MagicMock()
#         mock_connect_to_dw.return_value = mock_conn
#         mock_conn.cursor.return_value = mock_cursor

#         # create mock pyarrow table
#         data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
#         table_data = pa.table(data)

#         # call func with mock table_data and table_name
#         load_to_data_warehouse(table_data, "test_table_name")

#         mock_conn.commit.assert_called_once()
#         mock_cursor.close.assert_called_once()
#         mock_conn.close.assert_called_once()

#     @pytest.mark.it("test correct logger info message received")
#     @patch("src.loading_lambda.connect_to_dw")
#     def test_load_to_dw_info_logs(self, mock_connect_to_dw, caplog):
#         # create mocks for conn and cursor
#         mock_conn = MagicMock()
#         mock_cursor = MagicMock()
#         mock_connect_to_dw.return_value = mock_conn
#         mock_conn.cursor.return_value = mock_cursor

#         # create mock pyarrow table
#         data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
#         table_data = pa.table(data)

#         # call func with mock table_data and table_name
#         load_to_data_warehouse(table_data, "test_table_name")
#         expected = "Successfully loaded data into test_table_name"
#         assert expected in caplog.text

#     @pytest.mark.it("test correct logger error message received")
#     @patch("src.loading_lambda.connect_to_dw")
#     def test_load_to_dw_error_logs(self, mock_connect_to_dw, caplog):
#         # create mocks for conn and cursor
#         mock_conn = MagicMock()
#         mock_cursor = MagicMock()
#         mock_connect_to_dw.return_value = mock_conn
#         mock_conn.cursor.return_value = mock_cursor

#         # create mock pyarrow table
#         data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"]}
#         table_data = pa.table(data)

#         # mocks an error
#         mock_cursor.execute.side_effect = Exception("Test Exception")

#         # call func with mock table_data and table_name
#         with pytest.raises(Exception):
#             load_to_data_warehouse(table_data, "test_table_name")
#             err_msg = "Error during loading test_table_name"
#             assert err_msg in caplog.text


# class TestLambdaHandler:
#     @pytest.mark.it("use patch to verify function calling")
#     @patch("src.loading_lambda.load_fact_table")
#     @patch("src.loading_lambda.load_dim_tables")
#     def test_lambda_handler_func_calls(self, mock_dim_tables, mock_fact_table):
#         event = {}
#         context = DummyContext()
#         lambda_handler(event, context)

#         mock_dim_tables.assert_called_once()
#         mock_fact_table.assert_called_once()

#     @pytest.mark.it("test correct logger messages are received")
#     def test_lambda_handler_logs(self, caplog):
#         with patch(
#             "src.loading_lambda.load_dim_tables",
#             side_effect=Exception("Test Exception"),
#         ):
#             context = DummyContext()
#             event = {}
#             with pytest.raises(Exception):
#                 lambda_handler(event, context)
#                 assert "Error in loading lambda execution" in caplog.text
