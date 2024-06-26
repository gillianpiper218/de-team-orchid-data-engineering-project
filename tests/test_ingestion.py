import unittest
from unittest import mock
import pytest
from unittest.mock import patch
from moto import mock_aws
import os
import boto3
from botocore.exceptions import ClientError
from pg8000 import DatabaseError, InterfaceError
from datetime import datetime
import logging
import json
from src.ingestion_lambda import (
    connect_to_db,
    get_table_names,
    select_all_tables_for_baseline,
    select_and_write_updated_data,
    delete_empty_s3_files,
    retrieve_secret_credentials,
    check_baseline_exists,
    lambda_handler,
    copy_baseline_to_updated
)

LOGGER = logging.getLogger(__name__)


class DummyContext:
    pass


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(scope="function")
def secrets_manager_client(aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager")


@pytest.fixture
def bucket(s3):
    s3.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return s3


class TestRetrieveSecretCredentials:
    @pytest.mark.it("unit test: check function retrieves secret")
    def test_retieve_secret(self, secrets_manager_client):
        secrets_manager_client.create_secret(
            Name="test_retrieve",
            SecretString='{"host": "test_host", "password": "test_password", "dbname": "test_db", "port": "test_port", "username": "test_username"}',
        )
        expected = (
            "test_host",
            "test_password",
            "test_db",
            "test_port",
            "test_username",
        )

        with mock.patch("builtins.input", return_value="test_retrieve"):
            result = retrieve_secret_credentials(secret_name="test_retrieve")

        assert result == expected


class TestConnectToDatabase:
    @pytest.mark.it("unit test: check connection to database")
    def test_connect_to_datebase(self, caplog):
        LOGGER.info("Testing now")
        connect_to_db()
        assert "Connected to the database successfully" in caplog.text

    @pytest.mark.it("unit test: check DatabaseError exception")
    def test_database_error_exception(self, caplog):
        LOGGER.info("Testing now")
        with patch("pg8000.connect") as mock_connection:
            mock_connection.side_effect = DatabaseError("Connection timed out")
            with pytest.raises(DatabaseError):
                connect_to_db()
        assert "Error connecting to database: Connection timed out" in caplog.text

    @pytest.mark.it("unit test: check InterfaceError exception")
    def test_interface_error_exception(self, caplog):
        LOGGER.info("Testing now")
        with patch("pg8000.connect") as mock_connection:
            mock_connection.side_effect = InterfaceError("Connection timed out")
            with pytest.raises(InterfaceError):
                connect_to_db()
        assert "Error connecting to the database: Connection timed out" in caplog.text


class TestGetTableNames:
    @pytest.mark.it("unit test: check function returns all tables names")
    def test_returns_table_names(self):
        result = get_table_names()
        table_name_list = [item[0] for item in result]
        assert "address" in table_name_list
        assert "staff" in table_name_list
        assert "currency" in table_name_list
        assert "payment" in table_name_list
        assert "department" in table_name_list
        assert "transaction" in table_name_list
        assert "design" in table_name_list
        assert "sales_order" in table_name_list
        assert "counterparty" in table_name_list
        assert "purchase_order" in table_name_list
        assert "payment_type" in table_name_list

    @pytest.mark.it("unit test: raises DatabaseError")
    def test_raises_DatabaseError(self, caplog):
        LOGGER.info("Testing now")
        with patch("pg8000.connect") as mock_connection:
            mock_connection.side_effect = DatabaseError("Connection timed out")
            with pytest.raises(DatabaseError):
                get_table_names()
        assert "Error connecting to database: Connection timed out" in caplog.text

    @pytest.mark.it("unit test: raises InterfaceError")
    def test_raises_InterfaceError(self, caplog):
        LOGGER.info("Testing now")
        with patch("src.ingestion_lambda.connect_to_db") as mock_connection:
            mock_connection.side_effect = InterfaceError("Connection timed out")
            get_table_names()
            assert (
                "Error connecting to the database: Connection timed out" in caplog.text
            )


class TestSelectAllTablesBaseline:
    @pytest.mark.it("unit test: function writes data to s3 bucket")
    def test_writes_to_s3(self, s3, caplog):
        test_bucket_name = "test_bucket"
        name_of_tables = get_table_names()
        s3.create_bucket(
            Bucket=test_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        response = s3.list_objects_v2(Bucket="test_bucket", Prefix="baseline")
        assert response["KeyCount"] == 0
        select_all_tables_for_baseline(
            bucket_name=test_bucket_name,
            query_limit="2",
            db=connect_to_db(),
        )
        response = s3.list_objects_v2(Bucket="test_bucket", Prefix="baseline")
        assert response["KeyCount"] == len(name_of_tables)
        for i in range(len(name_of_tables)):
            assert response["Contents"][i]["Key"][:9] == "baseline/"
        for i in range(len(name_of_tables)):
            assert response["Contents"][i]["Key"][-5:] == ".json"
        assert "Uploaded file to" in caplog.text

    @pytest.mark.it("unit test: NoSuchBucket exception")
    def test_no_bucket_exceptions(self, caplog):
        with pytest.raises(ClientError):
            test_bucket_name = "test_bucket"
            select_all_tables_for_baseline(
                bucket_name=test_bucket_name,
                query_limit="2",
                db=connect_to_db(),
            )
        assert "No bucket found" in caplog.text


class TestSelectAndWriteUpdatedData:
    @pytest.mark.it("unit test: check last updated is within the last 20 minutes")
    def test_data_within_20_mins(self, s3, secrets_manager_client):
        test_bucket_name = "test_bucket"
        s3.create_bucket(
            Bucket="test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        select_and_write_updated_data(
            name_of_tables=get_table_names(),
            bucket_name="test_bucket",
            query_minutes="real",
        )
        delete_empty_s3_files(bucket_name="test_bucket")
        obj_list = s3.list_objects_v2(Bucket="test_bucket", Prefix="updated")
        if obj_list["KeyCount"] != 0:
            for i in range(len(obj_list["Contents"])):
                object_key = obj_list["Contents"][0]["Key"]
                response = s3.get_object(Bucket=test_bucket_name, Key=object_key)
                contents = response["Body"].read().decode("utf-8")
                data = json.loads(contents)
                for dictionary in data:
                    if dictionary:
                        epoch_time = (dictionary["last_updated"]) / 1000
                        formatted_time = datetime.fromtimestamp(epoch_time)
                        current_time = datetime.now()
                        difference = current_time - formatted_time
                        differene_mins = difference.total_seconds() / 60
                        assert differene_mins < 20
        else:
            assert obj_list["KeyCount"] == 0

    @pytest.mark.it("unit test: NoSuchBucket exception")
    def test_no_bucket_exceptions(self, s3, caplog):
        with pytest.raises(ClientError):
            test_bucket_name = "test_bucket1"
            select_and_write_updated_data(
                bucket_name=test_bucket_name,
                db=connect_to_db(),
                override_time_condition=True,
            )
        assert "No bucket found" in caplog.text


class TestDeleteEmptyS3Files:
    @pytest.mark.it("unit test: Empty files in s3 deleted")
    def test_deleted_files_in_s3(self, s3):
        test_bucket_name = "test_bucket"
        s3.create_bucket(
            Bucket=test_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        test_body = "hello"
        s3.put_object(
            Bucket=test_bucket_name, Key="updated/test_file1.txt", Body=test_body
        )
        s3.put_object(Bucket=test_bucket_name, Key="updated/test_file2.txt")
        response = s3.list_objects_v2(Bucket=test_bucket_name, Prefix="updated/")
        assert response["KeyCount"] == 2
        delete_empty_s3_files(bucket_name=test_bucket_name)
        response = s3.list_objects_v2(Bucket=test_bucket_name)
        assert response["KeyCount"] == 1

    @pytest.mark.it("unit test: NoSuchBucket exception")
    def test_no_bucket_exceptions(self, caplog):
        with pytest.raises(ClientError):
            test_bucket_name = "test_bucket"
            delete_empty_s3_files(bucket_name=test_bucket_name)
        assert "No bucket found" in caplog.text


class TestLambdaHandler:

    @pytest.mark.it("test that connect_to_db success message is logged")
    def test_lambda_handler_logs_connect_to_db_success(self, caplog):
        event = {}
        context = DummyContext()
        lambda_handler(event, context)
        assert "Connected to the database successfully" in caplog.text

    @pytest.mark.it("test that if no baseline we get a message stating no baseline")
    def test_lambda_handler_gives_correct_info_if_no_baseline(self, caplog):
        with patch("src.ingestion_lambda.check_baseline_exists", return_value=False):
            context = DummyContext()
            event = {}
            lambda_handler(event, context)
            assert (
                "Baseline does not exist. Running baseline data extraction."
                in caplog.text
            )

    @pytest.mark.it("test that baseline exists produces correct message")
    def test_lambda_handler_gives_correct_info_if_baseline_exists(self, caplog):
        with patch("src.ingestion_lambda.check_baseline_exists", return_value=True):
            context = DummyContext()
            event = {}
            lambda_handler(event, context)
            assert "Baseline exists. Running updated data extraction."


class TestCopyBaselineToUpdated:
    @pytest.mark.it("Unit test: all files in one folder copied to another folder")
    def test_copy_all_files(self, s3, bucket):
        with open(
            "data/test_data/sales_order.json", "r", encoding="utf-8"
        ) as json_file:
            sales_order = json.load(json_file)
            test_body = json.dumps(sales_order)
            bucket.put_object(
                Bucket="test_bucket",
                Key="baseline/sales_order-2022-11-03 14:20:49.962.json",
                Body=test_body,
            )
        
        with open(
            "data/test_data/currency.json", "r", encoding="utf-8"
        ) as json_file:
            sales_order = json.load(json_file)
            test_body = json.dumps(sales_order)
            bucket.put_object(
                Bucket="test_bucket",
                Key="baseline/currency-2022-11-03 14:20:49.962.json",
                Body=test_body,
            )

        response = bucket.list_objects_v2(Bucket="test_bucket", Prefix="updated/")
        assert response["KeyCount"] == 0

        response = bucket.list_objects_v2(Bucket="test_bucket", Prefix="baseline/")
        assert response["KeyCount"] == 2

        copy_baseline_to_updated(bucket_name="test_bucket")

        response = bucket.list_objects_v2(Bucket="test_bucket", Prefix="updated/")
        assert response["KeyCount"] == 2

        response = bucket.list_objects_v2(Bucket="test_bucket", Prefix="baseline/")
        assert response["KeyCount"] == 2
