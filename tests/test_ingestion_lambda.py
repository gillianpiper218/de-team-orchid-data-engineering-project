import pytest
from unittest.mock import Mock, patch
from requests import Response
from moto import mock_aws
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pg8000 import DatabaseError, InterfaceError
from data.test_data.mock_db import mock_table_name_list
from pprint import pprint

# from freezegun import freeze_time
import logging
import json
from src.ingestion_function import (
    connect_to_db,
    get_table_names,
    select_all_tables_for_baseline,
    initial_data_for_latest,
    select_and_write_updated_data,
)

LOGGER = logging.getLogger(__name__)


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


@pytest.fixture
def bucket(s3):
    s3.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


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

    # may need looking at
    @pytest.mark.it("unit test: raises InterfaceError")
    def test_raises_InterfaceError(self, caplog):
        LOGGER.info("Testing now")
        with patch("src.ingestion_function.connect_to_db") as mock_connection:
            mock_connection.side_effect = InterfaceError("Connection timed out")
            # with pytest.raises(InterfaceError):
            get_table_names()
            assert (
                "Error connecting to the database: Connection timed out" in caplog.text
            )


class TestSelectAllTablesBaseline:

    @pytest.mark.it("unit test: function writes data to s3 bucket")
    def test_writes_to_s3(self, s3):
        test_bucket_name = "test_bucket"
        name_of_tables = get_table_names()
        s3.create_bucket(
            Bucket=test_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        select_all_tables_for_baseline(
            bucket_name=test_bucket_name,
            query_limit="2",
            db=connect_to_db(),
        )
        response = s3.list_objects_v2(Bucket="test_bucket", Prefix="baseline")
        assert response["KeyCount"] == len(name_of_tables)
        for i in range(len(name_of_tables)):
            assert (
                response["Contents"][i]["Key"]
                == f"baseline/{name_of_tables[i][0]}.json"
            )

    @pytest.mark.it("unit test: correct data types in s3")
    def test_data_types(self, s3):
        test_bucket_name = "test_bucket"
        name_of_tables = get_table_names()
        s3.create_bucket(
            Bucket=test_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        select_all_tables_for_baseline(
            bucket_name=test_bucket_name,
            query_limit="2",
            db=connect_to_db(),
        )
        for table in name_of_tables:
            response = s3.get_object(
                Bucket=test_bucket_name, Key=f"baseline/{table[0]}.json"
            )
            object_content = response["Body"].read().decode("utf-8")
            # print(object_content + '\n')
            print(object_content)


class TestSelectAllUpdatedRows:

    @pytest.mark.it("unit test: function returns a dictionary")
    def test_returns_updated_dictionary(self):
        pass

    @pytest.mark.it("unit test: dict contains correct keys")
    def test_updated_dict_keys(self):
        pass

    @pytest.mark.it("unit test: correct data types for values")
    def test_updated_dict_values(self):
        pass
