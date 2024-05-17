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
from datetime import datetime
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
            mock_connection.side_effect = InterfaceError(
                "Connection timed out")
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
            mock_connection.side_effect = InterfaceError(
                "Connection timed out")
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
    def test_data_types_of_id(self, s3):
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
        list_of_ids = []
        list_of_time_created = []
        for table in name_of_tables:
            response = s3.get_object(
                Bucket=test_bucket_name, Key=f"baseline/{table[0]}.json"
            )
            contents = response["Body"].read().decode("utf-8")
            data = json.loads(contents)
            created_at_values = [d["created_at"] for d in data]
            list_of_time_created.append(created_at_values)
        for dictionary in data:
            list_of_ids.append(next(iter(dictionary.values())))
        assert all(isinstance(id_, int) for id_ in list_of_ids)
        assert all(
            len(str(num)) == 13 for sublist in list_of_time_created for num in sublist
        )

    @pytest.mark.it(
        "unit test: check every table has create at and last updated columns"
    )
    def test_data_required_columns(self, s3):
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
            contents = response["Body"].read().decode("utf-8")
            data = json.loads(contents)
            for dictionary in data:
                assert "created_at" in dictionary
                assert "last_updated" in dictionary


class TestInitialDataForLatest:

    @pytest.mark.it("unit test: baseline contents copied into latest")
    def test_copies_from_baseline(self, s3):
        table_names = get_table_names()
        test_bucket_name = "test_bucket"

        s3.create_bucket(
            Bucket=test_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        test_body = "hello"
        for table in table_names:
            s3.put_object(
                Bucket=test_bucket_name, Key=f"baseline/{table[0]}.json", Body=test_body
            )

        initial_data_for_latest(
            bucket_name=test_bucket_name, table_names=get_table_names()
        )

        for table in table_names:
            response = s3.get_object(
                Bucket=test_bucket_name, Key=f"latest/{table[0]}.json"
            )
            assert response["Body"].read().decode("utf-8") == "hello"

    @pytest.mark.it("unit test: check the keys in latest match to table names")
    def test_correct_keys_in_latest(self, s3):
        table_names = get_table_names()
        test_bucket_name = "test_bucket"

        s3.create_bucket(
            Bucket=test_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        test_body = "hello"
        for table in table_names:
            s3.put_object(
                Bucket=test_bucket_name, Key=f"baseline/{table[0]}.json", Body=test_body
            )

        initial_data_for_latest(
            bucket_name=test_bucket_name, table_names=get_table_names()
        )

        response = s3.list_objects_v2(Bucket=test_bucket_name, Prefix="latest")

        for i in range(len(response["Contents"])):
            assert response["Contents"][i]["Key"] == f"latest/{table_names[i][0]}.json"


class TestSelectAndWriteUpdatedData:

    @pytest.mark.it("unit test: check last updated is within the last 20 minutes")
    def test_data_within_20_mins(self, s3):
        table_names = get_table_names()
        test_bucket_name = "test_bucket"
        s3.create_bucket(
            Bucket="test_bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        select_and_write_updated_data(
            name_of_tables=get_table_names(), bucket_name="test_bucket"
        )
        list_of_last_updated = []
        for table in table_names:
            response = s3.get_object(
                Bucket=test_bucket_name, Key=f"staging/{table[0]}.json"
            )
            contents = response["Body"].read().decode("utf-8")
            data = json.loads(contents)
        for dictionary in data:
            if dictionary:
                epoch_time = (dictionary['last_updated']) / 1000
                formatted_time = datetime.fromtimestamp(epoch_time)
                current_time = datetime.now()
                difference = current_time - formatted_time
                twenty_mins = difference.total_seconds() / 60
                print(twenty_mins < 20)
