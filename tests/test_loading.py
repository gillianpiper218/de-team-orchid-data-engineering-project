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


from src.loading_lambda import (
    connect_to_dw,
    retrieve_secret_credentials,
    get_latest_parquet_file)


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


@pytest.fixture(scope="function")
def secrets_manager_client(aws_credentials):
    with mock_aws():
        yield boto3.client("secretsmanager")


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
        connect_to_dw()
        assert "Connected to the database successfully" in caplog.text

    @pytest.mark.it("unit test: check DatabaseError exception")
    def test_database_error_exception(self, caplog):
        LOGGER.info("Testing now")
        with patch("pg8000.connect") as mock_connection:
            mock_connection.side_effect = DatabaseError("Connection timed out")
            with pytest.raises(DatabaseError):
                connect_to_dw()
        assert "Error connecting to database: Connection timed out" in caplog.text

    @pytest.mark.it("unit test: check InterfaceError exception")
    def test_interface_error_exception(self, caplog):
        LOGGER.info("Testing now")
        with patch("pg8000.connect") as mock_connection:
            mock_connection.side_effect = InterfaceError("Connection timed out")
            with pytest.raises(InterfaceError):
                connect_to_dw()
        assert "Error connecting to the database: Connection timed out" in caplog.text


class TestGetLatestParquetFile:
    @pytest.mark.it("Unit test: returns the latest parquet file in s3")
    def test_get_latest_files(self, s3):
        test_body = "hello"
        bucket = 'test-bucket'
        prefix = 'fact/sales_order/'
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.create_bucket(Bucket=bucket)
        files = [
            'fact/sales_order/file_20240101.parquet',
            'fact/sales_order/file_20240102.parquet',
            'fact/sales_order/file_20240103.parquet'
        ]
        for file in files:
            s3.put_object(Bucket=bucket, Key=file, Body=test_body)
        latest_file = get_latest_parquet_file(bucket, prefix)
        assert latest_file == ['fact/sales_order/file_20240103.parquet']
   