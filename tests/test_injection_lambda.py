import pytest
from unittest.mock import Mock, patch
from requests import Response
from moto import mock_aws
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# from freezegun import freeze_time
import logging
import json
from src.injestion_function import (
    connect_to_db,
    get_table_names,
    select_all_tables_for_baseline,
    select_all_updated_rows,
)

LOGGER = logging.getLogger(__name__)

load_dotenv()

# Database connection
DB_HOST = os.environ["DB_HOST"]
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]
DB_PORT = os.environ["DB_PORT"]


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
