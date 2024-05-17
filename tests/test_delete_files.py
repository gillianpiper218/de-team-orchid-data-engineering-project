import pytest
from unittest.mock import Mock, patch
from requests import Response
from moto import mock_aws
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from pg8000 import DatabaseError, InterfaceError
import logging
import json
from src.ingestion_function import delete_empty_s3_files

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


class TestDeleteFiles():

    @pytest.mark.it("unit test: Delete empty s3 files")
    def test_delete_empty_files(self, caplog):
        LOGGER.info("Testing now")
        # response = s3.list_objects_v2(Bucket="test_bucket", Prefix="staging/")
        s3_mock = Mock()
        s3_mock.list_objects_v2.return_value = {

        }

    @pytest.mark.it("unit test: No empty files")
    @pytest.mark.it("unit test: Error deleting empty files")
