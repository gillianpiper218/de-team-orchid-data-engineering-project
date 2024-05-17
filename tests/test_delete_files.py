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
from src.ingestion_function import delete_empty_s3_files, get_table_names
from pprint import pprint

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
