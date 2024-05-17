# PSEUDO CODE FOR TESTS


# TEST update_latest_with_new_record
# test that we can list files in staging
# test that empty files in staging are not appended to list (might not need this one)
# test that all files in staging with content are appended to list
# test that the file name format is correct (ie doesn't include /staging)


# test that a log entry is made if all staging files are empty (might not need this one)

# TEST output from update_latest_with_new_record
# prove that the col_id_name returns the correct value in the correct format
# prove that we can access the biggest id number
# prove that we can identify records in staging that are bigger than the biggest id number in latest
# test that all of the correct records are added to latest
# test that the appended data is in the correct format
# test that the original staging file is not altered
# test that the existing data in the latest file is not altered (ie it's an append not an overwrite)
# test that all of the id numbers in the latest file are still unique
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

import logging
import json

from src.ingestion_function import get_s3_object_data, update_latest_with_new_record

from data.test_data.mock_db import mock_db_data

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


# TEST get_s3_object_data()
# test that the correct file is picked up - create a mock file inside mock aws & check that it is accessed
# test that data returned is in correct format - in a dict
# test that non-existent key is handled - that an exception is raised.


# class TestGetS3ObjectData:
#     def test_that_the_correct_file_is_picked_up(self):
#         # table_names = get_table_names()
#         test_bucket_name = "test_bucket"

#         s3.create_bucket(
#             Bucket=test_bucket_name,
#             CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#         )
#         test_body = mock_db_data
#         test_data = json.dumps(test_body)

#         s3.put_object(Bucket=test_bucket_name, Key=test_data, Body=test_body)
