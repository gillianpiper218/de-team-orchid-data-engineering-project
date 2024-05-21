import pytest
from moto import mock_aws
import os
import boto3
from pg8000 import DatabaseError, InterfaceError
from pprint import pprint
from datetime import datetime
import logging
import json
from src.processing_lambda import (
    get_object_key,
    remove_created_at_and_last_updated,
    process_fact_sales_order,
    process_dim_counterparty,
    process_dim_currency,
    process_dim_date,
    process_dim_design,
    process_dim_location,
    process_dim_staff,
    convert_json_to_parquet
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


class TestGetObjectKey:
    @pytest.mark.it("Unit test: returns object key form specified table in s3")
    def test_return_object_key(self, s3):
        pass

    @pytest.mark.it("Unit test: raises exception for incorrect prefix")
    def test_incorrect_prefix(self, s3):
        pass

    @pytest.mark.it("Unit test: raises exception for incorrect table name")
    def test_incorrect_table_name(self, s3):
        pass


class TestRemoveCreatedAtAndLastUpdated:
    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3):
        pass

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3):
        pass


class TestProcessFactSalesOrder():
    @pytest.mark.it("Unit test: created_date and created_time keys exist")
    def test_created_date_and_time_existed(self, s3):
        pass

    @pytest.mark.it("Unit test: last_updated_date and last_updated_time keys exist")
    def test_last_updated_date_and_time_existed(self, s3):
        pass
    
    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3):
        pass

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct column names")
    def test_check_correct_columns_names(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct data type for columns")
    def test_check_correct_data_type(self, s3):
        pass


class TestProcessDimCounterparty():
    @pytest.mark.it("Unit test: commercial_contact key removed")
    def test_remove_commercial_contact(self, s3):
        pass

    @pytest.mark.it("Unit test: delivery_contact key removed")
    def test_remove_delivery_contact(self, s3):
        pass

    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3):
        pass

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct column names")
    def test_check_correct_columns_names(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct data type for columns")
    def test_check_correct_data_type(self, s3):
        pass


class TestProcessDimCurrency():
    @pytest.mark.it("Unit test: create currency_name column ")
    def test_currency_name_created(self, s3):
        pass

    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3):
        pass

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct column names")
    def test_check_correct_columns_names(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct data type for columns")
    def test_check_correct_data_type(self, s3):
        pass
        

class TestProcessDimDate():
    @pytest.mark.it("Unit test: check correct column names")
    def test_check_correct_columns_names(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct data type for columns")
    def test_check_correct_data_type(self, s3):
        pass


class TestProcessDimDesign():
    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3):
        pass

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct column names")
    def test_check_correct_columns_names(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct data type for columns")
    def test_check_correct_data_type(self, s3):
        pass

    
class TestProcessDimLocation():
    @pytest.mark.it("Unit test: rename address_id key to location_id")
    def test_rename_address_id(self, s3):
        pass

    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3):
        pass

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct column names")
    def test_check_correct_columns_names(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct data type for columns")
    def test_check_correct_data_type(self, s3):
        pass


class TestProcessDimStaff():
    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3):
        pass

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct column names")
    def test_check_correct_columns_names(self, s3):
        pass

    @pytest.mark.it("Unit test: check correct data type for columns")
    def test_check_correct_data_type(self, s3):
        pass


class TestConvertJsonToParquet():
    @pytest.mark.it("Unit test: check returned object is in parquet form")
    def test_check_returned_object(self, s3):
        pass