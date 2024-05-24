import pytest
from moto import mock_aws
import os
import boto3
from unittest import mock
from unittest.mock import patch
from pg8000 import DatabaseError, InterfaceError
from botocore.exceptions import ClientError
from pprint import pprint
from datetime import datetime
import logging
import json
import pandas as pd
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
    convert_to_parquet_put_in_s3,
    move_processed_ingestion_data,
    delete_files_from_updated_after_handling,
    lambda_handler
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


@pytest.fixture
def bucket(s3):
    s3.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return s3


@pytest.fixture
def process_bucket(s3):
    s3.create_bucket(
        Bucket="process_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    return s3


class TestGetObjectKey:
    @pytest.mark.it("Unit test: returns object key form specified table in s3")
    def test_return_object_key(self, s3, bucket):
        test_body = "hello"
        bucket.put_object(
            Bucket="test_bucket",
            Key="updated/counterparty_test_file_1.json",
            Body=test_body,
        )
        key = get_object_key(
            table_name="counterparty", prefix="updated/", bucket="test_bucket"
        )
        assert key == "updated/counterparty_test_file_1.json"

    @pytest.mark.it("Unit test: raises exception for incorrect prefix")
    def test_incorrect_prefix(self, s3, bucket):
        test_body = "hello"
        bucket.put_object(
            Bucket="test_bucket",
            Key="updated/counterparty_test_file_1.json",
            Body=test_body,
        )

        with pytest.raises(FileNotFoundError):
            get_object_key(
                table_name="counterparty", prefix="wrong_prefix/", bucket="test_bucket"
            )

    @pytest.mark.it("Unit test: raises exception for incorrect table name")
    def test_incorrect_table_name(self, s3, bucket):
        test_body = "hello"
        bucket.put_object(
            Bucket="test_bucket",
            Key="updated/counterparty_test_file_1.json",
            Body=test_body,
        )

        with pytest.raises(FileNotFoundError):
            get_object_key(
                table_name="wrong_table_name", prefix="updated/", bucket="test_bucket"
            )


class TestRemoveCreatedAtAndLastUpdated:
    @pytest.mark.it("Unit test: created_at and last_updated keys removed")
    def test_remove_created_at(self):
        df = pd.DataFrame(
            {
                "address_id": [1],
                "city": ["London"],
                "created_at": ["2022-11-03 14:20:49.962"],
                "last_updated": ["2022-11-03 14:30:41.962"],
            }
        )
        result, key = remove_created_at_and_last_updated(df)
        assert "created_at" not in result
        assert "last_updated" not in result


class TestProcessFactSalesOrder:
    @pytest.mark.it("Unit test: created_date and created_time keys exist")
    def test_created_date_and_time_existed(self, s3, bucket):
        with open(
            "data/test_data_unix_ts/sales_order_unix.json", "r", encoding="utf-8"
        ) as json_file:
            sales_order = json.load(json_file)
            test_body = json.dumps(sales_order["sales_order"])

            bucket.put_object(
                Bucket="test_bucket",
                Key="baseline/sales_order-2022-11-03 14:20:49.962.json",
                Body=test_body,
            )
            fact_sales_order, key = process_fact_sales_order(
                bucket="test_bucket", prefix="baseline/"
            )
            assert "created_date" in fact_sales_order
            assert "created_time" in fact_sales_order
            assert key == "fact/sales_order.parquet"

    @pytest.mark.it("Unit test: last_updated_date and last_updated_time keys exist")
    def test_last_updated_date_and_time_existed(self, s3, bucket):
        with open(
            "data/test_data_unix_ts/sales_order_unix.json", "r", encoding="utf-8"
        ) as json_file:
            sales_order = json.load(json_file)
            test_body = json.dumps(sales_order["sales_order"])

            bucket.put_object(
                Bucket="test_bucket",
                Key="baseline/sales_order-2022-11-03 14:20:49.962.json",
                Body=test_body,
            )
            fact_sales_order, key = process_fact_sales_order(
                bucket="test_bucket", prefix="baseline/"
            )
            assert "last_updated_date" in fact_sales_order
            assert "last_updated_time" in fact_sales_order
            assert key == "fact/sales_order.parquet"

    @pytest.mark.it("Unit test: created_at and last_updated keys removed")
    def test_remove_created_at(self, s3, bucket):
        with open(
            "data/test_data_unix_ts/sales_order_unix.json", "r", encoding="utf-8"
        ) as json_file:
            sales_order = json.load(json_file)
            test_body = json.dumps(sales_order["sales_order"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/sales_order.json", Body=test_body
        )

        result, key = process_fact_sales_order(
            bucket="test_bucket", prefix="baseline/")

        assert "created_date" in result
        assert "created_time" in result
        assert "created_at" not in result
        assert "last_updated" not in result
        assert key == "fact/sales_order.parquet"


class TestProcessDimCounterparty:
    @pytest.mark.it("Unit test: check correct column names")
    def test_check_correct_columns_names(self, s3, bucket):
        with open(
            "data/test_data/counterparty.json", "r", encoding="utf-8"
        ) as json_file:
            counterparty = json.load(json_file)
            test_body = json.dumps(counterparty["counterparty"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/counterparty.json", Body=test_body
        )

        with open("data/test_data/address.json", "r", encoding="utf-8") as json_file:
            address = json.load(json_file)
            test_body = json.dumps(address["address"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/address.json", Body=test_body
        )
        result, key = process_dim_counterparty(
            bucket="test_bucket", prefix="baseline/")

        assert "counterparty_id" in result
        assert "counterparty_legal_name" in result
        assert "counterparty_legal_address_line_1" in result
        assert "counterparty_legal_address_line_2" in result
        assert "counterparty_legal_district" in result
        assert "counterparty_legal_city" in result
        assert "counterparty_legal_postal_code" in result
        assert "counterparty_legal_country" in result
        assert "counterparty_legal_phone_number" in result
        assert key == "dimension/counterparty.parquet"

    @pytest.mark.it(
        "Unit test: commercial_contact, delivery_contact and legal_address_id keys removed"
    )
    def test_remove_commercial_contact(self, s3, bucket):
        with open(
            "data/test_data/counterparty.json", "r", encoding="utf-8"
        ) as json_file:
            counterparty = json.load(json_file)
            test_body = json.dumps(counterparty["counterparty"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/counterparty.json", Body=test_body
        )

        with open("data/test_data/address.json", "r", encoding="utf-8") as json_file:
            address = json.load(json_file)
            test_body = json.dumps(address["address"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/address.json", Body=test_body
        )

        result, key = process_dim_counterparty(
            bucket="test_bucket", prefix="baseline/")

        assert "commercial_contact" not in result
        assert "delivery_contact" not in result
        assert "legal_address_id" not in result
        assert key == "dimension/counterparty.parquet"

    @pytest.mark.it("Unit test: created_at and last_updated keys removed")
    def test_remove_created_at(self, s3, bucket):
        with open(
            "data/test_data/counterparty.json", "r", encoding="utf-8"
        ) as json_file:
            counterparty = json.load(json_file)
            test_body = json.dumps(counterparty["counterparty"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/counterparty.json", Body=test_body
        )

        with open("data/test_data/address.json", "r", encoding="utf-8") as json_file:
            address = json.load(json_file)
            test_body = json.dumps(address["address"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/address.json", Body=test_body
        )

        result, key = process_dim_counterparty(
            bucket="test_bucket", prefix="baseline/")

        assert "created_at" not in result
        assert "last_updated" not in result
        assert key == "dimension/counterparty.parquet"


class TestProcessDimCurrency:
    @pytest.mark.it("Unit test: create currency_name column ")
    def test_currency_name_created(self, s3, bucket):
        with open("data/test_data/currency.json", "r", encoding="utf-8") as json_file:
            currency = json.load(json_file)
            test_body = json.dumps(currency["currency"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/currency.json", Body=test_body
        )

        result, key = process_dim_currency(
            bucket="test_bucket", prefix="baseline/")

        assert "currency_name" in result
        assert key == "dimension/currency.parquet"

    @pytest.mark.it("Unit test: created_at and last_updated keys removed")
    def test_remove_created_at(self, s3, bucket):
        with open("data/test_data/currency.json", "r", encoding="utf-8") as json_file:
            currency = json.load(json_file)
            test_body = json.dumps(currency["currency"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/currency.json", Body=test_body
        )

        result, key = process_dim_currency(
            bucket="test_bucket", prefix="baseline/")

        assert "created_at" not in result
        assert "last_updated" not in result
        assert key == "dimension/currency.parquet"

    @pytest.mark.it("Unit test: check correct column names")
    def test_check_correct_columns_names(self, s3, bucket):
        with open("data/test_data/currency.json", "r", encoding="utf-8") as json_file:
            currency = json.load(json_file)
            test_body = json.dumps(currency["currency"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/currency.json", Body=test_body
        )

        result, key = process_dim_currency(
            bucket="test_bucket", prefix="baseline/")
        expected_columns = ["currency_id", "currency_code", "currency_name"]
        assert list(result.columns) == expected_columns
        assert key == "dimension/currency.parquet"

    @pytest.mark.it("Unit test: check correct data type for columns")
    def test_check_correct_data_type(self, s3, bucket):
        with open("data/test_data/currency.json", "r", encoding="utf-8") as json_file:
            currency = json.load(json_file)
            test_body = json.dumps(currency["currency"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/currency.json", Body=test_body
        )

        result, key = process_dim_currency(
            bucket="test_bucket", prefix="baseline/")

        assert result["currency_id"].dtype == "int64"
        assert result["currency_code"].dtype == "object"
        assert result["currency_name"].dtype == "object"
        assert key == "dimension/currency.parquet"


class TestProcessDimDate:
    @pytest.mark.it("Unit test: check correct column names")
    def test_check_correct_columns_names(self, s3, bucket):
        with open(
            "data/test_data_unix_ts/sales_order_unix.json", "r", encoding="utf-8"
        ) as json_file:
            sales_order = json.load(json_file)
            # add columns fact sales order should also have
            model_f_s_o = sales_order["sales_order"]
            for s_o in model_f_s_o:
                s_o["agreed_payment_date"] = "2024-05-23"
                s_o["agreed_delivery_date"] = "2024-05-24"
            test_body = json.dumps(model_f_s_o)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/'sales_order'.json", Body=test_body
        )

        result, key = process_dim_date(
            bucket="test_bucket", prefix="baseline/")
        expected_columns = [
            "date_id",
            "year",
            "month",
            "day",
            "day_of_week",
            "day_name",
            "month_name",
            "quarter",
        ]
        assert list(result.columns) == expected_columns
        assert key == "dimension/date.parquet"

    @pytest.mark.it("Unit test: check correct data type for columns")
    def test_check_correct_data_type(self, s3, bucket):
        with open(
            "data/test_data_unix_ts/sales_order_unix.json", "r", encoding="utf-8"
        ) as json_file:
            sales_order = json.load(json_file)
            test_body = json.dumps(sales_order["sales_order"])
            # add columns fact sales order should also have
            model_f_s_o = sales_order["sales_order"]
            for s_o in model_f_s_o:
                s_o["agreed_payment_date"] = "2024-05-23"
                s_o["agreed_delivery_date"] = "2024-05-24"
            test_body = json.dumps(model_f_s_o)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/sales_order.json", Body=test_body
        )

        result, key = process_dim_date(
            bucket="test_bucket", prefix="baseline/")

        expected_column_dtypes = {
            "date_id": "object",
            "year": "int64",
            "month": "int64",
            "day": "int64",
            "day_of_week": "int64",
            "day_name": "object",
            "month_name": "object",
            "quarter": "int64",
        }

        for col, expected_dtype in expected_column_dtypes.items():
            assert result[col].dtype == expected_dtype
        assert key == "dimension/date.parquet"


class TestProcessDimDesign:
    @pytest.mark.it("Unit test: created_at and last_updated keys removed")
    def test_remove_created_at(self, s3, bucket):
        with open("data/test_data/design.json", "r", encoding="utf-8") as json_file:
            design = json.load(json_file)
            test_body = json.dumps(design)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/design.json", Body=test_body
        )

        result, key = process_dim_design(
            bucket="test_bucket", prefix="baseline/")

        assert "created_at" not in result
        assert "last_updated" not in result
        assert key == "dimension/design.parquet"


class TestProcessDimLocation:
    @pytest.mark.it("Unit test: rename address_id key to location_id")
    def test_rename_address_id(self, s3, bucket):
        with open("data/test_data/address.json", "r", encoding="utf-8") as json_file:
            address = json.load(json_file)
            test_body = json.dumps(address)
        bucket.put_object(
            Bucket="test_bucket", Key="baseline/address.json", Body=test_body
        )
        result, key = process_dim_location(
            bucket="test_bucket", prefix="baseline/")
        assert "address_id" not in result
        assert "location_id" in result
        assert key == "dimension/location.parquet"

    @pytest.mark.it("Unit test: created_at and last_updated keys removed")
    def test_remove_created_at(self, s3, bucket):
        with open("data/test_data/address.json", "r", encoding="utf-8") as json_file:
            location = json.load(json_file)
            test_body = json.dumps(location)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/address.json", Body=test_body
        )

        result, key = process_dim_location(
            bucket="test_bucket", prefix="baseline/")

        assert "created_at" not in result
        assert "last_updated" not in result
        assert key == "dimension/location.parquet"


class TestProcessDimStaff:
    @pytest.mark.it("Unit test: created_at and last_updated keys removed")
    def test_remove_created_at(self, s3, bucket):
        with open("data/test_data/staff.json", "r", encoding="utf-8") as json_file:
            staff = json.load(json_file)
            test_body = json.dumps(staff)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/staff.json", Body=test_body
        )

        result, key = process_dim_staff(
            bucket="test_bucket", prefix="baseline/")

        assert "created_at" not in result
        assert "last_updated" not in result
        assert key == "dimension/staff.parquet"


class TestConvertDateframeToParquet:
    @pytest.mark.it("Unit test: check returned object is in parquet form")
    def test_check_returned_object(self, s3, bucket, process_bucket):

        with open("data/test_data/staff.json", "r", encoding="utf-8") as json_file:
            staff = json.load(json_file)
            test_body = json.dumps(staff)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/staff.json", Body=test_body
        )

        test_df, key = process_dim_staff(
            bucket="test_bucket", prefix="baseline/")

        convert_to_parquet_put_in_s3(s3, test_df, key, bucket="process_bucket")
        response = s3.list_objects_v2(Bucket="process_bucket")
        assert response["Contents"][0]["Key"] == "dimension/staff.parquet"


class TestMoveProcessedIngestionData:
    @pytest.mark.it("Unit test: Updated files moved to new location")
    def test_updated_files_moved(self, s3):
        s3.create_bucket(Bucket="de-team-orchid-totesys-ingestion", CreateBucketConfiguration={
                         'LocationConstraint': 'eu-west-2', },)

        s3.put_object(
            Body='filetoupload',
            Bucket="de-team-orchid-totesys-ingestion",
            Key='updated/test.txt',
        )
        updated_files = s3.list_objects_v2(
            Bucket="de-team-orchid-totesys-ingestion", Prefix='updated')
        pprint(updated_files)
        move_processed_ingestion_data(s3)
        processed_files = s3.list_objects_v2(
            Bucket="de-team-orchid-totesys-ingestion", Prefix='processed_updated')
        pprint(processed_files)
        assert updated_files['Contents'][0]['Key'][-8:
                                                   ] == processed_files['Contents'][0]['Key'][-8:]

    @pytest.mark.it("unit test: NoSuchBucket exception")
    def test_no_bucket_exceptions(self, caplog, s3):
        with pytest.raises(ClientError):
            move_processed_ingestion_data(s3)
        assert "No bucket found" in caplog.text

    @pytest.mark.it("unit test: No data in updated")
    def test_no_data_updated(self, caplog, s3):
        s3.create_bucket(Bucket="de-team-orchid-totesys-ingestion", CreateBucketConfiguration={
                         'LocationConstraint': 'eu-west-2', },)
        move_processed_ingestion_data(s3)
        assert 'No files were found in updated' in caplog.text


class TestDeleteFilesFromUpdated:

    @pytest.mark.it("unit test: files are deleted from updated")
    def test_files_deleted_after_processing(self, s3):
        s3.create_bucket(Bucket="de-team-orchid-totesys-ingestion", CreateBucketConfiguration={
                         'LocationConstraint': 'eu-west-2', },)

        s3.put_object(
            Body='filetoupload',
            Bucket="de-team-orchid-totesys-ingestion",
            Key='updated/test.txt',
        )
        updated_files = s3.list_objects_v2(
            Bucket="de-team-orchid-totesys-ingestion", Prefix='updated')
        count_of_updated_before = updated_files['KeyCount']
        delete_files_from_updated_after_handling(s3)
        updated_files = s3.list_objects_v2(
            Bucket="de-team-orchid-totesys-ingestion", Prefix='updated')
        count_of_updated_after = updated_files['KeyCount']
        assert count_of_updated_before == 1
        assert count_of_updated_after == 0

    @pytest.mark.it("unit test: NoSuchBucket exception")
    def test_no_bucket_exceptions_deleted(self, caplog, s3):
        with pytest.raises(ClientError):
            delete_files_from_updated_after_handling(s3)
        assert "No bucket found" in caplog.text

    @pytest.mark.it("unit test: No files to be deleted")
    def test_no_files_deleted(self, caplog, s3):
        s3.create_bucket(Bucket="de-team-orchid-totesys-ingestion", CreateBucketConfiguration={
                         'LocationConstraint': 'eu-west-2', },)
        delete_files_from_updated_after_handling(s3)
        assert "No files to be moved" in caplog.text


# check if any contents in updated, & if there isn't: ✔️
# info logger saying nothing to update ✔️
# if there is updated data:
# run the delete function first, to strip duplicate files from updated
# for the remaining contents in updated
# if there's a table called xyz then
# run the function to access updated data for xyz
# elif  abc, then run abc
# elif  def, then run def etc etc
# finally do the copy/delete to clean up updated
# logger.info when all jobs done


class TestProcessingLambdaHandler:

    @pytest.mark.it("unit test: test that no updates gives correct log message")
    def test_no_updates_exits_handler_with_correct_info(self, s3, caplog):
        context = DummyContext()
        event = {}
        s3.create_bucket(Bucket="de-team-orchid-totesys-ingestion", CreateBucketConfiguration={
                         'LocationConstraint': 'eu-west-2', },)
        folder_name = "updated"
        s3.put_object(Bucket="de-team-orchid-totesys-ingestion",
                      Key=(folder_name+'/'))

        lambda_handler(event, context)
        assert (
            "No new updated data to process"
            in caplog.text
        )

    @pytest.mark.it("unit test: correct log info message after delete duplicates ran")
    def test_delete_duplicates_info_message(self, s3, caplog):
        pass
        # context = DummyContext()
        # event = {}

        # s3.create_bucket(Bucket="de-team-orchid-totesys-ingestion", CreateBucketConfiguration={
        #                  'LocationConstraint': 'eu-west-2', },)
        # folder_name = "updated"
        # s3.put_object(Bucket="de-team-orchid-totesys-ingestion",
        #               Key=(folder_name+'/'))
        # s3.put_object(Bucket="de-team-orchid-totesys-ingestion",
        #               Key=("updated/hello.txt"))
        # lambda_handler(event, context)
        # assert ("The Delete function has successfully been ran" in caplog.text)
