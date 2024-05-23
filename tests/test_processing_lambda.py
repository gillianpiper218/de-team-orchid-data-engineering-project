import pytest
from moto import mock_aws
import os
import boto3
from pg8000 import DatabaseError, InterfaceError
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
    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self):
        df = pd.DataFrame(
            {
                "address_id": [1],
                "city": ["London"],
                "created_at": ["2022-11-03 14:20:49.962"],
                "last_updated": ["2022-11-03 14:30:41.962"],
            }
        )
        result = remove_created_at_and_last_updated(df)
        assert "created_at" not in result

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3):
        df = pd.DataFrame(
            {
                "address_id": [1],
                "city": ["London"],
                "created_at": ["2022-11-03 14:20:49.962"],
                "last_updated": ["2022-11-03 14:30:41.962"],
            }
        )
        result = remove_created_at_and_last_updated(df)
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
                Key="updated/sales_order-2022-11-03 14:20:49.962.json",
                Body=test_body,
            )
            fact_sales_order = process_fact_sales_order(bucket="test_bucket")
            assert "created_date" in fact_sales_order
            assert "created_time" in fact_sales_order

    @pytest.mark.it("Unit test: last_updated_date and last_updated_time keys exist")
    def test_last_updated_date_and_time_existed(self, s3, bucket):
        with open(
            "data/test_data_unix_ts/sales_order_unix.json", "r", encoding="utf-8"
        ) as json_file:
            sales_order = json.load(json_file)
            test_body = json.dumps(sales_order["sales_order"])

            bucket.put_object(
                Bucket="test_bucket",
                Key="updated/sales_order-2022-11-03 14:20:49.962.json",
                Body=test_body,
            )
            fact_sales_order = process_fact_sales_order(bucket="test_bucket")
            assert "last_updated_date" in fact_sales_order
            assert "last_updated_time" in fact_sales_order

    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3, bucket):
        with open(
            "data/test_data_unix_ts/sales_order_unix.json", "r", encoding="utf-8"
        ) as json_file:
            sales_order = json.load(json_file)
            test_body = json.dumps(sales_order["sales_order"])

        bucket.put_object(
            Bucket="test_bucket", Key="updated/sales_order.json", Body=test_body
        )

        result = process_fact_sales_order(bucket="test_bucket")

        assert "created_date" in result
        assert "created_time" in result
        assert "created_at" not in result

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3, bucket):
        with open(
            "data/test_data_unix_ts/sales_order_unix.json", "r", encoding="utf-8"
        ) as json_file:
            sales_order = json.load(json_file)
            test_body = json.dumps(sales_order["sales_order"])

        bucket.put_object(
            Bucket="test_bucket", Key="updated/sales_order.json", Body=test_body
        )

        result = process_fact_sales_order(bucket="test_bucket")

        assert "last_updated_date" in result
        assert "last_updated_time" in result
        assert "last_updated" not in result


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
        result = process_dim_counterparty(bucket="test_bucket")

        assert "counterparty_id" in result
        assert "counterparty_legal_name" in result
        assert "counterparty_legal_address_line_1" in result
        assert "counterparty_legal_address_line_2" in result
        assert "counterparty_legal_district" in result
        assert "counterparty_legal_city" in result
        assert "counterparty_legal_postal_code" in result
        assert "counterparty_legal_country" in result
        assert "counterparty_legal_phone_number" in result

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

        result = process_dim_counterparty(bucket="test_bucket")

        assert "commercial_contact" not in result
        assert "delivery_contact" not in result
        assert "legal_address_id" not in result

    @pytest.mark.it("Unit test: created_at key removed")
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

        result = process_dim_counterparty(bucket="test_bucket")

        assert "created_at" not in result

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3, bucket):
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

        result = process_dim_counterparty(bucket="test_bucket")

        assert "last_updated" not in result


class TestProcessDimCurrency:
    @pytest.mark.it("Unit test: create currency_name column ")
    def test_currency_name_created(self, s3, bucket):
        with open("data/test_data/currency.json", "r", encoding="utf-8") as json_file:
            currency = json.load(json_file)
            test_body = json.dumps(currency["currency"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/currency.json", Body=test_body
        )

        result = process_dim_currency(bucket="test_bucket")

        assert "currency_name" in result

    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3, bucket):
        with open("data/test_data/currency.json", "r", encoding="utf-8") as json_file:
            currency = json.load(json_file)
            test_body = json.dumps(currency["currency"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/currency.json", Body=test_body
        )

        result = process_dim_currency(bucket="test_bucket")

        assert "created_at" not in result

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3, bucket):
        with open("data/test_data/currency.json", "r", encoding="utf-8") as json_file:
            currency = json.load(json_file)
            test_body = json.dumps(currency["currency"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/currency.json", Body=test_body
        )

        result = process_dim_currency(bucket="test_bucket")

        assert "last_updated" not in result

    @pytest.mark.it("Unit test: check correct column names")
    def test_check_correct_columns_names(self, s3, bucket):
        with open("data/test_data/currency.json", "r", encoding="utf-8") as json_file:
            currency = json.load(json_file)
            test_body = json.dumps(currency["currency"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/currency.json", Body=test_body
        )

        result = process_dim_currency(bucket="test_bucket")
        expected_columns = ["currency_id", "currency_code", "currency_name"]
        assert list(result.columns) == expected_columns

    @pytest.mark.it("Unit test: check correct data type for columns")
    def test_check_correct_data_type(self, s3, bucket):
        with open("data/test_data/currency.json", "r", encoding="utf-8") as json_file:
            currency = json.load(json_file)
            test_body = json.dumps(currency["currency"])

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/currency.json", Body=test_body
        )

        result = process_dim_currency(bucket="test_bucket")

        assert result["currency_id"].dtype == "int64"
        assert result["currency_code"].dtype == "object"
        assert result["currency_name"].dtype == "object"


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
            Bucket="test_bucket", Key="updated/'sales_order'.json", Body=test_body
        )

        result = process_dim_date(bucket="test_bucket")
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
            Bucket="test_bucket", Key="updated/sales_order.json", Body=test_body
        )

        result = process_dim_date(bucket="test_bucket")

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


class TestProcessDimDesign:
    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3, bucket):
        with open("data/test_data/design.json", "r", encoding="utf-8") as json_file:
            design = json.load(json_file)
            test_body = json.dumps(design)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/design.json", Body=test_body
        )

        result = process_dim_design(bucket="test_bucket")

        assert "created_at" not in result

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3, bucket):
        with open("data/test_data/design.json", "r", encoding="utf-8") as json_file:
            design = json.load(json_file)
            test_body = json.dumps(design)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/design.json", Body=test_body
        )

        result = process_dim_design(bucket="test_bucket")

        assert "last_updated" not in result


class TestProcessDimLocation:
    @pytest.mark.it("Unit test: rename address_id key to location_id")
    def test_rename_address_id(self, s3, bucket):
        with open("data/test_data/address.json", "r", encoding="utf-8") as json_file:
            address = json.load(json_file)
            test_body = json.dumps(address)
        bucket.put_object(
            Bucket="test_bucket", Key="baseline/address.json", Body=test_body
        )
        result = process_dim_location(bucket="test_bucket")
        assert "address_id" not in result
        assert "location_id" in result

    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3, bucket):
        with open("data/test_data/address.json", "r", encoding="utf-8") as json_file:
            location = json.load(json_file)
            test_body = json.dumps(location)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/address.json", Body=test_body
        )

        result = process_dim_location(bucket="test_bucket")

        assert "created_at" not in result

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3, bucket):
        with open("data/test_data/address.json", "r", encoding="utf-8") as json_file:
            location = json.load(json_file)
            test_body = json.dumps(location)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/address.json", Body=test_body
        )

        result = process_dim_location(bucket="test_bucket")

        assert "last_updated" not in result


class TestProcessDimStaff:
    @pytest.mark.it("Unit test: created_at key removed")
    def test_remove_created_at(self, s3, bucket):
        with open("data/test_data/staff.json", "r", encoding="utf-8") as json_file:
            staff = json.load(json_file)
            test_body = json.dumps(staff)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/staff.json", Body=test_body
        )

        result = process_dim_staff(bucket="test_bucket")

        assert "created_at" not in result

    @pytest.mark.it("Unit test: last_updated key removed")
    def test_remove_last_updated(self, s3, bucket):
        with open("data/test_data/staff.json", "r", encoding="utf-8") as json_file:
            staff = json.load(json_file)
            test_body = json.dumps(staff)

        bucket.put_object(
            Bucket="test_bucket", Key="baseline/staff.json", Body=test_body
        )

        result = process_dim_staff(bucket="test_bucket")

        assert "last_updated" not in result


class TestConvertDateframeToParquet:
    @pytest.mark.it("Unit test: check returned object is in parquet form")
    def test_check_returned_object(self, s3, bucket):
        test_df = pd.DataFrame(
            {
                "address_id": [1],
                "city": ["London"],
                "created_at": ["2022-11-03 14:20:49.962"],
                "last_updated": ["2022-11-03 14:30:41.962"],
            }
        )
        convert_to_parquet_put_in_s3(
            s3, test_df, "dimension/location.parquet", bucket="test_bucket"
        )
        response = s3.list_objects_v2(Bucket="test_bucket")
        assert response["Contents"][0]["Key"] == "dimension/location.parquet"
