import pytest
from moto import mock_aws  # type: ignore
import boto3  # type: ignore
import os
from src.lambda_ellen import retrieve_secret_credentials
from unittest import mock


@ pytest.fixture(scope='class')
def aws_creds():
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@ pytest.fixture(scope='function')
def secrets_manager_client(aws_creds):
    with mock_aws():
        yield boto3.client('secretsmanager')


class TestRetrieveSecret:
    @pytest.mark.it('unit test: check function retrieves secret')
    def test_retieve_secret(self, secrets_manager_client):
        secrets_manager_client.create_secret(Name="test_retrieve",
                                             SecretString='{"host": "test_host", "password": "test_password", "dbname": "test_db", "port": "test_port", "username": "test_username"}')
        expected = ('test_host', 'test_password',
                    'test_db', 'test_port', 'test_username')

        with mock.patch('builtins.input', return_value='test_retrieve'):
            result = retrieve_secret_credentials(secret_name='test_retrieve')

        assert result == expected

    # @ pytest.mark.it('unit test:returns error message for non-existent secret')
    # def test_does_not_retieve_secret(self, secrets_manager_client):
    #     with mock.patch('builtins.input', return_value='hello'):
    #         result = retrieve_secret_credentials()
    #     expected = 'The secret hello does not exist'

    #     assert result == expected
