import os
import unittest
from typing import Dict
from mock import patch
from pyhocon import ConfigFactory
import requests
from databuilder.publisher.neptune_csv_publisher import NeptuneCSVPublisher
from databuilder.clients.neptune_client import BadBulkUploadException


class MockResponse:
    def __init__(self, status_code: int, data: Dict):
        self.data = data
        self.status_code = status_code

    def json(self):
        return self.data


def bulk_upload_request_response_bad(*positional, **passed_values):
    return MockResponse(
        status_code=503,
        data={
            "status": "200 OK",
            "payload": {
                "loadId": None
            }
        }
    )


def bulk_upload_request_response(*positional, **passed_values):
    return MockResponse(
        status_code=200,
        data={
            "status": "200 OK",
            "payload": {
                "loadId": "guid_as_string"
            }
        }
    )


def request_get_response(*positional, **kwargs):
    return MockResponse(
        status_code=200,
        data={
            "status": "200 OK",
            "payload": {
                "feedCount": [
                    {
                        "LOAD_FAILED": 0,
                        "LOAD_SUCCESS": 1,
                    }
                ],
                "overallStatus": {
                    "fullUri": "s3://bucket/key",
                    "runNumber": 1,
                    "retryNumber": 0,
                    "status": "LOAD_SUCCESS",
                    "totalTimeSpent": 0,
                    "startTime": 1,
                    "totalRecords": 8,
                    "totalDuplicates": 0,
                    "parsingErrors": 0,
                    "datatypeMismatchErrors": 0,
                    "insertErrors": 0,
                },
                "failedFeeds": [],
                "errors": {}
            }
        }
    )


class TestNeptuneCSVPublisher(unittest.TestCase):
    def setUp(self):
        # type: () -> None
        self._resource_path = '{}/../resources/csv_publisher'.format(
            os.path.join(os.path.dirname(__file__))
        )

    @patch('requests.post', side_effect=bulk_upload_request_response)
    @patch('requests.get', side_effect=request_get_response)
    @patch('databuilder.utils.s3_client.upload_file')
    def test_publisher(self, s3_client_mock, request_get_mock, request_post_mock):
        job_config = {
            NeptuneCSVPublisher.NODE_FILES_DIR: '{}/nodes'.format(self._resource_path),
            NeptuneCSVPublisher.RELATION_FILES_DIR: '{}/relations'.format(self._resource_path),
            NeptuneCSVPublisher.BUCKET_NAME: 's3_bucket',
            NeptuneCSVPublisher.BASE_AMUNDSEN_DATA_PATH: 's3_directory',
            NeptuneCSVPublisher.REGION: 'any_zone',
            NeptuneCSVPublisher.AWS_ACCESS_KEY: 'let_me_in',
            NeptuneCSVPublisher.AWS_SECRET_KEY: "123456",
            NeptuneCSVPublisher.AWS_ARN: 'test_arn',
            NeptuneCSVPublisher.NEPTUNE_HOST: 'what_a_great_host',
        }
        conf = ConfigFactory.from_dict(job_config)
        publisher = NeptuneCSVPublisher()
        publisher.init(conf)

        publisher.publish()
        self.assertEqual(s3_client_mock.call_count, 3)
        self.assertEqual(request_get_mock.call_count, 1)
        self.assertEqual(request_post_mock.call_count, 1)

    @patch('requests.post', side_effect=bulk_upload_request_response)
    @patch('requests.get', side_effect=request_get_response)
    @patch('databuilder.utils.s3_client.upload_file')
    def test_publisher(self, s3_client_mock, request_get_mock, request_post_mock):
        job_config = {
            NeptuneCSVPublisher.NODE_FILES_DIR: '{}/nodes'.format(self._resource_path),
            NeptuneCSVPublisher.RELATION_FILES_DIR: '{}/relations'.format(self._resource_path),
            NeptuneCSVPublisher.BUCKET_NAME: 's3_bucket',
            NeptuneCSVPublisher.BASE_AMUNDSEN_DATA_PATH: 's3_directory',
            NeptuneCSVPublisher.REGION: 'any_zone',
            NeptuneCSVPublisher.AWS_ACCESS_KEY: 'let_me_in',
            NeptuneCSVPublisher.AWS_SECRET_KEY: "123456",
            NeptuneCSVPublisher.AWS_ARN: 'test_arn',
            NeptuneCSVPublisher.NEPTUNE_HOST: 'what_a_great_host',
        }
        conf = ConfigFactory.from_dict(job_config)
        publisher = NeptuneCSVPublisher()
        publisher.init(conf)

        publisher.publish()
        self.assertEqual(s3_client_mock.call_count, 3)
        self.assertEqual(request_get_mock.call_count, 1)
        self.assertEqual(request_post_mock.call_count, 1)

    @patch('requests.post', side_effect=bulk_upload_request_response_bad)
    @patch('requests.get', side_effect=request_get_response)
    @patch('databuilder.utils.s3_client.upload_file')
    def test_publisher_no_loader_id(self, s3_client_mock, request_get_mock, request_post_mock):
        job_config = {
            NeptuneCSVPublisher.NODE_FILES_DIR: '{}/nodes'.format(self._resource_path),
            NeptuneCSVPublisher.RELATION_FILES_DIR: '{}/relations'.format(self._resource_path),
            NeptuneCSVPublisher.BUCKET_NAME: 's3_bucket',
            NeptuneCSVPublisher.BASE_AMUNDSEN_DATA_PATH: 's3_directory',
            NeptuneCSVPublisher.REGION: 'any_zone',
            NeptuneCSVPublisher.AWS_ACCESS_KEY: 'let_me_in',
            NeptuneCSVPublisher.AWS_SECRET_KEY: "123456",
            NeptuneCSVPublisher.AWS_ARN: 'test_arn',
            NeptuneCSVPublisher.NEPTUNE_HOST: 'what_a_great_host',
        }
        conf = ConfigFactory.from_dict(job_config)
        publisher = NeptuneCSVPublisher()
        publisher.init(conf)
        with self.assertRaises(BadBulkUploadException):
            publisher.publish()
        self.assertEqual(s3_client_mock.call_count, 3)
        self.assertEqual(request_post_mock.call_count, 1)
        self.assertEqual(request_get_mock.call_count, 0)


if __name__ == '__main__':
    unittest.main()