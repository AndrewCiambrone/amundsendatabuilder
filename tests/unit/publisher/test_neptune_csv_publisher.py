import os
import unittest
import uuid

from mock import patch, MagicMock
from neo4j import GraphDatabase
from pyhocon import ConfigFactory
import boto3
from botocore.stub import Stubber
import requests

from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neptune_csv_publisher import NeptuneCSVPublisher


def request_post_response(**passed_values):
    print(passed_values)


def request_get_response(**passed_values):
    print(passed_values)


class TestNeptuneCSVPublisher(unittest.TestCase):
    def setUp(self):
        # type: () -> None
        self._resource_path = '{}/../resources/csv_publisher'.format(
            os.path.join(os.path.dirname(__file__))
        )
        client = boto3.client('s3')
        self.s3_stub = Stubber(client)
        self.request_post_patcher = patch.object(requests, 'post', request_post_response)
        self.request_post_patcher.start()

        self.request_get_patcher = patch.object(requests, 'get', request_get_response)
        self.request_get_patcher.start()

    def tearDown(self) -> None:
        self.request_post_patcher.stop()
        self.request_get_patcher.stop()

    def test_publisher(self):
        pass




if __name__ == '__main__':
    unittest.main()