import datetime

import os
import time
from os import listdir
from os.path import isfile, join

from databuilder.publisher.base_publisher import Publisher
from databuilder.utils import s3_client
from databuilder.clients.neptune_client import BulkUploaderNeptuneClient


class NeptuneCSVPublisher(Publisher):

    # A directory that contains CSV files for nodes
    NODE_FILES_DIR = 'node_files_directory'
    # A directory that contains CSV files for relationships
    RELATION_FILES_DIR = 'relation_files_directory'
    # s3 bucket to upload files to
    BUCKET_NAME = 'bucket_name'
    # Base amundsen data path
    BASE_AMUNDSEN_DATA_PATH = 'base_amundsen_data_path'

    NEPTUNE_HOST = 'neptune_host'

    # AWS Region
    REGION = 'region'

    AWS_ACCESS_KEY = 'aws_access_key'
    AWS_SECRET_KEY = 'aws_secret_key'
    AWS_SESSION_TOKEN = 'aws_session_token'
    AWS_ARN = 'aws_arn'

    def __init__(self):
        # type: () -> None
        super(NeptuneCSVPublisher, self).__init__()

    def init(self, conf):

        self.node_files_dir = conf.get_string(NeptuneCSVPublisher.NODE_FILES_DIR)
        self.relation_files_dir = conf.get_string(NeptuneCSVPublisher.RELATION_FILES_DIR)

        self.bucket_name = conf.get_string(NeptuneCSVPublisher.BUCKET_NAME)
        self.base_amundsen_data_path = conf.get_string(NeptuneCSVPublisher.BASE_AMUNDSEN_DATA_PATH)
        self.aws_region = conf.get_string(NeptuneCSVPublisher.REGION)
        self.aws_access_key = conf.get_string(NeptuneCSVPublisher.AWS_ACCESS_KEY)
        self.aws_secret_key = conf.get_string(NeptuneCSVPublisher.AWS_SECRET_KEY)
        self.aws_session_token = conf.get_string(NeptuneCSVPublisher.AWS_SESSION_TOKEN, default=None)
        self.aws_arn = conf.get_string(NeptuneCSVPublisher.AWS_ARN)

        self.neptune_host = conf.get_string(NeptuneCSVPublisher.NEPTUNE_HOST)

    def publish_impl(self):
        if not self._is_upload_required():
            return
        
        datetime_portion = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        s3_folder_location = "{base_directory}/{datetime_portion}".format(
            base_directory=self.base_amundsen_data_path,
            datetime_portion=datetime_portion,
        )

        self.upload_files(s3_folder_location)
        bulk_loader_client = BulkUploaderNeptuneClient(
            neptune_host=self.neptune_host,
            region=self.aws_region,
            access_key=self.aws_access_key,
            access_secret=self.aws_secret_key,
            arn=self.aws_arn,
            session_token=self.aws_session_token
        )

        bulk_upload_id = bulk_loader_client.make_bulk_upload_request(
            bucket=self.bucket_name,
            s3_folder_location=s3_folder_location,
        )
        print("bulk loading id is: {}".format(bulk_upload_id))

        is_complete, status = bulk_loader_client.is_bulk_status_job_done(
            load_id=bulk_upload_id,
        )
        while not is_complete:
            time.sleep(5)
            is_complete, status = bulk_loader_client.is_bulk_status_job_done(
                load_id=bulk_upload_id,
            )

        print(status)

    def _is_upload_required(self):
        return len(self.node_files_dir) or len(self.relation_files_dir)

    def upload_files(self, s3_folder_location):
        node_names = [join(self.node_files_dir, f) for f in listdir(self.node_files_dir) if isfile(join(self.node_files_dir, f))]
        edge_names = [join(self.relation_files_dir, f) for f in listdir(self.relation_files_dir) if isfile(join(self.relation_files_dir, f))]
        file_names = node_names + edge_names
        print("Uploading {} files to s3".format(len(file_names)))
        for file_location in file_names:
            with open(file_location, 'rb') as file_csv:
                file_name = os.path.basename(file_location)
                file_path = "{s3_folder_location}/{file_name}".format(
                    s3_folder_location=s3_folder_location,
                    file_name=file_name
                )
                s3_client.upload_file(self.bucket_name, file_path, file_csv)

    def get_scope(self):
        # type: () -> str
        return 'publisher.neptune_csv_publisher'