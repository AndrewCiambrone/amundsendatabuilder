import datetime

from os import listdir
from os.path import isfile, join

from databuilder.publisher.base_publisher import Publisher
from databuilder.utils import s3_client


class NeptuneCSVPublisher(Publisher):

    # A directory that contains CSV files for nodes
    NODE_FILES_DIR = 'node_files_directory'
    # A directory that contains CSV files for relationships
    RELATION_FILES_DIR = 'relation_files_directory'
    # s3 bucket to upload files to
    BUCKET_NAME = 'bucket_name'
    # Base amundsen data path
    BASE_AMUNDSEN_DATA_PATH = 'base_amundsen_data_path'

    def __init__(self):
        # type: () -> None
        super(NeptuneCSVPublisher, self).__init__()

    def init(self, conf):
        self.bucket_name = conf.get_string(NeptuneCSVPublisher.BUCKET_NAME)
        self.base_amundsen_data_path = conf.get_string(NeptuneCSVPublisher.BASE_AMUNDSEN_DATA_PATH)
        self.node_files_dir = conf.get_string(NeptuneCSVPublisher.NODE_FILES_DIR)
        self.relation_files_dir = conf.get_string(NeptuneCSVPublisher.RELATION_FILES_DIR)

    def publish_impl(self):
        self.upload_files(self.node_files_dir)
        self.upload_files(self.relation_files_dir)

    def upload_files(self, directory):
        file_names = [join(directory, f) for f in listdir(directory) if isfile(join(directory, f))]
        datetime_portion = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M")
        for file_name in file_names:
            with open(file_name, 'r', encoding='utf8') as file_csv:
                file_path = "{base_directory}/{datetime_portion}/{file_name}".format(
                    base_directory=self.base_amundsen_data_path,
                    datetime_portion=datetime_portion,
                    file_name=file_name
                )
                s3_client.upload_file(self.BUCKET_NAME, file_path, file_csv)

    def get_scope(self):
        # type: () -> str
        return 'publisher.neptune_csv_publisher'