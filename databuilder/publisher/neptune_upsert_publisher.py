from csv import DictReader
from os import listdir
from os.path import isfile, join

from gremlin_python.process.traversal import T

from databuilder.clients.neptune_client import NeptuneSessionClient
from databuilder.publisher.base_publisher import Publisher
from databuilder import Scoped


class NeptuneUpsertPublisher(Publisher):

    # A directory that contains CSV files for nodes
    NODE_FILES_DIR = 'node_files_directory'
    # A directory that contains CSV files for relationships
    RELATION_FILES_DIR = 'relation_files_directory'

    # Base amundsen data path
    BASE_AMUNDSEN_DATA_PATH = 'base_amundsen_data_path'

    NEPTUNE_HOST = 'neptune_host'

    # AWS Region
    REGION = 'region'

    AWS_ACCESS_KEY = 'aws_access_key'
    AWS_SECRET_KEY = 'aws_secret_key'
    AWS_SESSION_TOKEN = 'aws_session_token'

    def __init__(self):
        # type: () -> None
        super(NeptuneUpsertPublisher, self).__init__()

    def init(self, conf):
        self.node_files_dir = conf.get_string(NeptuneUpsertPublisher.NODE_FILES_DIR)
        self.relation_files_dir = conf.get_string(NeptuneUpsertPublisher.RELATION_FILES_DIR)

        self.neptune_session_client = NeptuneSessionClient()
        neptune_client_conf = Scoped.get_scoped_conf(conf, self._session_client.get_scope())
        self.neptune_session_client.init(neptune_client_conf)

    def publish_impl(self):
        node_names = [join(self.node_files_dir, f) for f in listdir(self.node_files_dir) if isfile(join(self.node_files_dir, f))]
        edge_names = [join(self.relation_files_dir, f) for f in listdir(self.relation_files_dir) if isfile(join(self.relation_files_dir, f))]

        upserted_node_ids = set()
        for node_file_location in node_names:
            with open(node_file_location, 'r') as file_csv:
                reader = DictReader(file_csv)
                for row in reader:
                    if row['~id'] in upserted_node_ids:
                        continue
                    self.upsert_node_row(row)
                    upserted_node_ids.add(row['~id'])

        for edge_file_location in edge_names:
            with open(edge_file_location, 'r') as file_csv:
                reader = DictReader(file_csv)
                for row in reader:
                    self.upsert_edge_row(row)

    def upsert_node_row(self, row):
        node_properties = {
            key: value
            for key, value in row.items()
            if key not in ['~id', '~label']
        }
        print('Upserting {} {}'.format(row['~label'], row['~id']))
        self.neptune_session_client.upsert_node(
            node_id=row['~id'],
            node_label=row['~label'],
            node_properties=node_properties
        )

    def upsert_edge_row(self, row):
        edge_properties = {
            key: value
            for key, value in row.items()
            if key not in ['~id', '~label', '~to', '~from']
        }
        print('Upserting {} {}'.format(row['~label'], row['~id']))
        self.neptune_session_client.upsert_edge(
            start_node_id=row['~from'],
            end_node_id=row['~to'],
            edge_id=row['~id'],
            edge_label=row['~label'],
            edge_properties=edge_properties
        )

    def get_scope(self):
        # type: () -> str
        return 'publisher.neptune_upsert_publisher'