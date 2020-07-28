import datetime

from csv import DictReader
import os
from os import listdir
from os.path import isfile, join

from databuilder.publisher.base_publisher import Publisher
from databuilder.utils import s3_client
from databuilder import neptune_client


class NeptuneCSVPublisher(Publisher):

    # A directory that contains CSV files for nodes
    NODE_FILES_DIR = 'node_files_directory'
    # A directory that contains CSV files for relationships
    RELATION_FILES_DIR = 'relation_files_directory'

    # Base amundsen data path
    BASE_AMUNDSEN_DATA_PATH = 'base_amundsen_data_path'

    NEPTUNE_ENDPOINT = 'neptune_endpoint'
    NEPTUNE_PORT = 'neptune_port'

    # AWS Region
    REGION = 'region'

    AWS_ACCESS_KEY = 'aws_access_key'
    AWS_SECRET_KEY = 'aws_secret_key'

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

        self.neptune_endpoint = conf.get_string(NeptuneCSVPublisher.NEPTUNE_ENDPOINT)
        self.neptune_port = conf.get_int(NeptuneCSVPublisher.NEPTUNE_PORT)

    def publish_impl(self):
        node_names = [join(self.node_files_dir, f) for f in listdir(self.node_files_dir) if isfile(join(self.node_files_dir, f))]
        edge_names = [join(self.relation_files_dir, f) for f in listdir(self.relation_files_dir) if isfile(join(self.relation_files_dir, f))]

        for node_file_location in node_names:
            with open(node_file_location, 'rb') as file_csv:
                reader = DictReader(file_csv)
                for row in reader:
                    self.upsert_node_row(row)

        for edge_file_location in edge_names:
            with open(edge_file_location, 'rb') as file_csv:
                reader = DictReader(file_csv)
                for row in reader:
                    self.upsert_edge_row(row)

    def upsert_node_row(self, row):
        create_transversal_query = "addV({label})".format(
            label=row['~label']
        )
        for key, value in row.items():
            key_split = key.split(':')
            key = key_split.split(':')[0]
            value_type = key_split.split(':')[1]
            if key == '~id':
                create_transversal_query = create_transversal_query + ".property('id', '{node_id}')".format(
                    node_id=row.get('~id')
                )
            elif key == '~label':
                pass
            else:
                if value_type == "String":
                    value = "'{value}'".format(value=value)
                create_transversal_query = create_transversal_query + ".property('{key}', {value})".format(
                    key=key,
                    value=value
                )

        gremlin_query = """
            g.V().has('{node_label}', 'id', '{node_id}').
                fold().
                coalesce(unfold(), {create_transversal}
        """.format(
            node_id=row.get('~id'),
            node_label=row.get('~label'),
            create_transversal=create_transversal_query
        )
        neptune_client.query_with_gremlin(
            neptune_endpoint=self.neptune_endpoint,
            neptune_port=self.neptune_port,
            region=self.aws_region,
            access_key=self.aws_access_key,
            secret_key=self.aws_secret_key,
            gremlin_query=gremlin_query
        )

    def upsert_edge_row(self, row):
        create_query = "V('{from_node_id}').addE('{label}').to(V('{to_node_id}')).property('id', '{edge_id}'".format(
            from_node_id=row['~from'],
            label=row['~label'],
            to_node_id=row['~to'],
            edge_id=row['~id']
        )
        property_portion = ''
        for key, value in row.items():
            key_split = key.split(':')
            key = key_split.split(':')[0]
            value_type = key_split.split(':')[1]

            if value_type == "String":
                value = "'{value}'".format(value=value)

            if key not in ['~id', '~from', '~to', '~label']:
                property_portion = property_portion + ".property('{key}', {value})".format(
                    key=key,
                    value=value
                )
        gremlin_query = """
            g.V('{from_node_id}').outE('{label}').hasId('{edge_id}').fold().coalesce(unfold(), {create_query}){property_portion}
        """.format(
            from_node_id=row['~from'],
            label=row['~label'],
            edge_id=row['~id'],
            create_query=create_query,
            property_portion=property_portion
        )
        neptune_client.query_with_gremlin(
            neptune_endpoint=self.neptune_endpoint,
            neptune_port=self.neptune_port,
            region=self.aws_region,
            access_key=self.aws_access_key,
            secret_key=self.aws_secret_key,
            gremlin_query=gremlin_query
        )

    def get_scope(self):
        # type: () -> str
        return 'publisher.neptune_upsert_publisher'