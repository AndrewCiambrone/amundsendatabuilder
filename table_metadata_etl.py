import os
import textwrap
import uuid
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base
from argparse import ArgumentParser

from databuilder.extractor.neptune_extractor import NeptuneExtractor
from databuilder.transformer.base_transformer import NoopTransformer
from databuilder.extractor.neptune_search_data_extractor import NeptuneSearchDataExtractor
from databuilder.extractor.postgres_metadata_extractor import PostgresMetadataExtractor
from databuilder.extractor.restapi.github_file_extractor import GithubFileExtractor
from databuilder.extractor.dashboard.looker.looker_view_descriptions_extractor import LookerViewDescriptionsExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.job.job import DefaultJob
from databuilder.task.task import DefaultTask
from databuilder.loader.file_system_neptune_csv_loader import FSNeptuneCSVLoader
from databuilder.publisher.neptune_csv_publisher import NeptuneCSVPublisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.task.merge_task import MergeTask, ChildMergeTaskExtractorWrapper
from databuilder.models.table_metadata import TableMetadata, ColumnMetadata, DescriptionMetadata

Base = declarative_base()


def connection_string():
    user = os.getenv('DW_USER')
    password = os.getenv('DW_PASS')
    host = os.getenv('DW_URL')
    port = '5439'
    db = os.getenv('DW_DB')
    return "postgresql://%s:%s@%s:%s/%s" % (user, password, host, port, db)


def create_redshift_extraction_job_with_looker(schema="public"):

    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships/'.format(tmp_folder=tmp_folder)
    s3_bucket = os.getenv('S3_BUCKET')
    s3_directory = "amundsen"
    access_key = os.getenv('AWS_KEY')
    access_secret = os.getenv('AWS_SECRET_KEY')
    aws_zone = os.getenv("AWS_ZONE")
    neptune_endpoint = os.getenv('NEPTUNE_ENDPOINT')
    neptune_port = os.getenv("NEPTUNE_PORT")
    neptune_host = "{}:{}".format(neptune_endpoint, neptune_port)

    looker_repo_name = os.getenv('LOOKER_REPO_NAME')
    looker_views_base_directory = os.getenv('LOOKER_VIEWS_BASE_DIRECTORY')
    github_user_name = os.getenv('GITHUB_USER_NAME')
    github_access_token = os.getenv('GITHUB_ACCESS_TOKEN')
    github_org = os.getenv('GITHUB_ORG')


    where_clause_suffix = textwrap.dedent("""
        where table_schema = '{schema}'
    """.format(schema=schema))

    job_config = ConfigFactory.from_dict({
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.REPO_DIRECTORY): looker_views_base_directory,
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.REPO_NAME): looker_repo_name,
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.GITHUB_USER_NAME): github_user_name,
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.GITHUB_ACCESS_TOKEN): github_access_token,
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.GITHUB_ORG_NAME): github_org,
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.EXPECTED_FILE_EXTENSIONS): ['.view.lkml'],
        'extractor.looker_view_descriptions_extractor.{}'.format(LookerViewDescriptionsExtractor.LOOKER_TABLE_SOURCE_CLUSTER): os.getenv('DW_DB'),
        'extractor.looker_view_descriptions_extractor.{}'.format(LookerViewDescriptionsExtractor.LOOKER_TABLE_SOURCE_DATABASE): 'postgres',
        'extractor.postgres_metadata.{}'.format(PostgresMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY): where_clause_suffix,
        'extractor.postgres_metadata.{}'.format(PostgresMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME): True,
        'extractor.postgres_metadata.{}'.format(PostgresMetadataExtractor.DESCRIPTIONS_SOURCE): "data-warehouse",
        'extractor.postgres_metadata.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING): connection_string(),
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.NODE_DIR_PATH): node_files_folder,
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.RELATION_DIR_PATH): relationship_files_folder,
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.SHOULD_DELETE_CREATED_DIR): False,
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.FORCE_CREATE_DIR): True,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.NODE_FILES_DIR): node_files_folder,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.RELATION_FILES_DIR): relationship_files_folder,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.BUCKET_NAME): s3_bucket,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.BASE_AMUNDSEN_DATA_PATH): s3_directory,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.REGION): aws_zone,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.AWS_ACCESS_KEY): access_key,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.AWS_SECRET_KEY): access_secret,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.NEPTUNE_HOST): neptune_host
    })
    get_description_metadata_owner_key = lambda description: description.description_owner_key
    get_description_metadata_description = lambda description: description._text
    get_description_metadata_source = lambda description: description._source
    get_column_keys_on_table_metadata = lambda table_metadata: [table_metadata._get_col_key(column) for column in table_metadata.columns]

    def get_column_description(table_metadata, column_id):
        # type: (TableMetadata, str) -> str
        column = [column for column in table_metadata.columns if table_metadata._get_col_key(column) == column_id][0]
        if column.description is None:
            return None
        return column.description._text

    def get_column_description_source(table_metadata, column_id):
        # type: (TableMetadata, str) -> str
        column = [column for column in table_metadata.columns if table_metadata._get_col_key(column) == column_id][0]
        if column.description is None:
            return None
        return column.description._source

    def set_column_description(table_metadata, column_id, description):
        # type: (TableMetadata, str) -> str
        column = [column for column in table_metadata.columns if table_metadata._get_col_key(column) == column_id][0]
        if column.description is None:
            column.description = DescriptionMetadata(
                source="looker",
                text=description,
                description_owner_key=table_metadata._get_col_key(column)
            )
        else:
            column.description._text = description

    def set_column_description_source(table_metadata, column_id, source):
        # type: (TableMetadata, str) -> str
        column = [column for column in table_metadata.columns if table_metadata._get_col_key(column) == column_id][0]
        if column.description is None:
            column.description = DescriptionMetadata(
                source="looker",
                text=None,
                description_owner_key=table_metadata._get_col_key(column)
            )
        else:
            column.description._source = source


    child_extractor_wrappers = [
        ChildMergeTaskExtractorWrapper(
            extractor=LookerViewDescriptionsExtractor(),
            key_mapper=get_description_metadata_owner_key,
            property_name_to_getter_mappers={
                'description': get_description_metadata_description,
                'source': get_description_metadata_source
            }
        )
    ]
    parent_record_property_name_getters = {
        'description': get_column_description,
        'source': get_column_description_source
    }

    parent_record_property_name_setters = {
        'description': set_column_description,
        'source': set_column_description_source
    }

    job = DefaultJob(
        conf=job_config,
        task=MergeTask(
            parent_extractor=PostgresMetadataExtractor(),
            parent_record_identifiers=get_column_keys_on_table_metadata,
            parent_record_property_name_getters=parent_record_property_name_getters,
            parent_record_property_name_setters=parent_record_property_name_setters,
            child_extractor_wrappers=child_extractor_wrappers,
            loader=FSNeptuneCSVLoader()
        ),
        publisher=NeptuneCSVPublisher()
    )
    return job


def create_redshift_extraction_job(schema="public"):
    tmp_folder = '/var/tmp/amundsen/table_metadata'
    node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships/'.format(tmp_folder=tmp_folder)
    s3_bucket = os.getenv('S3_BUCKET')
    s3_directory = "amundsen"
    access_key = os.getenv('AWS_KEY')
    access_secret = os.getenv('AWS_SECRET_KEY')
    aws_zone = os.getenv("AWS_ZONE")
    neptune_endpoint = os.getenv('NEPTUNE_ENDPOINT')
    neptune_port = os.getenv("NEPTUNE_PORT")
    neptune_host = "{}:{}".format(neptune_endpoint, neptune_port)

    where_clause_suffix = textwrap.dedent("""
        where table_schema = '{schema}'
    """.format(schema=schema))

    job_config = ConfigFactory.from_dict({
        'extractor.postgres_metadata.{}'.format(PostgresMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY): where_clause_suffix,
        'extractor.postgres_metadata.{}'.format(PostgresMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME): True,
        'extractor.postgres_metadata.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING): connection_string(),
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.NODE_DIR_PATH): node_files_folder,
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.RELATION_DIR_PATH): relationship_files_folder,
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.SHOULD_DELETE_CREATED_DIR): False,
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.FORCE_CREATE_DIR): True,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.NODE_FILES_DIR): node_files_folder,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.RELATION_FILES_DIR): relationship_files_folder,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.BUCKET_NAME): s3_bucket,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.BASE_AMUNDSEN_DATA_PATH): s3_directory,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.REGION): aws_zone,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.AWS_ACCESS_KEY): access_key,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.AWS_SECRET_KEY): access_secret,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.NEPTUNE_HOST): neptune_host
    })

    job = DefaultJob(
        conf=job_config,
        task=DefaultTask(
            extractor=PostgresMetadataExtractor(),
            loader=FSNeptuneCSVLoader()
        ),
        publisher=NeptuneCSVPublisher()
    )
    return job


def create_elastic_search_publisher_job():
    extracted_search_data_path = '/var/tmp/amundsen/search_data.json'

    access_key = os.getenv('AWS_KEY')
    access_secret = os.getenv('AWS_SECRET_KEY')
    aws_zone = os.getenv("AWS_ZONE")

    neptune_endpoint = os.getenv('NEPTUNE_ENDPOINT')
    neptune_port = os.getenv("NEPTUNE_PORT")

    elastic_search_host = os.getenv('ELASTIC_SEARCH_HOST')

    elastic_search_client = Elasticsearch(
        elastic_search_host
    )

    task = DefaultTask(
        extractor=NeptuneSearchDataExtractor(),
        loader=FSElasticsearchJSONLoader(),
        transformer=NoopTransformer()
    )

    elasticsearch_new_index_key = 'tables' + str(uuid.uuid4())
    model_name = 'databuilder.models.table_elasticsearch_document.TableESDocument'
    neptune_host = "wss://{}:{}/gremlin".format(neptune_endpoint, neptune_port)

    job_config = ConfigFactory.from_dict({
        'extractor.search_data.extractor.neptune.{}'.format(NeptuneExtractor.NEPTUNE_HOST_CONFIG_KEY): neptune_host,
        'extractor.search_data.extractor.neptune.{}'.format(NeptuneExtractor.REGION_CONFIG_KEY): aws_zone,
        'extractor.search_data.extractor.neptune.{}'.format(NeptuneExtractor.AWS_ACCESS_KEY_CONFIG_KEY): access_key,
        'extractor.search_data.extractor.neptune.{}'.format(NeptuneExtractor.AWS_SECRET_KEY_CONFIG_KEY): access_secret,
        'extractor.search_data.extractor.neptune.{}'.format(NeptuneExtractor.MODEL_CLASS_CONFIG_KEY): model_name,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'loader.filesystem.elasticsearch.{}'.format(FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY): 'w',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_PATH_CONFIG_KEY):
            extracted_search_data_path,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.FILE_MODE_CONFIG_KEY): 'r',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY):
            elastic_search_client,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY):
            elasticsearch_new_index_key,
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY): 'table',
        'publisher.elasticsearch.{}'.format(ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY): 'table_search_index'
    })

    job = DefaultJob(
        conf=job_config,
        task=task,
        publisher=ElasticsearchPublisher()
    )

    return job


def main(schema):
    # assert schema
    # redshift_job = create_redshift_extraction_job(schema)
    # redshift_job.launch()

    assert schema
    redshift_job = create_redshift_extraction_job_with_looker(schema)
    redshift_job.launch()

    elastic_job = create_elastic_search_publisher_job()
    elastic_job.launch()


if __name__ == '__main__':
    arg_parser = ArgumentParser()
    arg_parser.add_argument(
        "--schema", help="Name of schema we are collecting table info from", dest="schema"
    )
    args = arg_parser.parse_args()
    schema_name = args.schema
    main(schema_name)
