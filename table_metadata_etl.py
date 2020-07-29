import os
import textwrap
import uuid
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.neptune_extractor import NeptuneExtractor
from databuilder.transformer.base_transformer import NoopTransformer
from databuilder.extractor.neptune_search_data_extractor import NeptuneSearchDataExtractor
from databuilder.extractor.postgres_metadata_extractor import PostgresMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neptune_csv_loader import FSNeptuneCSVLoader
from databuilder.publisher.neptune_csv_publisher import NeptuneCSVPublisher
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.task.task import DefaultTask

Base = declarative_base()


def connection_string():
    user = os.getenv('DW_USER')
    password = os.getenv('DW_PASS')
    host = os.getenv('DW_URL')
    port = '5439'
    db = os.getenv('DW_DB')
    return "postgresql://%s:%s@%s:%s/%s" % (user, password, host, port, db)


def create_redshift_extraction_job():

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
    neptune_host = "{}:{}/gremlin".format(neptune_endpoint, neptune_port)


    where_clause_suffix = textwrap.dedent("""
        where table_schema = 'public'
    """)

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


def main():
    redshift_job = create_redshift_extraction_job()
    redshift_job.launch()

    elastic_job = create_elastic_search_publisher_job()
    elastic_job.launch()


if __name__ == '__main__':
    main()
