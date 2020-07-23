import os
import textwrap
import uuid
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.extractor.postgres_user_extractor import PostgresUserExtractor
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
    users_sql = os.getenv('USER_SQL')

    tmp_folder = '/var/tmp/amundsen/users'
    node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships/'.format(tmp_folder=tmp_folder)
    s3_bucket = os.getenv('S3_BUCKET')
    s3_directory = "amundsen"
    access_key = os.getenv('AWS_KEY')
    access_secret = os.getenv('AWS_SECRET_KEY')
    aws_zone = os.getenv("AWS_ZONE")
    neptune_endpoint = os.getenv('NEPTUNE_ENDPOINT')
    neptune_port = os.getenv("NEPTUNE_PORT")

    job_config = ConfigFactory.from_dict({
        'extractor.postgres_users.{}'.format(PostgresUserExtractor.SQL_STATEMENT_KEY): users_sql,
        'extractor.postgres_users.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING): connection_string(),
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
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.NEPTUNE_ENDPOINT): neptune_endpoint,
        'publisher.neptune_csv_publisher.{}'.format(NeptuneCSVPublisher.NEPTUNE_PORT): neptune_port,
    })
    job = DefaultJob(
        conf=job_config,
        task=DefaultTask(
            extractor=PostgresUserExtractor(),
            loader=FSNeptuneCSVLoader()
        ),
        publisher=NeptuneCSVPublisher()
    )
    return job


def main():
    redshift_job = create_redshift_extraction_job()
    redshift_job.launch()



if __name__ == '__main__':
    main()