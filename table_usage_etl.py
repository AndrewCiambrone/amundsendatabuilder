import os
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.extractor.postgres_table_usage_extractor import PostgresTableUsageExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neptune_csv_loader import FSNeptuneCSVLoader
from databuilder.publisher.neptune_upsert_publisher import NeptuneUpsertPublisher
from databuilder.task.task import DefaultTask
from databuilder import neptune_client

Base = declarative_base()


def connection_string():
    user = os.getenv('DW_USER')
    password = os.getenv('DW_PASS')
    host = os.getenv('DW_URL')
    port = '5439'
    db = os.getenv('DW_DB')
    return "postgresql://%s:%s@%s:%s/%s" % (user, password, host, port, db)


def create_redshift_extraction_job():
    table_usage_sql = os.getenv('TABLE_USEAGE_SQL')


    tmp_folder = '/var/tmp/amundsen/table_usage'
    node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships/'.format(tmp_folder=tmp_folder)

    access_key = os.getenv('AWS_KEY')
    access_secret = os.getenv('AWS_SECRET_KEY')
    aws_zone = os.getenv("AWS_ZONE")
    neptune_endpoint = os.getenv('NEPTUNE_ENDPOINT')
    neptune_port = os.getenv("NEPTUNE_PORT")
    neptune_host = "wss://{}:{}/gremlin".format(neptune_endpoint, neptune_port)

    job_config = ConfigFactory.from_dict({
        'extractor.postgres_table_usage.{}'.format(PostgresTableUsageExtractor.SQL_STATEMENT_KEY): table_usage_sql,
        'extractor.postgres_table_usage.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING): connection_string(),
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.NODE_DIR_PATH): node_files_folder,
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.RELATION_DIR_PATH): relationship_files_folder,
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.SHOULD_DELETE_CREATED_DIR): False,
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.FORCE_CREATE_DIR): True,
        'publisher.neptune_upsert_publisher.{}'.format(NeptuneUpsertPublisher.NODE_FILES_DIR): node_files_folder,
        'publisher.neptune_upsert_publisher.{}'.format(NeptuneUpsertPublisher.RELATION_FILES_DIR): relationship_files_folder,
        'publisher.neptune_upsert_publisher.{}'.format(NeptuneUpsertPublisher.REGION): aws_zone,
        'publisher.neptune_upsert_publisher.{}'.format(NeptuneUpsertPublisher.AWS_ACCESS_KEY): access_key,
        'publisher.neptune_upsert_publisher.{}'.format(NeptuneUpsertPublisher.AWS_SECRET_KEY): access_secret,
        'publisher.neptune_upsert_publisher.{}'.format(NeptuneUpsertPublisher.NEPTUNE_HOST): neptune_host,
    })
    job = DefaultJob(
        conf=job_config,
        task=DefaultTask(
            extractor=PostgresTableUsageExtractor(),
            loader=FSNeptuneCSVLoader()
        ),
        publisher=NeptuneUpsertPublisher()
    )
    return job


def main():
    redshift_job = create_redshift_extraction_job()
    redshift_job.launch()


if __name__ == '__main__':
    main()