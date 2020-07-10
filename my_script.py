import sys
import os
import textwrap
import uuid
from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.postgres_metadata_extractor import PostgresMetadataExtractor
from databuilder.extractor.sql_alchemy_extractor import SQLAlchemyExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neptune_csv_loader import FSNeptuneCSVLoader
from databuilder.task.task import DefaultTask

es_host = None
neptune_host = None
if len(sys.argv) > 1:
    es_host = sys.argv[1]
if len(sys.argv) > 2:
    neptune_host = sys.argv[2]

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

    where_clause_suffix = textwrap.dedent("""
        where table_schema = 'public'
    """)

    job_config = ConfigFactory.from_dict({
        'extractor.postgres_metadata.{}'.format(PostgresMetadataExtractor.WHERE_CLAUSE_SUFFIX_KEY): where_clause_suffix,
        'extractor.postgres_metadata.{}'.format(PostgresMetadataExtractor.USE_CATALOG_AS_CLUSTER_NAME): True,
        'extractor.postgres_metadata.extractor.sqlalchemy.{}'.format(SQLAlchemyExtractor.CONN_STRING): connection_string(),
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.NODE_DIR_PATH): node_files_folder,
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.RELATION_DIR_PATH): relationship_files_folder,
        'loader.filesystem_csv_neptune.{}'.format(FSNeptuneCSVLoader.SHOULD_DELETE_CREATED_DIR): False
    })
    job = DefaultJob(
        conf=job_config,
        task=DefaultTask(
            extractor=PostgresMetadataExtractor(),
            loader=FSNeptuneCSVLoader()
        )
    )
    return job


def main():
    job = create_redshift_extraction_job()
    job.launch()


if __name__ == '__main__':
    main()
