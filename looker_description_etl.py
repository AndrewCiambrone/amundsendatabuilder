import os
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base

from databuilder.extractor.restapi.github_file_extractor import GithubFileExtractor
from databuilder.extractor.dashboard.looker.looker_view_descriptions_extractor import LookerViewDescriptionsExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_neptune_csv_loader import FSNeptuneCSVLoader
from databuilder.publisher.neptune_upsert_publisher import NeptuneUpsertPublisher
from databuilder.task.task import DefaultTask
from databuilder import neptune_client

Base = declarative_base()


def create_looker_extraction_job():
    tmp_folder = '/var/tmp/amundsen/looker_description'
    node_files_folder = '{tmp_folder}/nodes/'.format(tmp_folder=tmp_folder)
    relationship_files_folder = '{tmp_folder}/relationships/'.format(tmp_folder=tmp_folder)

    access_key = os.getenv('AWS_KEY')
    access_secret = os.getenv('AWS_SECRET_KEY')
    aws_zone = os.getenv("AWS_ZONE")
    neptune_endpoint = os.getenv('NEPTUNE_ENDPOINT')
    neptune_port = os.getenv("NEPTUNE_PORT")
    neptune_host = "wss://{}:{}/gremlin".format(neptune_endpoint, neptune_port)

    looker_repo_name = os.getenv('LOOKER_REPO_NAME')
    looker_views_base_directory = os.getenv('LOOKER_VIEWS_BASE_DIRECTORY')
    github_user_name = os.getenv('GITHUB_USER_NAME')
    github_access_token = os.getenv('GITHUB_ACCESS_TOKEN')
    github_org = os.getenv('GITHUB_ORG')


    job_config = ConfigFactory.from_dict({
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.REPO_DIRECTORY): looker_views_base_directory,
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.REPO_NAME): looker_repo_name,
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.GITHUB_USER_NAME): github_user_name,
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.GITHUB_ACCESS_TOKEN): github_access_token,
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.GITHUB_ORG_NAME): github_org,
        'extractor.looker_view_descriptions_extractor.extractor.github_file_extractor.{}'.format(GithubFileExtractor.EXPECTED_FILE_EXTENSIONS): ['.view.lkml'],
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
            extractor=LookerViewDescriptionsExtractor(),
            loader=FSNeptuneCSVLoader()
        ),
        publisher=NeptuneUpsertPublisher()
    )
    return job


def main():
    redshift_job = create_looker_extraction_job()
    redshift_job.launch()


if __name__ == '__main__':
    main()