from databuilder.task.neptune_staleness_removal_task import NeptuneStalenessRemovalTask
from databuilder.job.job import DefaultJob
from pyhocon import ConfigFactory
import os


def create_remove_stale_data_job():
    access_key = os.getenv('AWS_KEY')
    access_secret = os.getenv('AWS_SECRET_KEY')
    aws_zone = os.getenv("AWS_ZONE")
    neptune_endpoint = os.getenv('NEPTUNE_ENDPOINT')
    neptune_port = os.getenv("NEPTUNE_PORT")
    neptune_host = "wss://{}:{}/gremlin".format(neptune_endpoint, neptune_port)
    target_relations = ['DESCRIPTION', 'DESCRIPTION_OF', 'COLUMN', 'COLUMN_OF', 'TABLE', 'TABLE_OF']
    target_nodes = ['Table', 'User', 'Column', 'Programmatic_Description', "Schema"]
    job_config = ConfigFactory.from_dict({
        'task.remove_stale_data': {
            NeptuneStalenessRemovalTask.BATCH_SIZE: 1000,
            NeptuneStalenessRemovalTask.TARGET_RELATIONS: target_relations,
            NeptuneStalenessRemovalTask.TARGET_NODES: target_nodes,
            NeptuneStalenessRemovalTask.STALENESS_CUT_OFF_IN_SECONDS: 86400,  # 1 day
            NeptuneStalenessRemovalTask.NEPTUNE_HOST: neptune_host,
            NeptuneStalenessRemovalTask.AWS_REGION: aws_zone,
            NeptuneStalenessRemovalTask.AWS_ACCESS_KEY: access_key,
            NeptuneStalenessRemovalTask.AWS_ACCESS_SECRET: access_secret
        }
    })
    job = DefaultJob(
        conf=job_config,
        task=NeptuneStalenessRemovalTask()
    )
    return job


if __name__ == '__main__':
    job = create_remove_stale_data_job()
    job.launch()