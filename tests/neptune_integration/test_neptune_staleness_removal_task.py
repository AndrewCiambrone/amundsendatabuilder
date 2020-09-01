import unittest
from datetime import datetime, timedelta

from databuilder.clients.neptune_client import NeptuneSessionClient
from databuilder.task.neptune_staleness_removal_task import NeptuneStalenessRemovalTask
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from mock import patch
from databuilder.serializers.neptune_serializer import (
    NEPTUNE_LAST_SEEN_AT_NODE_PROPERTY_NAME_BULK_LOADER_FORMAT,
    NEPTUNE_LAST_SEEN_AT_EDGE_PROPERTY_NAME_BULK_LOADER_FORMAT,
    NEPTUNE_CREATION_TYPE_NODE_PROPERTY_NAME_BULK_LOADER_FORMAT,
    NEPTUNE_CREATION_TYPE_EDGE_PROPERTY_NAME_BULK_LOADER_FORMAT,
    NEPTUNE_CREATION_TYPE_JOB
)
from databuilder.job.job import DefaultJob
from pyhocon import ConfigFactory


def get_test_graph(self):
    local_testing_connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
    return traversal().withRemote(local_testing_connection)


class TestNeptuneStalenessRemovalTask(unittest.TestCase):

    def setUp(self) -> None:
        self.patcher = patch.object(NeptuneSessionClient, '_create_graph_source', get_test_graph)
        self.patcher.start()
        self.key_name = 'key'

    def tearDown(self) -> None:
        self._clear_graph()
        self.patcher.stop()

    def _clear_graph(self):
        g = get_test_graph('test')
        g.E().drop().iterate()
        g.V().drop().iterate()

    def test_successful_staleness_removal_task(self):
        client = NeptuneSessionClient(
            self.key_name,
            'localhost',
            'test',
            'test',
            'test'
        )

        # create a recent node
        client.upsert_node(node_id="test_1", node_label="Table", node_properties={
            NEPTUNE_CREATION_TYPE_NODE_PROPERTY_NAME_BULK_LOADER_FORMAT: NEPTUNE_CREATION_TYPE_JOB,
            NEPTUNE_LAST_SEEN_AT_NODE_PROPERTY_NAME_BULK_LOADER_FORMAT: datetime.utcnow()
        })

        # create a Stale node
        client.upsert_node(node_id="test_2", node_label="Table", node_properties={
            NEPTUNE_CREATION_TYPE_NODE_PROPERTY_NAME_BULK_LOADER_FORMAT: NEPTUNE_CREATION_TYPE_JOB,
            NEPTUNE_LAST_SEEN_AT_NODE_PROPERTY_NAME_BULK_LOADER_FORMAT: datetime.utcnow() - timedelta(days=2)
        })

        # create a recent edge
        client.upsert_edge(
            start_node_id="test_1",
            end_node_id="test_2",
            edge_id="test_edge_1",
            edge_label="TABLE_TO_TABLE",
            edge_properties={
                NEPTUNE_CREATION_TYPE_EDGE_PROPERTY_NAME_BULK_LOADER_FORMAT: NEPTUNE_CREATION_TYPE_JOB,
                NEPTUNE_LAST_SEEN_AT_EDGE_PROPERTY_NAME_BULK_LOADER_FORMAT: datetime.utcnow()
            }
        )

        # create a stale edge
        client.upsert_edge(
            start_node_id="test_1",
            end_node_id="test_2",
            edge_id="test_edge_2",
            edge_label="TABLE_TO_TABLE",
            edge_properties={
                NEPTUNE_CREATION_TYPE_EDGE_PROPERTY_NAME_BULK_LOADER_FORMAT: NEPTUNE_CREATION_TYPE_JOB,
                NEPTUNE_LAST_SEEN_AT_EDGE_PROPERTY_NAME_BULK_LOADER_FORMAT: datetime.utcnow() - timedelta(days=2)
            }
        )

        target_relations = ['TABLE_TO_TABLE']
        target_nodes = ['Table']
        job_config = ConfigFactory.from_dict({
            'task.remove_stale_data': {
                NeptuneStalenessRemovalTask.BATCH_SIZE: 1000,
                NeptuneStalenessRemovalTask.TARGET_RELATIONS: target_relations,
                NeptuneStalenessRemovalTask.TARGET_NODES: target_nodes,
                NeptuneStalenessRemovalTask.STALENESS_CUT_OFF_IN_SECONDS: 86400,  # 1 day
                NeptuneStalenessRemovalTask.STALENESS_MAX_PCT: 75,
                NeptuneStalenessRemovalTask.NEPTUNE_HOST: 'localhost',
                NeptuneStalenessRemovalTask.AWS_REGION: 'autozone',
                NeptuneStalenessRemovalTask.AWS_ACCESS_KEY: self.key_name,
                NeptuneStalenessRemovalTask.AWS_ACCESS_SECRET: 'super_secret_key'
            }
        })

        job = DefaultJob(
            conf=job_config,
            task=NeptuneStalenessRemovalTask()
        )
        job.launch()
        stale_nodes = client.get_graph().V().has(self.key_name, 'test_2').toList()
        stale_edges = client.get_graph().E().has(self.key_name, 'test_edge_2').toList()
        self.assertEqual(0, len(stale_nodes))
        self.assertEqual(0, len(stale_edges))



if __name__ == '__main__':
    unittest.main()
