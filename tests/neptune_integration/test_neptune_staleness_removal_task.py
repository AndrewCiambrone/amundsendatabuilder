import unittest
from datetime import datetime, timedelta

from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from mock import patch
from pyhocon import ConfigFactory

from databuilder.clients.neptune_client import NeptuneSessionClient
from databuilder.job.job import DefaultJob
from databuilder.serializers.neptune_serializer import (
    NEPTUNE_LAST_SEEN_AT_NODE_PROPERTY_NAME_BULK_LOADER_FORMAT,
    NEPTUNE_LAST_SEEN_AT_EDGE_PROPERTY_NAME_BULK_LOADER_FORMAT,
    NEPTUNE_CREATION_TYPE_NODE_PROPERTY_NAME_BULK_LOADER_FORMAT,
    NEPTUNE_CREATION_TYPE_EDGE_PROPERTY_NAME_BULK_LOADER_FORMAT,
    NEPTUNE_CREATION_TYPE_JOB
)
from databuilder.task.neptune_staleness_removal_task import NeptuneStalenessRemovalTask


def get_test_graph(self):
    local_testing_connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
    return traversal().withRemote(local_testing_connection)


class TestNeptuneStalenessRemovalTask(unittest.TestCase):

    def setUp(self) -> None:
        self.patcher = patch.object(NeptuneSessionClient, '_create_graph_source', get_test_graph)
        self.patcher.start()
        self.key_name = 'key'
        self.client = NeptuneSessionClient(
            self.key_name,
            'localhost',
            'test',
            'test',
            'test'
        )

    def tearDown(self) -> None:
        self._clear_graph()
        self.patcher.stop()

    def _clear_graph(self):
        g = get_test_graph('test')
        g.E().drop().iterate()
        g.V().drop().iterate()

    def get_job_config(self):
        target_relations = ['TABLE_TO_TABLE']
        target_nodes = ['Table']
        return ConfigFactory.from_dict({
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

    def _create_test_node(self, node_id, node_label, is_stale=False, is_user_created=False):
        # type: (str, str, bool, bool) -> None
        creation_type = "User" if is_user_created else NEPTUNE_CREATION_TYPE_JOB
        last_seen = datetime.utcnow() - timedelta(days=2) if is_stale else datetime.utcnow()
        self.client.upsert_node(node_id=node_id, node_label=node_label, node_properties={
            NEPTUNE_CREATION_TYPE_NODE_PROPERTY_NAME_BULK_LOADER_FORMAT: creation_type,
            NEPTUNE_LAST_SEEN_AT_NODE_PROPERTY_NAME_BULK_LOADER_FORMAT: last_seen
        })

    def _create_test_edge(self, start_id, end_id, edge_id, label, is_stale=False, is_user_created=False):
        creation_type = "User" if is_user_created else NEPTUNE_CREATION_TYPE_JOB
        last_seen = datetime.utcnow() - timedelta(days=2) if is_stale else datetime.utcnow()
        self.client.upsert_edge(
            start_node_id=start_id,
            end_node_id=end_id,
            edge_id=edge_id,
            edge_label=label,
            edge_properties={
                NEPTUNE_CREATION_TYPE_EDGE_PROPERTY_NAME_BULK_LOADER_FORMAT: creation_type,
                NEPTUNE_LAST_SEEN_AT_EDGE_PROPERTY_NAME_BULK_LOADER_FORMAT: last_seen
            }
        )

    def test_successful_staleness_removal_task(self):
        # create a recent node
        self._create_test_node(node_id="test_1", node_label="Table")
        # create a Stale node
        self._create_test_node(node_id="test_2", node_label="Table", is_stale=True)

        # create a recent edge
        self._create_test_edge('test_1', 'test_2', 'test_edge_1', 'TABLE_TO_TABLE')
        # create a stale edge
        self._create_test_edge('test_1', 'test_2', 'test_edge_2', 'TABLE_TO_TABLE', is_stale=True)

        job_config = self.get_job_config()
        job = DefaultJob(
            conf=job_config,
            task=NeptuneStalenessRemovalTask()
        )
        job.launch()
        stale_nodes = self.client.get_graph().V().has(self.key_name, 'test_2').toList()
        stale_edges = self.client.get_graph().E().has(self.key_name, 'test_edge_2').toList()
        self.assertEqual(0, len(stale_nodes))
        self.assertEqual(0, len(stale_edges))

    def test_ensure_user_created_entities_are_not_deleted(self):
        self._create_test_node(node_id="test_1", node_label="Table", is_stale=True, is_user_created=True)
        self._create_test_node(node_id="test_2", node_label="Table", is_stale=True, is_user_created=True)
        self._create_test_edge('test_1', 'test_2', 'test_edge_1', 'TABLE_TO_TABLE', is_stale=True, is_user_created=True)

        job_config = self.get_job_config()
        job = DefaultJob(
            conf=job_config,
            task=NeptuneStalenessRemovalTask()
        )
        job.launch()
        stale_nodes = self.client.get_graph().V().has(self.key_name, 'test_1').toList()
        stale_edges = self.client.get_graph().E().has(self.key_name, 'test_edge_1').toList()
        self.assertEqual(1, len(stale_nodes))
        self.assertEqual(1, len(stale_edges))

    def test_ensure_only_nodes_and_edges_specified_are_deleted(self):
        self._create_test_node(node_id="test_1", node_label="Table", is_stale=True)
        self._create_test_node(node_id="test_2", node_label="Table", is_stale=False)
        self._create_test_node(node_id="test_3", node_label="User", is_stale=True)
        self._create_test_node(node_id="test_4", node_label="User", is_stale=True)
        self._create_test_edge('test_1', 'test_2', 'test_edge_1', 'TABLE_TO_TABLE', is_stale=True)
        self._create_test_edge('test_1', 'test_2', 'test_edge_2', 'TABLE_TO_TABLE', is_stale=False)
        self._create_test_edge('test_4', 'test_3', 'test_edge_1', 'FRIENDS', is_stale=True)

        job_config = self.get_job_config()
        job = DefaultJob(
            conf=job_config,
            task=NeptuneStalenessRemovalTask()
        )
        job.launch()

        stale_nodes = self.client.get_graph().V().hasLabel("Table").toList()
        user_nodes = self.client.get_graph().V().hasLabel("User").toList()
        stale_edges = self.client.get_graph().E().has(self.key_name, 'test_edge_1').toList()
        friend_edges = self.client.get_graph().E().hasLabel("FRIENDS").toList()
        self.assertEqual(1, len(stale_nodes))
        self.assertEqual(2, len(user_nodes))
        self.assertEqual(1, len(stale_edges))
        self.assertEqual(1, len(friend_edges))

    def test_ensure_validation_works(self):
        self._create_test_node(node_id="test_1", node_label="Table", is_stale=True)
        self._create_test_node(node_id="test_2", node_label="Table", is_stale=True)

        job_config = self.get_job_config()
        job = DefaultJob(
            conf=job_config,
            task=NeptuneStalenessRemovalTask()
        )
        with self.assertRaises(Exception):
            job.launch()


if __name__ == '__main__':
    unittest.main()
