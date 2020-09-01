import unittest
from databuilder.clients.neptune_client import NeptuneSessionClient
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from mock import patch


def get_test_graph(self):
    local_testing_connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
    return traversal().withRemote(local_testing_connection)


class BaseNeptuneSessionClientTestCase(unittest.TestCase):
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