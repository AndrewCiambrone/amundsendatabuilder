import unittest
from databuilder.clients.neptune_client import NeptuneSessionClient
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from mock import patch
from pyhocon import ConfigFactory
from databuilder import Scoped


def get_test_graph(self):
    local_testing_connection = DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
    return traversal().withRemote(local_testing_connection)


class BaseNeptuneSessionClientTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.patcher = patch.object(NeptuneSessionClient, '_create_graph_source', get_test_graph)
        self.patcher.start()
        self.key_name = 'key'
        self.client = NeptuneSessionClient()
        client_scope = self.client.get_scope()
        self.conf = ConfigFactory.from_dict({
            client_scope: {
                NeptuneSessionClient.NEPTUNE_HOST_NAME: 'localhost',
                NeptuneSessionClient.AWS_REGION: 'locally hopefully',
                NeptuneSessionClient.AWS_ACCESS_KEY: 'access_key',
                NeptuneSessionClient.AWS_ACCESS_SECRET: 'access_secret',
                NeptuneSessionClient.GRAPH_ID_PROPERTY_NAME: self.key_name
            }
        })
        self.client.init(Scoped.get_scoped_conf(self.conf, self.client.get_scope()))



    def tearDown(self) -> None:
        self._clear_graph()
        self.patcher.stop()

    def _clear_graph(self):
        g = get_test_graph('test')
        g.E().drop().iterate()
        g.V().drop().iterate()