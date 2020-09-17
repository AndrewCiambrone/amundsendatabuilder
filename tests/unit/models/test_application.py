import unittest

from databuilder.models.application import Application
from databuilder.models.graph_node import GraphNode
from databuilder.models.graph_relationship import GraphRelationship
from databuilder.models.table_metadata import TableMetadata


class TestApplication(unittest.TestCase):

    def setUp(self):
        # type: () -> None
        super(TestApplication, self).setUp()

        self.application = Application(task_id='hive.default.test_table',
                                       dag_id='event_test',
                                       schema='default',
                                       table_name='test_table',
                                       app_name='Airflow',
                                       application_url_template='airflow_host.net/admin/airflow/tree?dag_id={dag_id}')

        self.expected_node_result = GraphNode(
            id='application://gold.airflow/event_test/hive.default.test_table',
            label='Application',
            node_attribute ={
                'application_url': 'airflow_host.net/admin/airflow/tree?dag_id=event_test',
                'id': 'event_test/hive.default.test_table',
                'name': 'Airflow',
                'description': 'Airflow with id event_test/hive.default.test_table'
            }
        )

        self.expected_relation_result = GraphRelationship(
            start_key='hive://gold.default/test_table',
            start_label=TableMetadata.TABLE_NODE_LABEL,
            end_key='application://gold.airflow/event_test/hive.default.test_table',
            end_label='Application',
            type='DERIVED_FROM',
            reverse_type='GENERATES',
            relationship_attributes={}
        )

    def test_create_next_node(self):
        # type: () -> None
        next_node = self.application.create_next_node()
        self.assertEquals(next_node, self.expected_node_result)

    def test_create_next_relation(self):
        # type: () -> None
        next_relation = self.application.create_next_relation()
        self.assertEquals(next_relation, self.expected_relation_result)

    def test_get_table_model_key(self):
        # type: () -> None
        table = self.application.get_table_model_key()
        self.assertEquals(table, 'hive://gold.default/test_table')

    def test_get_application_model_key(self):
        # type: () -> None
        application = self.application.get_application_model_key()
        self.assertEquals(application, self.expected_node_result.id)

    def test_create_nodes(self):
        # type: () -> None
        nodes = self.application.create_nodes()
        self.assertEquals(len(nodes), 1)
        self.assertEquals(nodes[0], self.expected_node_result)

    def test_create_relation(self):
        # type: () -> None
        relation = self.application.create_relation()
        self.assertEquals(len(relation), 1)
        self.assertEquals(relation[0], self.expected_relation_result)
