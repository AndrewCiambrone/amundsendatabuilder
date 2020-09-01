import unittest

from tests.neptune_integration.base_neptune_session_client_test import BaseNeptuneSessionClientTestCase, get_test_graph


class TestNeptuneClient(BaseNeptuneSessionClientTestCase):

    def test_upsert_node(self):
        self.client.upsert_node('test_1', 'label', {
            'name:String(single)': "Name1"
        })
        created_node_name = get_test_graph(None).V().has(self.key_name, 'test_1').values('name').next()
        self.assertEqual(created_node_name, 'Name1')
        self.client.upsert_node('test_1', 'label', {
            'name:String(single)': "Name2"
        })
        created_node_name = get_test_graph(None).V().has(self.key_name, 'test_1').values('name').next()
        self.assertEqual(created_node_name, 'Name2')

    def test_upsert_edge(self):
        self.client.upsert_node('test_1', 'label', {})
        self.client.upsert_node('test_2', 'label', {})
        self.client.upsert_edge('test_1', 'test_2', 'test_edge_1', 'edgy_edge', {
            'name:String(single)': "Name"
        })
        created_edge_name = get_test_graph(None).E().has(self.key_name, 'test_edge_1').values('name').next()
        self.assertEqual(created_edge_name, 'Name')
        self.client.upsert_edge('test_1', 'test_2', 'test_edge_1', 'edgy_edge', {
            'name:String(single)': "Name2"
        })
        created_edge_name = get_test_graph(None).E().has(self.key_name, 'test_edge_1').values('name').next()
        self.assertEqual(created_edge_name, 'Name2')

    def test_get_number_of_edges_grouped_by_label(self):
        self.client.upsert_node('test_1', 'label', {})
        self.client.upsert_node('test_2', 'label', {})
        self.client.upsert_node('test_3', 'label', {})
        self.client.upsert_edge('test_1', 'test_2', 'test_edge_1', 'edgy_edge', {})
        self.client.upsert_edge('test_1', 'test_1', 'test_edge_1', 'self_reflection', {})
        result_list = self.client.get_number_of_edges_grouped_by_label()
        self.assertEqual(2, len(result_list))
        edgy_edge_result = [result for result in result_list if result.get('type') == 'edgy_edge'][0]
        self_reflection_result = [result for result in result_list if result.get('type') == 'self_reflection'][0]
        self.assertEqual(edgy_edge_result.get('count'), 1)
        self.assertEqual(self_reflection_result.get('count'), 1)


if __name__ == '__main__':
    unittest.main()