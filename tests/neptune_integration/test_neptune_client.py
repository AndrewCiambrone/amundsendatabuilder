import unittest

from tests.neptune_integration.base_neptune_session_client_test import BaseNeptuneSessionClientTestCase, get_test_graph
from gremlin_python.process import traversal

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

    def test_get_number_of_edges_grouped_by_label_filtered(self):
        self.client.upsert_node('test_1', 'label', {})
        self.client.upsert_node('test_2', 'label', {})
        self.client.upsert_node('test_3', 'label', {})
        self.client.upsert_edge('test_1', 'test_2', 'test_edge_1', 'edgy_edge', {
            'type:String(single)': 'user'
        })
        self.client.upsert_edge('test_1', 'test_1', 'test_edge_1', 'self_reflection', {
            'type:String(single)': 'job'
        })
        filter_properties = [
            ('type', 'job', traversal.eq)
        ]
        result_list = self.client.get_number_of_edges_grouped_by_label(filter_properties=filter_properties)
        self.assertEqual(1, len(result_list))
        edgy_edge_result = [result for result in result_list if result.get('type') == 'self_reflection'][0]
        self.assertEqual(edgy_edge_result.get('count'), 1)

    def test_get_number_of_nodes_grouped_by_label(self):
        self.client.upsert_node('test_1', 'label', {})
        self.client.upsert_node('test_2', 'label', {})
        self.client.upsert_node('test_3', 'label1', {})
        self.client.upsert_node('test_4', 'label2', {})
        self.client.upsert_node('test_5', 'label2', {})
        self.client.upsert_node('test_6', 'label2', {})
        result_list = self.client.get_number_of_nodes_grouped_by_label()
        self.assertEqual(len(result_list), 3)
        label_result = [result for result in result_list if result.get('type') == 'label'][0]
        label1_result = [result for result in result_list if result.get('type') == 'label1'][0]
        label2_result = [result for result in result_list if result.get('type') == 'label2'][0]
        self.assertEqual(label_result.get('count'), 2)
        self.assertEqual(label1_result.get('count'), 1)
        self.assertEqual(label2_result.get('count'), 3)

    def test_get_number_of_nodes_grouped_by_label_filtered(self):
        self.client.upsert_node('test_1', 'label', {
            'type:String(single)': 'user'
        })
        self.client.upsert_node('test_2', 'label', {
            'type:String(single)': 'job'
        })
        self.client.upsert_node('test_3', 'label1', {
            'type:String(single)': 'job'
        })
        self.client.upsert_node('test_4', 'label2', {
            'type:String(single)': 'job'
        })
        self.client.upsert_node('test_5', 'label2', {
            'type:String(single)': 'job'
        })
        self.client.upsert_node('test_6', 'label2', {
            'type:String(single)': 'user'
        })
        filter_properties = [
            ('type', 'job', traversal.eq)
        ]
        result_list = self.client.get_number_of_nodes_grouped_by_label(filter_properties=filter_properties)
        self.assertEqual(len(result_list), 3)
        label_result = [result for result in result_list if result.get('type') == 'label'][0]
        label1_result = [result for result in result_list if result.get('type') == 'label1'][0]
        label2_result = [result for result in result_list if result.get('type') == 'label2'][0]
        self.assertEqual(label_result.get('count'), 1)
        self.assertEqual(label1_result.get('count'), 1)
        self.assertEqual(label2_result.get('count'), 2)

    def test_delete_all_edges(self):
        self.client.upsert_node('test_1', 'label', {})
        self.client.upsert_node('test_2', 'label', {})
        self.client.upsert_node('test_3', 'label', {})
        self.client.upsert_edge('test_1', 'test_2', 'test_edge_1', 'edgy_edge', {
            'type:String(single)': 'user'
        })
        self.client.upsert_edge('test_1', 'test_1', 'test_edge_1', 'self_reflection', {
            'type:String(single)': 'job'
        })
        self.client.delete_edges([], [])
        edges = get_test_graph(None).E().toList()
        self.assertEqual(len(edges), 0)

    def test_delete_edges_with_filtering(self):
        self.client.upsert_node('test_1', 'label', {})
        self.client.upsert_node('test_2', 'label', {})
        self.client.upsert_node('test_3', 'label', {})
        self.client.upsert_edge('test_1', 'test_2', 'test_edge_1', 'edgy_edge', {
            'type:String(single)': 'user'
        })
        self.client.upsert_edge('test_1', 'test_1', 'test_edge_1', 'self_reflection', {
            'type:String(single)': 'job'
        })
        filter_properties = [
            ('type', 'job', traversal.eq)
        ]
        self.client.delete_edges(filter_properties, [])
        edges = get_test_graph(None).E().toList()
        self.assertEqual(len(edges), 1)

    def test_delete_edges_with_filtering_and_labels_listed(self):
        self.client.upsert_node('test_1', 'label', {})
        self.client.upsert_node('test_2', 'label', {})
        self.client.upsert_node('test_3', 'label', {})
        self.client.upsert_edge('test_1', 'test_2', 'test_edge_1', 'edgy_edge', {
            'type:String(single)': 'user'
        })
        self.client.upsert_edge('test_1', 'test_1', 'test_edge_1', 'self_reflection', {
            'type:String(single)': 'job'
        })
        filter_properties = [
            ('type', 'job', traversal.eq)
        ]
        self.client.delete_edges(filter_properties, ['edgy_edge'])
        edges = get_test_graph(None).E().toList()
        self.assertEqual(len(edges), 2)

    def test_delete_all_nodes(self):
        self.client.upsert_node('test_1', 'label', {})
        self.client.upsert_node('test_2', 'label', {})
        self.client.upsert_node('test_3', 'label', {})
        self.client.delete_nodes([], [])
        nodes = get_test_graph(None).V().toList()
        self.assertEqual(len(nodes), 0)

    def test_delete_all_nodes_filtering_by_label(self):
        self.client.upsert_node('test_1', 'label', {})
        self.client.upsert_node('test_2', 'label1', {})
        self.client.upsert_node('test_3', 'label', {})
        self.client.delete_nodes([], ['label1'])
        nodes = get_test_graph(None).V().toList()
        self.assertEqual(len(nodes), 2)

    def test_delete_all_nodes_filtering_by_label_and_property_names(self):
        self.client.upsert_node('test_1', 'label', {
            'type:String(single)': 'user'
        })
        self.client.upsert_node('test_2', 'label1', {
            'type:String(single)': 'user'
        })
        self.client.upsert_node('test_3', 'label', {
            'type:String(single)': 'job'
        })
        filter_properties = [
            ('type', 'job', traversal.eq)
        ]
        self.client.delete_nodes(filter_properties, ['label1'])

        nodes = get_test_graph(None).V().toList()
        self.assertEqual(len(nodes), 3)

        filter_properties = [
            ('type', 'user', traversal.eq)
        ]
        self.client.delete_nodes(filter_properties, ['label1'])
        nodes = get_test_graph(None).V().toList()
        self.assertEqual(len(nodes), 2)


if __name__ == '__main__':
    unittest.main()