from typing import Optional, Dict, Any, Union, Mapping
from databuilder.utils.aws4authwebsocket.transport import Aws4AuthWebsocketTransport, WebsocketClientTransport
from gremlin_python.driver.driver_remote_connection import \
    DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import GraphTraversalSource
from gremlin_python.process.traversal import T, Order, gt, Cardinality, Column
from gremlin_python.process.graph_traversal import __

g = None


def get_graph(*,
              host: str,
              port: Optional[int] = None,
              user: str = None,
              password: Optional[Union[str, Mapping[str, str]]] = None,
              driver_remote_connection_options: Mapping[str, Any] = {},
              aws4auth_options: Mapping[str, Any] = {},
              websocket_options: Mapping[str, Any] = {}) -> GraphTraversalSource:
    global g
    if g is None:
        g = create_gremlin_session(host=host, password=password, aws4auth_options=aws4auth_options)
    return g


def upsert_node(g, node_id, node_label, node_properties):
    create_traversal = __.addV(node_label).property(T.id, node_id)
    node_traversal = g.V().hasId(node_id).\
        fold().\
        coalesce(__.unfold(), create_traversal)
    for key, value in node_properties.items():
        if not value:
            continue
        key = key.split(':')[0]
        node_traversal = node_traversal.property(Cardinality.single, key, value)

    node_traversal.next()


def upsert_edge(g, start_node_id, end_node_id, edge_id, edge_label, edge_properties: Dict[str, Any]):
    create_traversal = __.V(start_node_id).addE(edge_label).to(__.V(end_node_id)).property(T.id, edge_id)
    edge_traversal = g.V(start_node_id).outE(edge_label).hasId(edge_id).\
        fold().\
        coalesce(__.unfold(), create_traversal)
    for key, value in edge_properties.items():
        key_split = key.split(':')
        key = key_split[0]
        value_type = key_split[1]
        if "Long" in value_type:
            value = int(value)
        edge_traversal = edge_traversal.property(key, value)

    edge_traversal.next()


def get_table_info_for_search(g):
    result = g.V().hasLabel('Table'). \
        project('database', 'cluster', 'schema', 'schema_description', 'name', 'key', 'description', 'column_names', 'column_descriptions', 'total_usage', 'unique_usage', 'tags'). \
        by(__.out('TABLE_OF').out('SCHEMA_OF').out('CLUSTER_OF').values('name')). \
        by(__.out('TABLE_OF').out('SCHEMA_OF').values('name')). \
        by(__.out('TABLE_OF').values('name')). \
        by(__.coalesce(__.out('TABLE_OF').out('DESCRIPTION').values('description'), __.constant(''))). \
        by('name'). \
        by(T.id). \
        by(__.coalesce(__.out('DESCRIPTION').values('description'), __.constant(''))). \
        by(__.out('COLUMN').values('name').fold()). \
        by(__.out('COLUMN').out('DESCRIPTION').values('description').fold()). \
        by(__.coalesce(__.outE('READ_BY').values('read_count'), __.constant(0)).sum()). \
        by(__.outE('READ_BY').count()). \
        by(__.inE('TAG').outV().id().fold()). \
        toList()
    return result


def get_all_nodes_grouped_by_label(*, g):
    return g.V().groupCount().by(T.label).unfold(). \
        project('label', 'count'). \
        by(Column.keys). \
        by(Column.values).\
        toList()


def create_gremlin_session( *, host: str, port: Optional[int] = None, user: str = None,
                 password: Optional[Union[str, Mapping[str, str]]] = None,
                 driver_remote_connection_options: Mapping[str, Any] = {},
                 aws4auth_options: Mapping[str, Any] = {}, websocket_options: Mapping[str, Any] = {}) -> GraphTraversalSource:
        driver_remote_connection_options = dict(driver_remote_connection_options)
        # as others, we repurpose host a url
        driver_remote_connection_options.update(url=host)
        # port should be part of that url
        if port is not None:
            raise NotImplementedError(f'port is not allowed! port={port}')

        # always g for Neptune
        driver_remote_connection_options.update(traversal_source='g')

        # for IAM auth, we need the triplet
        if isinstance(password, Mapping):
            if user or 'aws_access_key_id' not in password or 'aws_secret_access_key' not in password or \
                    'service_region' not in password:
                raise NotImplementedError(f'to use authentication, pass a Mapping with aws_access_key_id, '
                                          f'aws_secret_access_key, service_region!')

            aws_access_key_id = password['aws_access_key_id']
            aws_secret_access_key = password['aws_secret_access_key']
            service_region = password['service_region']

            def factory() -> Aws4AuthWebsocketTransport:
                return Aws4AuthWebsocketTransport(aws_access_key_id=aws_access_key_id,
                                                  aws_secret_access_key=aws_secret_access_key,
                                                  service_region=service_region,
                                                  extra_aws4auth_options=aws4auth_options or {},
                                                  extra_websocket_options=websocket_options or {})
            driver_remote_connection_options.update(transport_factory=factory)
        elif password is not None:
            raise NotImplementedError(f'to use authentication, pass a Mapping with aws_access_key_id, '
                                      f'aws_secret_access_key, service_region!')
        else:
            raise NotImplementedError(f'to use authentication, pass a Mapping with aws_access_key_id, '
                                      f'aws_secret_access_key, service_region!')

            # we could use the default Transport, but then we'd have to take different options, which feels clumsier.
            def factory() -> WebsocketClientTransport:
                return WebsocketClientTransport(extra_websocket_options=websocket_options or {})
            driver_remote_connection_options.update(transport_factory=factory)

        return traversal().withRemote(DriverRemoteConnection(**driver_remote_connection_options))
