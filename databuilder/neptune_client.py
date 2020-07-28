import requests
import hashlib, hmac
from datetime import datetime
import urllib
import os
import json
from typing import Optional, Dict, Any, Union, Mapping
from databuilder.utils.aws4authwebsocket.transport import Aws4AuthWebsocketTransport, WebsocketClientTransport
from gremlin_python.driver.driver_remote_connection import \
    DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import GraphTraversalSource
from gremlin_python.process.traversal import T, Order, gt
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
        node_traversal = node_traversal.property(key, value)

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
        if value_type == "Long":
            value = int(value)
        edge_traversal = edge_traversal.property(key, value)

    edge_traversal.next()


def get_table_info_for_search(g):
    result = g.V().hasLabel('Table'). \
        project('database', 'cluster', 'schema', 'schema_description', 'name', 'key', 'description', 'column_names', 'column_descriptions', 'total_usage', 'unique_usage', 'tags'). \
        by(__.out('TABLE_OF').out('SCHEMA_OF').out('CLUSTER_OF').values('name')). \
        by(__.out('TABLE_OF').out('SCHEMA_OF').values('name')). \
        by(__.out('TABLE_OF').values('name')). \
        by(__.coalesce(__.out('TABLE_OF').out('DESCRIPTION_OF').values('description'), __.constant(''))). \
        by('name'). \
        by(T.id). \
        by(__.coalesce(__.out('DESCRIPTION').values('description'), __.constant(''))). \
        by(__.out('COLUMN').values('name').fold()). \
        by(__.out('COLUMN').out('DESCRIPTION').fold()). \
        by(__.coalesce(__.outE('READ_BY').values('read_count'), __.constant(0)).sum()). \
        by(__.outE('READ_BY').count()). \
        by(__.inE('TAG').outV().id().fold()). \
        toList()
    return result


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


def make_bulk_upload_request(neptune_endpoint, neptune_port, bucket, s3_folder_location, region, access_key, secret_key):
    # type: (str, str, str, str, str, str, str) -> str
    s3_source = "s3://{bucket}/{s3_folder_location}".format(
        bucket=bucket,
        s3_folder_location=s3_folder_location
    )
    payload = {
        "source": s3_source,
        "format": "csv",
        "region": region,
        "accessKey": access_key,
        "secretKey": secret_key
    }
    neptune_host = "{}:{}".format(neptune_endpoint, neptune_port)
    response_json = _make_signed_request(
        method='POST',
        host=neptune_host,
        endpoint='/loader/',
        aws_access_key=access_key,
        aws_secret_key=secret_key,
        aws_region=region,
        body_payload=payload
    )
    return response_json.get('payload', {}).get('loadId')


def query_with_gremlin(neptune_endpoint, neptune_port, region, access_key, secret_key, gremlin_query):
    # type: (str, str, str, str, str, str, str) -> Dict
    neptune_host = "{}:{}".format(neptune_endpoint, neptune_port)
    query_params = {'gremlin': gremlin_query}
    response_json = _make_signed_request(
        method='GET',
        host=neptune_host,
        endpoint='/gremlin/',
        aws_access_key=access_key,
        aws_secret_key=secret_key,
        aws_region=region,
        query_params=query_params
    )

    return response_json


def _normalize_query_string(query):
    # type: (str) -> str
    kv = (list(map(str.strip, s.split("=")))
          for s in query.split('&')
          if len(s) > 0)

    normalized = '&'.join('%s=%s' % (p[0], p[1] if len(p) > 1 else '')
                          for p in sorted(kv))
    return normalized


def _sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()


def _get_signature_key(key, dateStamp, regionName):
    kDate = _sign(('AWS4' + key).encode('utf-8'), dateStamp)
    kRegion = _sign(kDate, regionName)
    kService = _sign(kRegion, 'neptune-db')
    kSigning = _sign(kService, 'aws4_request')
    return kSigning


def _make_authorization_header(
        method,
        host,
        endpoint,
        query_string,
        payload,
        aws_access_key,
        aws_secret_key,
        aws_region,
        request_datetime
):
    # type: (str, str, str, str, str, str, str, str, datetime) -> str

    algorithm = 'AWS4-HMAC-SHA256'
    date_stamp_str = request_datetime.strftime('%Y%m%d')
    datetime_str = request_datetime.strftime('%Y%m%dT%H%M%SZ')
    credential_scope = date_stamp_str + '/' + aws_region + '/neptune-db/aws4_request'
    signed_headers = 'host;x-amz-date'
    signing_key = _get_signature_key(aws_secret_key, date_stamp_str, aws_region)

    canonical_headers = 'host:' + host + '\n' + 'x-amz-date:' + datetime_str + '\n'
    hashed_payload = hashlib.sha256(payload.encode('utf-8')).hexdigest()
    canonical_request = "{method}\n{endpoint}\n{querystring}\n{canonical_headers}\n{signed_headers}\n{hashed_payload}".format(
        method=method,
        endpoint=endpoint,
        querystring=query_string,
        canonical_headers=canonical_headers,
        signed_headers=signed_headers,
        hashed_payload=hashed_payload
    )

    hashed_request = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
    string_to_sign = "{algorithm}\n{amazon_date}\n{credential_scope}\n{hashed_request}".format(
        algorithm=algorithm,
        amazon_date=datetime_str,
        credential_scope=credential_scope,
        hashed_request=hashed_request
    )
    signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()
    return "{algorithm} Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}".format(
        algorithm=algorithm,
        access_key=aws_access_key,
        credential_scope=credential_scope,
        signed_headers=signed_headers,
        signature=signature
    )


def _make_signed_request(method, host, endpoint, aws_access_key, aws_secret_key, aws_region, query_params=None, body_payload=None):
    # type: (str, str, str, str, str, str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]) -> Dict[str, Any]

    if query_params is None:
        query_params = {}

    query_params = urllib.parse.urlencode(query_params, quote_via=urllib.parse.quote)
    query_params = query_params.replace('%27', '%22')
    query_params = _normalize_query_string(query_params)

    if body_payload is None:
        body_payload = {}

    body_payload = urllib.parse.urlencode(body_payload, quote_via=urllib.parse.quote)
    body_payload = body_payload.replace('%27', '%22')

    now = datetime.utcnow()
    amazon_date_str = now.strftime('%Y%m%dT%H%M%SZ')

    authorization_header = _make_authorization_header(
        method=method,
        host=host,
        endpoint=endpoint,
        query_string=query_params,
        payload=body_payload,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        request_datetime=now,
        aws_region=aws_region
    )

    full_url = "https://{host}{endpoint}".format(
        host=host,
        endpoint=endpoint
    )
    request_headers = {
        'x-amz-date': amazon_date_str,
        'Authorization': authorization_header
    }

    if method == 'POST':
        request_headers['content-type'] = 'application/x-www-form-urlencoded'
        response = requests.post(
            url=full_url,
            data=body_payload,
            verify=False,
            headers=request_headers
        )

    elif method == 'GET':
        response = requests.get(
            url=full_url,
            params=query_params,
            verify=False,
            headers=request_headers
        )
    else:
        raise Exception("HTTP METHOD: {0}".format(method))

    return response.json()

