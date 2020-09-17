import hashlib
import hmac
import urllib
from datetime import datetime
from typing import Union, Optional, Dict, Any, List, Tuple, Callable

import requests
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import (
    GraphTraversalSource,
    GraphTraversal
)
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T

from databuilder.utils.aws4authwebsocket.transport import (
    Aws4AuthWebsocketTransport
)
from databuilder import Scoped
from pyhocon import ConfigFactory, ConfigTree  # noqa: F401


class BadBulkUploadException(Exception):
    message = "Failed to insert rows into Neptune"


class BulkUploaderNeptuneClient:
    def __init__(self, neptune_host, region, access_key, access_secret, arn, session_token=None):
        # type: (str, str, str, str, str, Union[str, None]) -> None
        assert access_key
        assert access_secret
        assert access_key != access_secret
        self.neptune_host = neptune_host
        self.region = region
        self.access_key = access_key
        self.access_secret = access_secret
        self.session_token = session_token
        self.arn = arn

    def make_bulk_upload_request(self, bucket, s3_folder_location):
        # type: (str, str) -> str
        s3_source = "s3://{bucket}/{s3_folder_location}".format(
            bucket=bucket,
            s3_folder_location=s3_folder_location
        )
        payload = {
            "source": s3_source,
            "format": "csv",
            "region": self.region,
            "iamRoleArn": self.arn,
            "updateSingleCardinalityProperties": "TRUE"
        }
        response_json = self._make_signed_request(
            method='POST',
            endpoint='/loader/',
            body_payload=payload
        )
        load_id = response_json.get('payload', {}).get('loadId')
        if load_id is None:
            raise BadBulkUploadException()
        return load_id

    def is_bulk_status_job_done(self, load_id):
        # type: (str) -> Tuple[bool, str]
        status_response = self.get_status_on_bulk_loader(load_id)
        status = status_response.get('payload', {}).get('overallStatus', {}).get('status')
        if not status:
            print("Ran into Error")
            print(repr(status_response))
            raise Exception("Ran into error bulk loading")

        if status == "LOAD_FAILED":
            print(repr(status_response))
            raise Exception("Ran into error bulk loading")

        return status != 'LOAD_IN_PROGRESS', status

    def get_status_on_bulk_loader(self, load_id):
        # type: (str) -> Dict[str, Any]
        query_params = {
            'loadId': load_id,
            'errors': True,
            'details': True
        }
        response_json = self._make_signed_request(
            method='GET',
            endpoint='/loader',
            query_params=query_params
        )

        return response_json

    def _make_signed_request(self, method, endpoint, query_params=None, body_payload=None):
        # type: (str, str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]) -> Dict[str, Any]

        if query_params is None:
            query_params = {}

        query_params = urllib.parse.urlencode(query_params, quote_via=urllib.parse.quote)
        query_params = query_params.replace('%27', '%22')
        query_params = BulkUploaderNeptuneClient._normalize_query_string(query_params)

        if body_payload is None:
            body_payload = {}

        body_payload = urllib.parse.urlencode(body_payload, quote_via=urllib.parse.quote)
        body_payload = body_payload.replace('%27', '%22')

        now = datetime.utcnow()
        amazon_date_str = now.strftime('%Y%m%dT%H%M%SZ')

        authorization_header = self._make_authorization_header(
            method=method,
            endpoint=endpoint,
            query_string=query_params,
            payload=body_payload,
            request_datetime=now,
        )

        full_url = "https://{host}{endpoint}".format(
            host=self.neptune_host,
            endpoint=endpoint
        )
        request_headers = {
            'x-amz-date': amazon_date_str,
            'Authorization': authorization_header
        }

        if self.session_token:
            request_headers['x-amz-security-token'] = self.session_token

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

    def _make_authorization_header(
            self,
            method,
            endpoint,
            query_string,
            payload,
            request_datetime
    ):
        # type: (str, str, str, str, datetime) -> str

        algorithm = 'AWS4-HMAC-SHA256'
        date_stamp_str = request_datetime.strftime('%Y%m%d')
        datetime_str = request_datetime.strftime('%Y%m%dT%H%M%SZ')
        credential_scope = date_stamp_str + '/' + self.region + '/neptune-db/aws4_request'
        signed_headers = 'host;x-amz-date'
        signing_key = BulkUploaderNeptuneClient._get_signature_key(self.access_secret, date_stamp_str, self.region)

        canonical_headers = 'host:' + self.neptune_host + '\n' + 'x-amz-date:' + datetime_str + '\n'
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
            access_key=self.access_key,
            credential_scope=credential_scope,
            signed_headers=signed_headers,
            signature=signature
        )

    @staticmethod
    def _normalize_query_string(query):
        # type: (str) -> str
        kv = (list(map(str.strip, s.split("=")))
              for s in query.split('&')
              if len(s) > 0)

        normalized = '&'.join('%s=%s' % (p[0], p[1] if len(p) > 1 else '')
                              for p in sorted(kv))
        return normalized

    @staticmethod
    def _get_signature_key(key, dateStamp, regionName):
        # type: (str, str, str) -> str
        kDate = BulkUploaderNeptuneClient._sign(('AWS4' + key).encode('utf-8'), dateStamp)
        kRegion = BulkUploaderNeptuneClient._sign(kDate, regionName)
        kService = BulkUploaderNeptuneClient._sign(kRegion, 'neptune-db')
        kSigning = BulkUploaderNeptuneClient._sign(kService, 'aws4_request')
        return kSigning

    @staticmethod
    def _sign(key, msg):
        # type: (str, str) -> str
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()


class NeptuneSessionClient(Scoped):
    # What property is used to local nodes and edges by ids
    GRAPH_ID_PROPERTY_NAME = 'graph_id_property_name'
    # Neptune host name wss://<url>:<port>/gremlin
    NEPTUNE_HOST_NAME = 'neptune_host_name'
    # AWS Region the Neptune cluster is located
    AWS_REGION = 'aws_region'
    AWS_ACCESS_KEY = 'aws_access_key'
    AWS_ACCESS_SECRET = 'aws_access_secret'
    AWS_SESSION_TOKEN = 'aws_session_token'

    WEBSOCKET_OPTIONS = 'websocket_options'

    DEFAULT_CONFIG = ConfigFactory.from_dict(
        {
            GRAPH_ID_PROPERTY_NAME: T.id,
            AWS_SESSION_TOKEN: None,
            WEBSOCKET_OPTIONS: {},
        }
    )

    def __init__(self):
        # type: () -> NeptuneSessionClient
        self._graph: Optional[GraphTraversalSource] = None

    def init(self, conf):
        # type: (ConfigTree) -> None
        conf = conf.with_fallback(NeptuneSessionClient.DEFAULT_CONFIG)
        self.id_property_name = conf.get(NeptuneSessionClient.GRAPH_ID_PROPERTY_NAME)
        self.neptune_host = conf.get_string(NeptuneSessionClient.NEPTUNE_HOST_NAME)
        self.region = conf.get_string(NeptuneSessionClient.AWS_REGION)
        self.access_key = conf.get_string(NeptuneSessionClient.AWS_ACCESS_KEY)
        self.access_secret = conf.get_string(NeptuneSessionClient.AWS_ACCESS_SECRET)
        self.session_token = conf.get_string(NeptuneSessionClient.AWS_SESSION_TOKEN)
        self.websocket_options = conf.get(NeptuneSessionClient.WEBSOCKET_OPTIONS)

    def get_scope(self):
        return 'neptune.client'

    def get_graph(self) -> GraphTraversalSource:
        if self._graph is None:
            self._graph = self._create_graph_source()

        return self._graph

    def _create_graph_source(self) -> GraphTraversalSource:
        def factory() -> Aws4AuthWebsocketTransport:
            aws4auth_options = {}
            if self.session_token:
                aws4auth_options['session_token'] = self.session_token
            return Aws4AuthWebsocketTransport(
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.access_secret,
                service_region=self.region,
                extra_aws4auth_options=aws4auth_options,
                extra_websocket_options=self.websocket_options or {}
            )
        driver_remote_connection_options = {
            'url': self.neptune_host,
            'transport_factory': factory,
            'traversal_source': 'g'
        }

        return traversal().withRemote(DriverRemoteConnection(**driver_remote_connection_options))

    def upsert_node(self, node_id, node_label, node_properties):
        # type: (str, str, Dict[str, Any]) -> None
        create_traversal = __.addV(node_label).property(self.id_property_name, node_id)
        node_traversal = self.get_graph().V().has(self.id_property_name, node_id). \
            fold(). \
            coalesce(__.unfold(), create_traversal)

        node_traversal = NeptuneSessionClient._update_entity_properties_on_traversal(node_traversal, node_properties)
        node_traversal.next()

    def upsert_edge(self, start_node_id, end_node_id, edge_id, edge_label, edge_properties):
        # type: (str, str, str, str, Dict[str, Any]) -> None
        create_traversal = __.V().has(self.id_property_name, start_node_id).addE(edge_label).to(__.V().has(self.id_property_name, end_node_id)).property(self.id_property_name, edge_id)
        edge_traversal = self.get_graph().V().has(self.id_property_name, start_node_id).outE(edge_label).has(self.id_property_name, edge_id). \
            fold(). \
            coalesce(__.unfold(), create_traversal)

        edge_traversal = NeptuneSessionClient._update_entity_properties_on_traversal(edge_traversal, edge_properties)
        edge_traversal.next()

    @staticmethod
    def _update_entity_properties_on_traversal(graph_traversal, properties):
        # type: (GraphTraversal, Dict[str, Any]) -> GraphTraversal
        for key, value in properties.items():
            key_split = key.split(':')
            key = key_split[0]
            value_type = key_split[1]
            if "Long" in value_type:
                value = int(value)
            graph_traversal = graph_traversal.property(key, value)

        return graph_traversal

    @staticmethod
    def filter_traversal(graph_traversal, filter_properties):
        # type: (GraphTraversal, List[Tuple[str, Any, Callable]]) -> GraphTraversal
        for filter_property in filter_properties:
            (filter_property_name, filter_property_value, filter_operator) = filter_property
            graph_traversal = graph_traversal.has(filter_property_name, filter_operator(filter_property_value))
        return graph_traversal

    def delete_edges(self, filter_properties, edge_labels):
        # type: (List[Tuple[str, Any, Callable]], Optional[List[str]]) -> None
        tx = self.get_graph().E()
        if edge_labels:
            tx = tx.hasLabel(*edge_labels)
        tx = NeptuneSessionClient.filter_traversal(tx, filter_properties)

        tx.drop().iterate()

    def delete_nodes(self, filter_properties, node_labels):
        # type: ( List[Tuple[str, Any, Callable]], Optional[List[str]]) -> None
        tx = self.get_graph().V()
        if node_labels:
            tx = tx.hasLabel(*node_labels)
        tx = NeptuneSessionClient.filter_traversal(tx, filter_properties)

        tx.drop().iterate()
