from typing import Any, List  # noqa: F401

from pyhocon import ConfigTree  # noqa: F401

import time
from databuilder.extractor.base_extractor import Extractor
from gremlin_python.process.graph_traversal import __
from databuilder.clients.neptune_client import NeptuneSessionClient
from databuilder.models.table_metadata import (
    TableMetadata,
    TagMetadata,
    DescriptionMetadata,

)
from databuilder.models.column_usage_model import ColumnUsageModel
from databuilder.models.schema.schema_constant import (
    SCHEMA_REVERSE_RELATION_TYPE
)
from databuilder.models.cluster.cluster_constants import (
    CLUSTER_REVERSE_RELATION_TYPE
)
from databuilder import Scoped
from databuilder.serializers.neptune_serializer import (
    NEPTUNE_LAST_SEEN_AT_NODE_PROPERTY_NAME
)


class NeptuneSearchDataExtractor(Extractor):

    ENTITY_TYPE = 'entity_type'
    GREMLIN_QUERY_CONFIG_KEY = 'cypher_query'

    def _table_search_query(self):
        # type: () -> List[Any]
        graph = self._session_client.get_graph()
        schema_traversal = __.out(TableMetadata.TABLE_SCHEMA_RELATION_TYPE)
        cluster_traversal = schema_traversal.out(SCHEMA_REVERSE_RELATION_TYPE)
        db_traversal = cluster_traversal.out(CLUSTER_REVERSE_RELATION_TYPE)
        column_traversal = __.out(TableMetadata.TABLE_COL_RELATION_TYPE)
        traversal = graph.V().hasLabel(TableMetadata.TABLE_NODE_LABEL)
        traversal = traversal.project(
            'database',
            'cluster',
            'schema',
            'schema_description',
            'name',
            'key',
            'description',
            'column_names',
            'column_descriptions',
            'total_usage',
            'unique_usage',
            'tags',
            'last_updated_timestamp'
        )
        traversal = traversal.by(db_traversal.values('name'))  # database
        traversal = traversal.by(cluster_traversal.values('name'))  # cluster
        traversal = traversal.by(schema_traversal.values('name'))  # schema
        traversal = traversal.by(__.coalesce(
            schema_traversal.out(DescriptionMetadata.DESCRIPTION_RELATION_TYPE).values('description'),
            __.constant(''))
        )  # schema_description
        traversal = traversal.by('name')  # name
        traversal = traversal.by(self._session_client.id_property_name)  # key
        traversal = traversal.by(__.coalesce(
            __.out(DescriptionMetadata.DESCRIPTION_RELATION_TYPE).values('description'),
            __.constant(''))
        )  # description
        traversal = traversal.by(column_traversal.values('name').fold())  # column_names
        traversal = traversal.by(
            column_traversal.out(DescriptionMetadata.DESCRIPTION_RELATION_TYPE).values('description').fold()
        )  # column_descriptions
        traversal = traversal.by(__.coalesce(
            __.outE(ColumnUsageModel.TABLE_USER_RELATION_TYPE).values('read_count'),
            __.constant(0)).sum()
        )  # total_usage
        traversal = traversal.by(__.outE(ColumnUsageModel.TABLE_USER_RELATION_TYPE).count())  # unique_usage
        traversal = traversal.by(__.inE(TableMetadata.TAG_TABLE_RELATION_TYPE).outV().id().fold())  # tags
        traversal = traversal.by(__.coalesce(__.values(NEPTUNE_LAST_SEEN_AT_NODE_PROPERTY_NAME)))  # last_updated_timestamp
        return traversal.toList()

    DEFAULT_QUERY_BY_ENTITY = {
        'table': _table_search_query
    }

    def init(self, conf):
        # type: (ConfigTree) -> None
        self.conf = conf
        self.entity = conf.get_string(NeptuneSearchDataExtractor.ENTITY_TYPE, default='table').lower()

        self._session_client = NeptuneSessionClient()
        neptune_client_conf = Scoped.get_scoped_conf(conf, self._session_client.get_scope())
        self._session_client.init(neptune_client_conf)

        self._extract_iter = None
        self.results = None

    def close(self):
        # type: () -> Any
        self.neptune_extractor.close()

    def extract(self):
        # type: () -> Any
        """
        Invoke extract() method defined by neptune_extractor
        """
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()

        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def _get_extract_iter(self):
        if not self.results:
            self.results = self.DEFAULT_QUERY_BY_ENTITY[self.entity]()

        for result in self.results:
            result['badges'] = []
            result['display_name'] = None
            result['programmatic_descriptions'] = []

            if hasattr(self, 'model_class'):
                obj = self.model_class(**result)
                yield obj
            else:
                yield result

    def get_scope(self):
        # type: () -> str
        return 'extractor.search_data'