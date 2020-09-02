from typing import Any, List  # noqa: F401

from pyhocon import ConfigTree  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.neptune_extractor import NeptuneExtractor
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


class NeptuneSearchDataExtractor(Extractor):

    ENTITY_TYPE = 'entity_type'
    GREMLIN_QUERY_CONFIG_KEY = 'cypher_query'

    def _table_search_query(self, session_client):
        # type: (NeptuneSessionClient) -> List[Any]
        graph = self._session_client.get_graph()
        schema_traversal = __.out(TableMetadata.TABLE_SCHEMA_RELATION_TYPE)
        cluster_traversal = schema_traversal.out(SCHEMA_REVERSE_RELATION_TYPE)
        db_traversal = cluster_traversal.out(CLUSTER_REVERSE_RELATION_TYPE)
        column_traversal = __.out(TableMetadata.TABLE_COL_RELATION_TYPE)
        traversal = graph.hasLabel(TableMetadata.TABLE_NODE_LABEL)
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
            'tags'
        )
        traversal = traversal.by(db_traversal.values('name'))  # database
        traversal = traversal.by(cluster_traversal.values('name'))  # cluster
        traversal = traversal.by(schema_traversal.values('name'))  # schema
        traversal = traversal.by(__.coalesce(
            schema_traversal.out(DescriptionMetadata.DESCRIPTION_RELATION_TYPE).values('description'),
            __.constant(''))
        )  # schema_description
        traversal = traversal.by('name')  # name
        traversal = traversal.by(session_client.key_name)  # key
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
        return traversal.toList()

    DEFAULT_QUERY_BY_ENTITY = {
        'table': _table_search_query
    }

    def init(self, conf):
        # type: (ConfigTree) -> None
        self.conf = conf
        self.entity = conf.get_string(NeptuneSearchDataExtractor.ENTITY_TYPE, default='table').lower()
        if NeptuneSearchDataExtractor.GREMLIN_QUERY_CONFIG_KEY in conf:
            self.gremlin_query = conf.get_string(NeptuneSearchDataExtractor.GREMLIN_QUERY_CONFIG_KEY)
        else:
            self.gremlin_query = NeptuneSearchDataExtractor.DEFAULT_QUERY_BY_ENTITY[self.entity]

        self.neptune_extractor = NeptuneExtractor()
        key = self.neptune_extractor.get_scope() + '.' + NeptuneExtractor.GREMLIN_QUERY_CONFIG_KEY
        self.conf.put(key, self.gremlin_query)

        self.neptune_extractor.init(Scoped.get_scoped_conf(self.conf, self.neptune_extractor.get_scope()))

    def close(self):
        # type: () -> Any
        self.neptune_extractor.close()

    def extract(self):
        # type: () -> Any
        """
        Invoke extract() method defined by neptune_extractor
        """
        return self.neptune_extractor.extract()

    def get_scope(self):
        # type: () -> str
        return 'extractor.search_data'