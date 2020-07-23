import textwrap
from typing import Any  # noqa: F401

from pyhocon import ConfigTree  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.neptune_extractor import NeptuneExtractor


class NeptuneSearchDataExtractor(Extractor):

    ENTITY_TYPE = 'entity_type'
    GREMLIN_QUERY_CONFIG_KEY = 'cypher_query'
    DEFAULT_TABLE_QUERY = """
        g.V().hasLabel('Database').
            project('db_name', 'cluster_name', 'schema_name', 'tables').
                by('name').
                by(out('CLUSTER').values('name')).
                by(out('CLUSTER').out('SCHEMA').values('name')).
                by(out('CLUSTER').out('SCHEMA').out('TABLE').
                    project('table_id', 'table_name', 'columns').
                        by(id).
                        by('name').
                        by(out('COLUMN').values('name').fold()).
                    fold())
    """

    DEFAULT_QUERY_BY_ENTITY = {
        'table': DEFAULT_TABLE_QUERY
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