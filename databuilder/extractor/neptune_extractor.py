import time
import importlib
import logging
from typing import Any, Iterator, Union  # noqa: F401

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401

from databuilder.extractor.base_extractor import Extractor
from databuilder import neptune_client
from databuilder.clients.neptune_client import NeptuneSessionClient
from gremlin_python.process.graph_traversal import __
from gremlin_python.process.traversal import T

LOGGER = logging.getLogger(__name__)


class NeptuneExtractor(Extractor):

    NEPTUNE_HOST_CONFIG_KEY = 'neptune_host'

    # AWS Region
    REGION_CONFIG_KEY = 'region'

    AWS_ACCESS_KEY_CONFIG_KEY = 'aws_access_key'
    AWS_SECRET_KEY_CONFIG_KEY = 'aws_secret_key'
    AWS_SESSION_TOKEN_CONFIG_KEY = 'aws_session_token'

    GREMLIN_QUERY_CONFIG_KEY = 'gremlin_query'
    MODEL_CLASS_CONFIG_KEY = 'model_class'

    def init(self, conf):
        # type: (ConfigTree) -> None
        self.aws_access_key = conf.get_string(NeptuneExtractor.AWS_ACCESS_KEY_CONFIG_KEY)
        self.aws_secret_key = conf.get_string(NeptuneExtractor.AWS_SECRET_KEY_CONFIG_KEY)
        self.aws_session_token = conf.get_string(NeptuneExtractor.AWS_SESSION_TOKEN_CONFIG_KEY)
        self.aws_region = conf.get_string(NeptuneExtractor.REGION_CONFIG_KEY)
        self.neptune_host = conf.get_string(NeptuneExtractor.NEPTUNE_HOST_CONFIG_KEY)
        self._session_client = NeptuneSessionClient(
            neptune_host=self.neptune_host,
            region=self.aws_region,
            access_secret=self.aws_secret_key,
            access_key=self.aws_access_key,
            session_token=self.aws_session_token
        )

        self._extract_iter = None  # type: Union[None, Iterator]
        self.results = None

        model_class = conf.get(NeptuneExtractor.MODEL_CLASS_CONFIG_KEY, None)
        if model_class:
            module_name, class_name = model_class.rsplit(".", 1)
            mod = importlib.import_module(module_name)
            self.model_class = getattr(mod, class_name)

    def close(self):
        pass

    def _get_extract_iter(self):
        # type: () -> Iterator[Any]
        if not self.results:
            self._extract_data_from_neptune()
        for result in self.results:
            result['last_updated_timestamp'] = int(time.time())
            result['badges'] = []
            result['display_name'] = None
            result['programmatic_descriptions'] = []

            if hasattr(self, 'model_class'):
                obj = self.model_class(**result)
                yield obj
            else:
                yield result

    def _extract_data_from_neptune(self):
        g = self._session_client.get_graph()
        self.results = g.V().hasLabel('Table'). \
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

    def extract(self):
        # type: () -> Any
        """
        Return {result} object as it is or convert to object of
        {model_class}, if specified.
        """
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()

        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        # type: () -> str
        return 'extractor.neptune'