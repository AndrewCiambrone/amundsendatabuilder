import importlib
import logging
from typing import Any, Iterator, Union  # noqa: F401

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401

from databuilder.extractor.base_extractor import Extractor
from databuilder import neptune_client

LOGGER = logging.getLogger(__name__)


class NeptuneExtractor(Extractor):

    NEPTUNE_ENDPOINT_CONFIG_KEY = 'neptune_endpoint'
    NEPTUNE_PORT_CONFIG_KEY = 'neptune_port'

    # AWS Region
    REGION_CONFIG_KEY = 'region'

    AWS_ACCESS_KEY_CONFIG_KEY = 'aws_access_key'
    AWS_SECRET_KEY_CONFIG_KEY = 'aws_secret_key'

    GREMLIN_QUERY_CONFIG_KEY = 'gremlin_query'
    MODEL_CLASS_CONFIG_KEY = 'model_class'

    def init(self, conf):
        # type: (ConfigTree) -> None
        self.conf = conf.with_fallback(NeptuneExtractor.DEFAULT_CONFIG)
        self.aws_access_key = conf.get_string(NeptuneExtractor.AWS_ACCESS_KEY_CONFIG_KEY)
        self.aws_secret_key = conf.get_string(NeptuneExtractor.AWS_SECRET_KEY_CONFIG_KEY)
        self.aws_region = conf.get_string(NeptuneExtractor.REGION_CONFIG_KEY)
        self.neptune_endpoint = conf.get_string(NeptuneExtractor.NEPTUNE_ENDPOINT_CONFIG_KEY)
        self.neptune_port = conf.get_string(NeptuneExtractor.NEPTUNE_PORT_CONFIG_KEY)
        self.gremlin_query = conf.get_string(NeptuneExtractor.GREMLIN_QUERY_CONFIG_KEY)

        self._extract_iter = None  # type: Union[None, Iterator]

        model_class = conf.get(NeptuneExtractor.MODEL_CLASS_CONFIG_KEY, None)
        if model_class:
            module_name, class_name = model_class.rsplit(".", 1)
            mod = importlib.import_module(module_name)
            self.model_class = getattr(mod, class_name)

    def close(self):
        pass

    def _get_extract_iter(self):
        # type: () -> Iterator[Any]
        """
        Execute {cypher_query} and yield result one at a time
        """
        self.results = neptune_client.query_with_gremlin(
            neptune_endpoint=self.neptune_endpoint,
            neptune_port=self.neptune_port,
            region=self.aws_region,
            access_key=self.aws_access_key,
            secret_key=self.aws_secret_key,
            gremlin_query=self.gremlin_query
        )

        for result in self.results:
            if hasattr(self, 'model_class'):
                obj = self.model_class(**result)
                yield obj
            else:
                yield result

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