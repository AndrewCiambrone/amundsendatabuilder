import time
import importlib
import logging
from typing import Any, Iterator, Union  # noqa: F401

from pyhocon import ConfigTree, ConfigFactory  # noqa: F401

from databuilder.extractor.base_extractor import Extractor
from databuilder import neptune_client

LOGGER = logging.getLogger(__name__)


class NeptuneExtractor(Extractor):

    NEPTUNE_HOST_CONFIG_KEY = 'neptune_host'

    # AWS Region
    REGION_CONFIG_KEY = 'region'

    AWS_ACCESS_KEY_CONFIG_KEY = 'aws_access_key'
    AWS_SECRET_KEY_CONFIG_KEY = 'aws_secret_key'
    AWS_SESSION_TOKEN = 'aws_session_token'

    GREMLIN_QUERY_CONFIG_KEY = 'gremlin_query'
    MODEL_CLASS_CONFIG_KEY = 'model_class'

    def init(self, conf):
        # type: (ConfigTree) -> None
        self.aws_access_key = conf.get_string(NeptuneExtractor.AWS_ACCESS_KEY_CONFIG_KEY)
        self.aws_secret_key = conf.get_string(NeptuneExtractor.AWS_SECRET_KEY_CONFIG_KEY)
        self.aws_session_token = conf.get_string(NeptuneExtractor.AWS_SESSION_TOKEN)
        self.aws_region = conf.get_string(NeptuneExtractor.REGION_CONFIG_KEY)
        self.neptune_host = conf.get_string(NeptuneExtractor.NEPTUNE_HOST_CONFIG_KEY)
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
        auth_dict = {
            'aws_access_key_id': self.aws_access_key,
            'aws_secret_access_key': self.aws_secret_key,
            'service_region': self.aws_region
        }
        extra_options = {}
        if self.aws_session_token:
            extra_options['session_token'] = self.aws_session_token
        g = neptune_client.get_graph(
            host=self.neptune_host,
            password=auth_dict,
            aws4auth_options=extra_options
        )
        self.results = neptune_client.get_table_info_for_search(g)
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