import logging
from databuilder.loader.base_loader import Loader
from databuilder.utils.closer import Closer
from databuilder import neptune_client



LOGGER = logging.getLogger(__name__)


class NeptuneUpserterLoader(Loader):
    # AWS Region
    REGION_CONFIG_KEY = 'region'

    AWS_ACCESS_KEY_CONFIG_KEY = 'aws_access_key'
    AWS_SECRET_KEY_CONFIG_KEY = 'aws_secret_key'
    NEPTUNE_ENDPOINT_CONFIG_KEY = 'neptune_endpoint'
    NEPTUNE_PORT_CONFIG_KEY = 'neptune_port'

    def __init__(self):
        # type: () -> None
        self._closer = Closer()

    def init(self, conf):
        self.conf = conf

    def load(self, record):
        # type: (Neo4jCsvSerializable) -> None
        pass

    def close(self):
        # type: () -> None
        """
        Any closeable callable registered in _closer, it will close.
        :return:
        """
        self._closer.close()

    def get_scope(self):
        # type: () -> str
        return "loader.neptune_upserter_loader"