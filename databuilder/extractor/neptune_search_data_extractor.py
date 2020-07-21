import textwrap
from typing import Any  # noqa: F401

from pyhocon import ConfigTree  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.neptune_extractor import NeptuneExtractor
from databuilder.publisher.neo4j_csv_publisher import JOB_PUBLISH_TAG


class NeptuneSearchDataExtractor(Extractor):

    ENTITY_TYPE = 'entity_type'

    def init(self, conf):
        self.conf = conf
        self.entity = conf.get_string(NeptuneSearchDataExtractor.ENTITY_TYPE, default='table').lower()
        pass


    def close(self):
        # type: () -> Any
        pass

    def get_scope(self):
        # type: () -> str
        return 'extractor.search_data'