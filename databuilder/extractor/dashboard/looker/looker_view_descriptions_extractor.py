import logging
from collections import namedtuple

from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from typing import Iterator, Union, Dict, Any  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.restapi.github_file_extractor import GithubFileExtractor
from databuilder.models.table_metadata import DescriptionMetadata, ColumnMetadata
import lkml

LOGGER = logging.getLogger(__name__)


class LookerViewDescriptionsExtractor(Extractor):
    """
    Extracts Looker Descriptions using GithubFileExtractor
    """

    METADATA_SOURCE = 'looker'

    # CONFIG KEYS
    LOOKER_TABLE_SOURCE_DATABASE = 'LOOKER_TABLE_SOURCE_DATABASE'
    LOOKER_TABLE_SOURCE_CLUSTER = 'LOOKER_TABLE_SOURCE_CLUSTER'

    def init(self, conf):
        # type: (ConfigTree) -> None

        self._github_file_extractor = GithubFileExtractor()
        github_file_extractor_conf = Scoped.get_scoped_conf(conf, self._github_file_extractor.get_scope())

        self.source_database = conf.get_string(self.LOOKER_TABLE_SOURCE_DATABASE)
        self.source_cluster = conf.get_string(self.LOOKER_TABLE_SOURCE_CLUSTER)

        self._github_file_extractor.init(github_file_extractor_conf)
        self._extract_iter = None  # type: Union[None, Iterator]

    def extract(self):
        # type: () -> Union[User, None]
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()
        try:
            return next(self._extract_iter)
        except StopIteration:
            return None

    def get_scope(self):
        # type: () -> str
        return 'extractor.looker_view_descriptions_extractor'

    def _get_extract_iter(self):
        # type: () -> Iterator[DescriptionMetadata]
        """
        :return:
        """
        for file_contents in self._get_raw_extract_iter():
            model = lkml.load(file_contents)
            for view in model.get('views', []):
                sql_table_name = view['sql_table_name']
                schema = sql_table_name.split('.')[0]
                table_name = sql_table_name.split('.')[1]
                for dimension in view.get('dimensions', []):
                    column_name = dimension['name']
                    column_description = dimension['description']
                    column_key = ColumnMetadata.COLUMN_KEY_FORMAT.format(
                        db=self.source_database,
                        cluster=self.source_cluster,
                        schema=schema,
                        tbl=table_name,
                        col=column_name
                    )
                    yield DescriptionMetadata(
                        text=column_description,
                        source=self.METADATA_SOURCE,
                        description_owner_key=column_key
                    )

    def _get_raw_extract_iter(self):
        # type: () -> Iterator[Any]
        """
        Provides iterator of result row from the github file
        :return:
        """
        file_contents = self._github_file_extractor.extract()
        while file_contents:
            yield file_contents
            file_contents = self._github_file_extractor.extract()