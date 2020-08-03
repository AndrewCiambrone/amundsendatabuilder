import logging
from collections import namedtuple

from pyhocon import ConfigFactory, ConfigTree  # noqa: F401
from typing import Iterator, Union, Dict, Any  # noqa: F401

from databuilder import Scoped
from databuilder.extractor.base_extractor import Extractor
from databuilder.extractor.restapi.github_file_extractor import GithubFileExtractor
from databuilder.models.table_metadata import DescriptionMetadata
import lkml

LOGGER = logging.getLogger(__name__)


class LookerViewDescriptionsExtractor(Extractor):
    """
    Extracts Postgres users using SQLAlchemyExtractor
    """

    # CONFIG KEYS

    def init(self, conf):
        # type: (ConfigTree) -> None
        self.sql_stmt = self.sql_stmt.replace('%', '%%')

        self._github_file_extractor = GithubFileExtractor()
        github_file_extractor_conf = Scoped.get_scoped_conf(conf, self._github_file_extractor.get_scope())

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
        # type: () -> Iterator[User]
        """
        :return:
        """
        for file_contents in self._get_raw_extract_iter():
            model = lkml.load(file_contents)
            yield model

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