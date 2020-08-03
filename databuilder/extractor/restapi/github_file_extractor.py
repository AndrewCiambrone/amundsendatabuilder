import logging
import importlib
from typing import Iterator, Any  # noqa: F401

from pyhocon import ConfigTree  # noqa: F401

from databuilder.extractor.base_extractor import Extractor
from databuilder.rest_api.base_rest_api_query import BaseRestApiQuery  # noqa: F401
from databuilder.github_client import GithubClient


LOGGER = logging.getLogger(__name__)


class GithubFileExtractor(Extractor):
    """
    An Extractor that calls on github to extract files
    """

    GITHUB_ORG_NAME = 'github_org_name'
    GITHUB_USER_NAME = 'github_user_name'
    GITHUB_ACCESS_TOKEN = 'github_access_token'
    REPO_NAME = 'repo_name'
    REPO_DIRECTORY = 'repo_directory'

    def init(self, conf):
        # type: (ConfigTree) -> None
        self._iterator = None  # type: Iterator[Dict[str, Any]]
        self.github_org_name = conf.get_string(self.GITHUB_ORG_NAME)
        self.github_user_name = conf.get_string(self.GITHUB_USER_NAME)
        self.github_access_token = conf.get_string(self.GITHUB_ACCESS_TOKEN)
        self.repo_directory = conf.get_string(self.REPO_DIRECTORY)
        self.repo_name = conf.get_string(self.REPO_NAME)
        self._client = GithubClient(
            organization_name=self.github_org_name,
            github_username=self.github_user_name,
            github_access_token=self.github_access_token
        )

    def extract(self):
        # type: () -> Any

        """
        Fetch one result row from RestApiQuery, convert to {model_class} if specified before
        returning.
        :return:
        """
        file_urls = self._client.get_all_file_urls_in_directory()

        if not self._iterator:
            self._iterator = self._restapi_query.execute()

        try:
            record = next(self._iterator)
        except StopIteration:
            return None

        return record

    def get_scope(self):
        # type: () -> str
        return 'extractor.github_file_extractor'
