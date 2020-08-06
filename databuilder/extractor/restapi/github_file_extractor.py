import logging
import importlib
from typing import Iterator, Any, Union  # noqa: F401
from urllib.parse import urlparse

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
    EXPECTED_FILE_EXTENSIONS = 'expected_file_extensions'

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

        self.expected_file_extensions = conf.get_list(self.EXPECTED_FILE_EXTENSIONS)

        self._extract_iter = None  # type: Union[None, Iterator]
        self.file_urls = None

    def extract(self):
        # type: () -> Any

        """
        Fetch one result row from RestApiQuery, convert to {model_class} if specified before
        returning.
        :return:
        """
        if not self._extract_iter:
            self._extract_iter = self._get_extract_iter()

        try:
            file_url = next(self._get_extract_iter())
            file_contents = self._client.get_file_contents_from_url(file_url)
            yield file_contents
        except StopIteration:
            return None

    def _get_extract_iter(self):
        if not self.file_urls:
            self.file_urls = self._client.get_all_file_urls_in_directory(
                self.repo_name,
                self.repo_directory
            )
            self.file_urls = iter([file_url for file_url in self.file_urls if self.wanted_file_type(file_url)])

        return self.file_urls

    def get_scope(self):
        # type: () -> str
        return 'extractor.github_file_extractor'

    def wanted_file_type(self, file_url):
        file_path = urlparse(file_url).path
        return file_path.endswith(tuple(self.expected_file_extensions))

