import requests
from requests.auth import HTTPBasicAuth
from typing import List, Dict, Any


class GithubClient:
    def __init__(self, organization_name, github_username, github_access_token):
        # type: (str, str, str) -> None
        self.organization_name = organization_name
        self.github_username = github_username
        self.github_access_token = github_access_token
        self._request_auth = HTTPBasicAuth(self.github_username, self.github_access_token)
        self._request_headers = {
            "Accept": "application/vnd.github.v3+json"
        }

    def get_all_file_urls_in_directory(self, repo_name, directory):
        # type: (str, str) -> List[str]
        try:
            return self._get_all_file_urls_in_directory(repo_name, directory)
        except Exception as e:
            # handle unknown exception
            raise e

    def _get_all_file_urls_in_directory(self, repo_name,  directory):
        # type: (str, str) -> List[str]
        file_urls = []
        url = 'https://api.github.com/repos/{org_name}/{repo_name}/contents/{directory}'.format(
            org_name=self.organization_name,
            repo_name=repo_name,
            directory=directory
        )

        response = requests.get(
            url=url,
            headers=self._request_headers,
            auth=self._request_auth
        )
        print(response.status_code)
        if response.status_code != 200:
            print(url)
            print(response.content)
        response_json = response.json()
        for file_object in response_json:
            if _is_file_object_a_directory(file_object):
                file_object_path = file_object['path']
                subdirectory_files = self._get_all_file_urls_in_directory(repo_name, file_object_path)
                file_urls.extend(subdirectory_files)
            elif _is_file_object_a_file(file_object):
                file_object_url = file_object.get('download_url')
                file_urls.append(file_object_url)

        return file_urls

    def get_file_contents_from_url(self, file_url):
        response = requests.get(
            url=file_url,
            headers=self._request_headers,
            auth=self._request_auth
        )
        return response.content.decode(response.encoding)


def _is_file_object_a_directory(file_object):
    # type: (Dict[str, Any]) -> bool
    file_object_type = file_object['type']
    return file_object_type == 'dir'


def _is_file_object_a_file(file_object):
    # type: (Dict[str, Any]) -> bool
    file_object_type = file_object['type']
    return file_object_type == 'file'