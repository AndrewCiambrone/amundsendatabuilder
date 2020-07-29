from datetime import datetime, timedelta
import requests
from typing import List, Dict, Any

# WIP not being used for anything yet
class LookerClient:
    def __init__(self, base_url, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.base_url = base_url
        self.access_token = None
        self.access_token_expiration_datetime = None

    def login(self):
        login_url = "{base_url}/3.1/login".format(
            base_url=self.base_url
        )
        response = requests.post(
            url=login_url,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )

        response_json = response.json()
        self.access_token = response_json.get('access_token')
        self.access_token_expiration_datetime = datetime.utcnow() + timedelta(seconds=int(response_json.get('expires_in')))

    def refresh_auth_token_if_needed(self):
        utc_now = datetime.utcnow()
        if utc_now > self.access_token_expiration_datetime - timedelta(seconds=1):
            self.login()

    def get_all_lookml_models(self) -> List[Dict[str, Any]]:
        self.refresh_auth_token_if_needed()
        get_all_lookml_models_url = "{base_url}/3.1/lookml_models".format(
            base_url=self.base_url
        )
        headers = {
            "Authorization": "Token {token}".format(token=self.access_token)
        }
        response = requests.get(
            url=get_all_lookml_models_url,
            headers=headers
        )
        response_json = response.json()
        lookml_models = []
        for record in response_json:
            explore_names = [explore['name'] for explore in record['explores']]
            lookml_models.append({
                'name': record['name'],
                'explore_names': explore_names
            })
        return lookml_models

    def get_all_table_and_column_descriptions_for_explore(self, explore_name, lookml_name):
        get_lookml_explore_url = "{base_url}/3.1/lookml_models/{lookml_name}/explores/{explore_name}".format(
            base_url=self.base_url,
            lookml_name=lookml_name,
            explore_name=explore_name
        )
        headers = {
            "Authorization": "Token {token}".format(token=self.access_token)
        }
        response = requests.get(
            url=get_lookml_explore_url,
            headers=headers
        )
        response_json = response.json()
        explore_tables_schema = response_json['sql_table_name'].split(' ')[0].split('.')[0]
        explore_tables_name = response_json['sql_table_name'].split(' ')[0].split('.')[1]
        explore_description = response_json['description']
        core_field_name = "{explore}.core_fields".format(explore=explore_name)
        core_fields = set([explore_set['value'] for explore_set in response_json['sets'] if explore_set['name'] == core_field_name])
        dimensions = response_json['fields']['dimensions']
        column_descriptions = [
            {
                'name': dimension['name'],
                'description': dimension['description']
            }
            for dimension in dimensions
            if dimension['name'] in core_fields
        ]

        return {
            'schema_name': explore_tables_schema,
            'table_name': explore_tables_name,
            'table_description': explore_description
        }



