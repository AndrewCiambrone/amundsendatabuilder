import requests
import hashlib, hmac
from typing import Union, Optional, Dict, Any
from datetime import datetime
import urllib


class BulkUploaderNeptuneClient:
    def __init__(self, neptune_host, region, access_key, access_secret, session_token=None):
        # type: (str, str, str, str, Union[str, None]) -> None
        self.neptune_host = neptune_host
        self.region = region
        self.access_key = access_key
        self.access_secret = access_secret
        self.session_token = session_token

    def make_bulk_upload_request(self, bucket, s3_folder_location):
        # type: (str, str) -> str
        s3_source = "s3://{bucket}/{s3_folder_location}".format(
            bucket=bucket,
            s3_folder_location=s3_folder_location
        )
        payload = {
            "source": s3_source,
            "format": "csv",
            "region": self.region,
            "accessKey": self.access_key,
            "secretKey": self.access_secret,
            "updateSingleCardinalityProperties": "TRUE"
        }
        response_json = self._make_signed_request(
            method='POST',
            endpoint='/loader/',
            body_payload=payload
        )
        load_id = response_json.get('payload', {}).get('loadId')
        if load_id is None:
            print(response_json)
        return load_id

    def is_bulk_status_job_done(self, load_id):
        status_response = self.get_status_on_bulk_loader(load_id)
        status = status_response.get('payload', {}).get('overallStatus', {}).get('status')
        if not status:
            print("Ran into Error")
            print(repr(status_response))
        return status != 'LOAD_IN_PROGRESS', status

    def get_status_on_bulk_loader(self, load_id):
        query_params = {
            'loadId': load_id,
            'errors': True,
            'details': True
        }
        response_json = self._make_signed_request(
            method='GET',
            endpoint='/loader',
            query_params=query_params
        )

        return response_json

    def _make_signed_request(self, method, endpoint, query_params=None, body_payload=None):
        # type: (str, str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]) -> Dict[str, Any]

        if query_params is None:
            query_params = {}

        query_params = urllib.parse.urlencode(query_params, quote_via=urllib.parse.quote)
        query_params = query_params.replace('%27', '%22')
        query_params = BulkUploaderNeptuneClient._normalize_query_string(query_params)

        if body_payload is None:
            body_payload = {}

        body_payload = urllib.parse.urlencode(body_payload, quote_via=urllib.parse.quote)
        body_payload = body_payload.replace('%27', '%22')

        now = datetime.utcnow()
        amazon_date_str = now.strftime('%Y%m%dT%H%M%SZ')

        authorization_header = self._make_authorization_header(
            method=method,
            endpoint=endpoint,
            query_string=query_params,
            payload=body_payload,
            request_datetime=now,
        )

        full_url = "https://{host}{endpoint}".format(
            host=self.neptune_host,
            endpoint=endpoint
        )
        request_headers = {
            'x-amz-date': amazon_date_str,
            'Authorization': authorization_header
        }

        if self.session_token:
            request_headers['x-amz-security-token'] = self.session_token

        if method == 'POST':
            request_headers['content-type'] = 'application/x-www-form-urlencoded'
            response = requests.post(
                url=full_url,
                data=body_payload,
                verify=False,
                headers=request_headers
            )

        elif method == 'GET':
            response = requests.get(
                url=full_url,
                params=query_params,
                verify=False,
                headers=request_headers
            )
        else:
            raise Exception("HTTP METHOD: {0}".format(method))

        return response.json()

    def _make_authorization_header(
            self,
            method,
            endpoint,
            query_string,
            payload,
            request_datetime
    ):
        # type: (str, str, str, str, datetime) -> str

        algorithm = 'AWS4-HMAC-SHA256'
        date_stamp_str = request_datetime.strftime('%Y%m%d')
        datetime_str = request_datetime.strftime('%Y%m%dT%H%M%SZ')
        credential_scope = date_stamp_str + '/' + self.region + '/neptune-db/aws4_request'
        signed_headers = 'host;x-amz-date'
        signing_key = BulkUploaderNeptuneClient._get_signature_key(self.access_secret, date_stamp_str, self.region)

        canonical_headers = 'host:' + self.neptune_host + '\n' + 'x-amz-date:' + datetime_str + '\n'
        hashed_payload = hashlib.sha256(payload.encode('utf-8')).hexdigest()
        canonical_request = "{method}\n{endpoint}\n{querystring}\n{canonical_headers}\n{signed_headers}\n{hashed_payload}".format(
            method=method,
            endpoint=endpoint,
            querystring=query_string,
            canonical_headers=canonical_headers,
            signed_headers=signed_headers,
            hashed_payload=hashed_payload
        )

        hashed_request = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
        string_to_sign = "{algorithm}\n{amazon_date}\n{credential_scope}\n{hashed_request}".format(
            algorithm=algorithm,
            amazon_date=datetime_str,
            credential_scope=credential_scope,
            hashed_request=hashed_request
        )
        signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()
        return "{algorithm} Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}".format(
            algorithm=algorithm,
            access_key=self.access_key,
            credential_scope=credential_scope,
            signed_headers=signed_headers,
            signature=signature
        )

    @staticmethod
    def _normalize_query_string(query):
        # type: (str) -> str
        kv = (list(map(str.strip, s.split("=")))
              for s in query.split('&')
              if len(s) > 0)

        normalized = '&'.join('%s=%s' % (p[0], p[1] if len(p) > 1 else '')
                              for p in sorted(kv))
        return normalized

    @staticmethod
    def _get_signature_key(key, dateStamp, regionName):
        # type: (str, str, str) -> str
        kDate = BulkUploaderNeptuneClient._sign(('AWS4' + key).encode('utf-8'), dateStamp)
        kRegion = BulkUploaderNeptuneClient._sign(kDate, regionName)
        kService = BulkUploaderNeptuneClient._sign(kRegion, 'neptune-db')
        kSigning = BulkUploaderNeptuneClient._sign(kService, 'aws4_request')
        return kSigning

    @staticmethod
    def _sign(key, msg):
        # type: (str, str) -> str
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()
