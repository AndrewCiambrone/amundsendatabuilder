import requests
import sys, datetime, hashlib, hmac
from datetime import datetime
import urllib
import os
import json
from typing import Optional, Dict, Any


def make_bulk_upload_request(neptune_endpoint, neptune_port, bucket, s3_folder_location, region, access_key, secret_key):
    # type: (str, str, str, str, str, str, str) -> None
    s3_source = "s3://{bucket}/{s3_folder_location}".format(
        bucket=bucket,
        s3_folder_location=s3_folder_location
    )
    payload = {
        "source": s3_source,
        "format": "csv",
        "region": region,
        "accessKey": access_key,
        "secretKey": secret_key
    }
    neptune_host = "{}:{}".format(neptune_endpoint, neptune_port)
    response_json = make_signed_request(
        method='POST',
        host=neptune_host,
        endpoint='/loader/',
        aws_access_key=access_key,
        aws_secret_key=secret_key,
        aws_region=region,
        body_payload=payload
    )


def normalize_query_string(query):
    # type: (str) -> str
    kv = (list(map(str.strip, s.split("=")))
          for s in query.split('&')
          if len(s) > 0)

    normalized = '&'.join('%s=%s' % (p[0], p[1] if len(p) > 1 else '')
                          for p in sorted(kv))
    return normalized


def sign(key, msg):
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()


def get_signature_key(key, dateStamp, regionName):
    kDate = sign(('AWS4' + key).encode('utf-8'), dateStamp)
    kRegion = sign(kDate, regionName)
    kService = sign(kRegion, 'neptune-db')
    kSigning = sign(kService, 'aws4_request')
    return kSigning


def make_authorization_header(method, host, canonical_uri, canonical_querystring, canonical_headers,  aws_access_key, aws_secret_key, aws_region, request_datetime):
    # type: (str, str, str, str, str, str, str, str, datetime) -> str

    algorithm = 'AWS4-HMAC-SHA256'
    date_stamp_str = request_datetime.strftime('%Y%m%d')
    datetime_str = request_datetime.strftime('%Y%m%dT%H%M%SZ')
    credential_scope = date_stamp_str + '/' + aws_region + '/neptune-db/aws4_request'
    signed_headers = 'host;x-amz-date'
    signing_key = get_signature_key(aws_secret_key, date_stamp_str, aws_region)
    canonical_request = method + '\n' + canonical_uri + '\n' + canonical_querystring + '\n' + canonical_headers + '\n' + signed_headers + '\n' + payload_hash
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
        access_key=aws_access_key,
        credential_scope=credential_scope,
        signed_headers=signed_headers,
        signature=signature
    )


def make_signed_request(method, host, endpoint, aws_access_key, aws_secret_key, aws_region, query_params=None, body_payload=None):
    # type: (str, str, str, str, str, str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]) -> Dict[str, Any]

    if query_params is None:
        query_params = ''


    if body_payload is None:
        body_payload = ''

    now = datetime.datetime.utcnow()
    amazon_date_str = now.strftime('%Y%m%dT%H%M%SZ')

    payload_hash = hashlib.sha256(body_payload.encode('utf-8')).hexdigest()

    authorization_header = make_authorization_header(
        host=host,
        canonical_uri=endpoint,
        aws_access_key=aws_access_key,
        aws_secret_key=aws_secret_key,
        request_datetime=now,
        aws_region=aws_region

    )

    full_url = "https://{host}{endpoint}".format(
        host=host,
        endpoint=endpoint
    )
    request_query_params = None
    request_body_payload = None
    request_headers = {
        'x-amz-date': amazon_date_str,
        'Authorization': authorization_header
    }

    if method == 'POST':
        request_headers['content-type'] = 'application/x-www-form-urlencoded'
        response = requests.post(
            url=full_url,
            data=request_body_payload,
            verify=False,
            headers=request_headers
        )

    elif method == 'GET':
        response = requests.get(
            url=full_url,
            params=request_query_params,
            verify=False,
            headers=request_headers
        )
    else:
        raise Exception("HTTP METHOD: {0}".format(method))

    return response.json()

