from requests import Session
from requests import request


def make_bulk_upload_request(neptune_endpoint, neptune_port, bucket, s3_folder_location, region, access_key, secret_key):
    # type: (str, str, str, str) -> None
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
    url = "{}:{}/loader".format(neptune_endpoint, neptune_port)
    req = request(
        method="POST",
        url=url,
        data=payload
    )
    s = Session()
    response = s.send(req)

