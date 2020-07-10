import boto3
from typing import IO, Optional


_s3_client = None # type: Optional[boto3.S3.Client]


def _get_s3_client():  # type: () -> boto3.S3.Client
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.resource("s3")
    return _s3_client


def upload_file( bucket_name, path, file_to_upload):  # type: (str, str, IO[bytes]) -> None
    bucket = _get_s3_client().Bucket(bucket_name)
    bucket.upload_fileobj(Fileobj=file_to_upload, Key=path, ExtraArgs={"ACL": "private"})


def upload_content(bucket, path, contents):  # (str, str, bytes) -> None
    s3_bucket = _get_s3_client().Bucket(bucket)
    s3_bucket.put_object(Body=contents, Key=path, ACL="private")


