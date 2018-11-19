import os
import re
from io import BytesIO
from logging import getLogger

from boto3.session import Session
from botocore.session import get_session

logger = getLogger(__name__)


def create_boto3_session_of_assume_role_delayed(profile_name=None):
    bc_session = get_session()
    session = Session(botocore_session=bc_session, profile_name=profile_name)
    cred_resolver = bc_session.get_component('credential_provider')  # type: CredentialResolver
    assume_role = cred_resolver.get_provider("assume-role")
    cred_resolver.remove("assume-role")
    cred_resolver.insert_after('shared-credentials-file', assume_role)
    return session


def get_s3_client():
    """

    :return:
    """
    session = create_boto3_session_of_assume_role_delayed()
    endpoint_url = os.environ.get('S3_ENDPOINT_URL')
    s3 = session.client('s3', endpoint_url=endpoint_url)
    return s3


def download_from_s3(s3, s3_path, local_path):
    logger.info('downloading %s to %s' % (s3_path, local_path))
    bucket_name, origin_path = split_bucket_and_key(s3_path)
    if bucket_name:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(bucket_name, origin_path, local_path)
        return local_path
    else:
        logger.error("invalid s3 path: %s" % s3_path)
        raise RuntimeError("invalid s3 path: %s" % s3_path)


def download_as_data_from_s3(s3, s3_path):
    logger.info(f'downloading {s3_path}')
    bucket_name, origin_path = split_bucket_and_key(s3_path)
    if not bucket_name:
        logger.error("invalid s3 path: %s" % s3_path)
        raise RuntimeError("invalid s3 path: %s" % s3_path)
    buffer = BytesIO()
    s3.download_fileobj(bucket_name, origin_path, buffer)
    ret = buffer.getvalue()
    buffer.close()
    return ret


def split_bucket_and_key(s3_path):
    """split `s3://bucket-name/path/to/file` -> (bucket-name, path/to/file)

    :param s3_path:
    :return:
    """
    matcher = re.search('^s3://([^/]+)/(.+)', s3_path)
    if matcher:
        return matcher.group(1), matcher.group(2)
    else:
        return None, None


def upload_file_to_s3(s3, local_path, s3_path):
    logger.info(f'upload {local_path} to {s3_path}')
    bucket_name, key = split_bucket_and_key(s3_path)
    s3.upload_file(local_path, bucket_name, key)


def upload_fileobj_to_s3(s3, fileobj, s3_path):
    logger.info(f'upload fileobj to {s3_path}')
    bucket_name, key = split_bucket_and_key(s3_path)
    s3.upload_fileobj(fileobj, bucket_name, key)


def list_paths(s3, s3_path):
    bucket_name, key = split_bucket_and_key(s3_path)
    response = s3.list_objects(Bucket=bucket_name, Prefix=key)
    return [x["Key"] for x in response.get("Contents") or []]
