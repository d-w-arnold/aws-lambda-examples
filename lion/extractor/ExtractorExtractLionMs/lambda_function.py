import json
import logging
import os
import shutil
import sys
import tempfile
from pathlib import Path

import urllib3

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3
from sih_lion import __version__ as sih_lion_version
from sih_lion.extractor import Extractor
from sih_lion.file_utils import FileMetaReader, FileMetaReaderSettings

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")
logger.info(f"sih-lion version: {sih_lion_version}")

ACCOUNT_OWNER_ID = "ACCOUNT_OWNER_ID"
LION_GLOBAL_AWS_REGION = "LION_GLOBAL_AWS_REGION"
PYPI_PACKAGE_S3_BUCKET_NAME = "PYPI_PACKAGE_S3_BUCKET_NAME"
PYPI_PACKAGE_S3_BUCKET_BRANCH = "PYPI_PACKAGE_S3_BUCKET_BRANCH"
CACHE_SQUARE_SOURCE_NAMES = "CACHE_SQUARE_SOURCE_NAMES"
CACHE_SQUARE_CODE = "CACHE_SQUARE_CODE"
CACHE_SQUARE_META = "CACHE_SQUARE_META"
REDIS_HOST = "REDIS_HOST"
REDIS_PORT = "REDIS_PORT"
REDIS_SSL = "REDIS_SSL"
REDIS_PW_SECRET = "REDIS_PW_SECRET"
REDIS_DECODE_RESPONSES = "REDIS_DECODE_RESPONSES"
FILENAMES_INFO = "FILENAMES_INFO"

AWS_SESSION_TOKEN = "AWS_SESSION_TOKEN"
PARAMETERS_SECRETS_EXTENSION_CACHE_ENABLED = "PARAMETERS_SECRETS_EXTENSION_CACHE_ENABLED"
PARAMETERS_SECRETS_EXTENSION_CACHE_SIZE = "PARAMETERS_SECRETS_EXTENSION_CACHE_SIZE"
PARAMETERS_SECRETS_EXTENSION_HTTP_PORT = "PARAMETERS_SECRETS_EXTENSION_HTTP_PORT"
PARAMETERS_SECRETS_EXTENSION_LOG_LEVEL = "PARAMETERS_SECRETS_EXTENSION_LOG_LEVEL"
PARAMETERS_SECRETS_EXTENSION_MAX_CONNECTIONS = "PARAMETERS_SECRETS_EXTENSION_MAX_CONNECTIONS"
SECRETS_MANAGER_TIMEOUT_MILLIS = "SECRETS_MANAGER_TIMEOUT_MILLIS"
SECRETS_MANAGER_TTL = "SECRETS_MANAGER_TTL"
SSM_PARAMETER_STORE_TIMEOUT_MILLIS = "SSM_PARAMETER_STORE_TIMEOUT_MILLIS"
SSM_PARAMETER_STORE_TTL = "SSM_PARAMETER_STORE_TTL"

TMP_FOP = "TMP_FOP"

status_str = "status"
responses_str = "responses"

extractor_str = "extractor"
atmos_param_str = "atmos_param"
static_grid_str = "static_grid"
static_grids_str = f"{static_grid_str}s"

http = urllib3.PoolManager()


# Define function to retrieve values from extension local HTTP server cache
def retrieve_extension_value(url: str) -> dict:
    url = f"http://localhost:{os.environ[PARAMETERS_SECRETS_EXTENSION_HTTP_PORT]}{url}"
    res = http.request("GET", url, headers={"X-Aws-Parameters-Secrets-Token": os.environ[AWS_SESSION_TOKEN]})
    return json.loads(res.data)


# Load Secrets Manager values from extension
def retrieve_extension_value_secret(secret_id: str) -> str:
    logger.info(f"## Loading AWS Secrets Manager value: {secret_id}")
    secretsmanager_res = retrieve_extension_value(f"/secretsmanager/get?secretId={secret_id}")
    secretsmanager_res_obs = {k: v if k != "SecretString" else "****" for k, v in dict(secretsmanager_res).items()}
    logger.info(f"## Secrets Manager Get Secret Value (extension) response: {secretsmanager_res_obs}")
    return secretsmanager_res["SecretString"]


def s3_download_fileobj(s3, bucket_name: str, obj_key: str, filename: str = None) -> str:
    if filename is None:
        filename = os.path.join(os.environ[TMP_FOP], bucket_name, obj_key)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "wb") as f:
        s3.download_fileobj(bucket_name, obj_key, f)
    return filename


def s3_list_objects(s3, bucket_name: str, prefix_path: str, continuation_token: str = None) -> dict:
    logger.info(f"## Listing S3 objects in: s3://{bucket_name}/{prefix_path}")
    list_objects_v2_kwargs = {
        k: v
        for k, v in {
            "Bucket": bucket_name,
            "MaxKeys": 1000,
            "Prefix": prefix_path,
            "ExpectedBucketOwner": os.environ[ACCOUNT_OWNER_ID],
            "ContinuationToken": continuation_token if continuation_token else None,
        }.items()
        if v
    }
    s3_res = s3.list_objects_v2(**list_objects_v2_kwargs)
    logger.debug(f"## S3 List Objects V2 response: {s3_res}")
    return s3_res


def clear_tmp_directory(tmp_fop: str):
    # pylint: disable=expression-not-assigned
    tmp_directory = Path(tmp_fop)
    # Remove all files
    [fp.unlink() for fp in tmp_directory.glob("*") if fp.is_file() or fp.is_symlink()]
    # Remove all directories
    [shutil.rmtree(dp) for dp in tmp_directory.glob("*") if dp.is_dir()]


def lambda_handler(event, context):
    env_keys = {
        ACCOUNT_OWNER_ID,
        LION_GLOBAL_AWS_REGION,
        PYPI_PACKAGE_S3_BUCKET_NAME,
        PYPI_PACKAGE_S3_BUCKET_BRANCH,
        CACHE_SQUARE_SOURCE_NAMES,
        CACHE_SQUARE_CODE,
        CACHE_SQUARE_META,
        REDIS_HOST,
        REDIS_PORT,
        REDIS_SSL,
        REDIS_PW_SECRET,
        REDIS_DECODE_RESPONSES,
        FILENAMES_INFO,
        AWS_SESSION_TOKEN,
        PARAMETERS_SECRETS_EXTENSION_CACHE_ENABLED,
        PARAMETERS_SECRETS_EXTENSION_CACHE_SIZE,
        PARAMETERS_SECRETS_EXTENSION_HTTP_PORT,
        PARAMETERS_SECRETS_EXTENSION_LOG_LEVEL,
        PARAMETERS_SECRETS_EXTENSION_MAX_CONNECTIONS,
        SECRETS_MANAGER_TIMEOUT_MILLIS,
        SECRETS_MANAGER_TTL,
        SSM_PARAMETER_STORE_TIMEOUT_MILLIS,
        SSM_PARAMETER_STORE_TTL,
    }
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    logger.info(f"## EVENT: {event}")

    os.environ[TMP_FOP] = tempfile.gettempdir()
    logger.info(f"Temporary folder path: {os.environ[TMP_FOP]}")

    secretsmanager_secret_string = retrieve_extension_value_secret(secret_id=os.environ["REDIS_PW_SECRET"])
    os.environ["REDIS_PASSWORD"] = json.loads(secretsmanager_secret_string)["password"]
    logger.info("## Redis password set")

    extractor = Extractor(logger)

    settings = FileMetaReaderSettings()
    logger.info(f"## FileMetaReaderSettings: '{settings.filenames_info}'")
    file_meta_reader = FileMetaReader(settings)

    event_bucket_name: str = event["detail"]["bucket"]["name"]
    event_obj_key: str = event["detail"]["object"]["key"]
    # event_obj_etag: str = event["detail"]["object"]["etag"]
    logger.info(f"## Event details: s3://{event_bucket_name}/{event_obj_key}")

    meta = file_meta_reader.find_filename_meta(extractor_str, event_obj_key.rsplit(sep="/", maxsplit=1)[1])
    logger.info(f"## File Meta: '{meta}'")

    meta_source_name: str = meta["source_name"]
    is_valid_meta_source_name: bool = False
    for i in json.loads(os.environ[CACHE_SQUARE_SOURCE_NAMES]):
        if meta_source_name == i:
            logger.info(f"## File Meta source name: '{meta_source_name}'")
            is_valid_meta_source_name = True
            break
    if not is_valid_meta_source_name:
        logger.info(f"## Skipping File Meta source name: {meta_source_name}")
        return {status_str: "SKIPPING"}

    static_grid_bucket_name = os.environ[PYPI_PACKAGE_S3_BUCKET_NAME]
    static_grid_prefix_path = (
        f"{os.environ[PYPI_PACKAGE_S3_BUCKET_BRANCH]}/sih_lion/{static_grids_str}/{extractor_str}/{meta_source_name}"
    )
    static_grid_obj_name_prefix = (
        f"{str(os.environ[CACHE_SQUARE_CODE]).lower()}_{meta['atmos_param_short']}_{meta_source_name}_"
    )

    s3 = boto3.client("s3", region_name=os.environ[LION_GLOBAL_AWS_REGION])
    logger.info("## Connected to S3 via client")

    meta_extension: str = ext if (ext := meta["extension"]) and ext.startswith(".") else f".{ext}"
    static_grid_obj_keys = sorted(
        [
            obj["Key"]
            for obj in s3_list_objects(
                s3,
                static_grid_bucket_name,
                static_grid_prefix_path,
            )["Contents"]
            if (key := str(obj["Key"]).rsplit(sep="/", maxsplit=1)[-1])
            and key.endswith(meta_extension)
            and key.startswith(static_grid_obj_name_prefix)
        ],
        reverse=True,
    )

    s3_uri_static_grids: str = (
        f"s3://{static_grid_bucket_name}/{static_grid_prefix_path}/{static_grid_obj_name_prefix}*{meta_extension}"
    )

    if static_grid_obj_keys:
        logger.info(f"## Found static grid files: {s3_uri_static_grids} '{static_grid_obj_keys}'")

        static_grid_obj_key = static_grid_obj_keys[0]
        extract_input = {
            atmos_param_str: s3_download_fileobj(
                s3,
                bucket_name=event_bucket_name,
                obj_key=event_obj_key,
                filename=os.path.join(os.environ[TMP_FOP], event_obj_key.rsplit(sep="/", maxsplit=1)[-1]),
            ),
            static_grid_str: s3_download_fileobj(
                s3,
                bucket_name=static_grid_bucket_name,
                obj_key=static_grid_obj_key,
                filename=os.path.join(os.environ[TMP_FOP], static_grid_obj_key.rsplit(sep="/", maxsplit=1)[-1]),
            ),
        }

        logger.info(f"## Extract Input: '{extract_input}'")

        extractor.extract(param_data_fp=extract_input[atmos_param_str], static_grid_fp=extract_input[static_grid_str])

        logger.info(f"## Clear the temporary folder path: {os.environ[TMP_FOP]}")
        clear_tmp_directory(os.environ[TMP_FOP])
        logger.info("## Temporary folder path cleared")

        return {status_str: "SUCCEEDED"}

    logger.info(f"## No static grid files found: {s3_uri_static_grids}")

    return {status_str: "SKIPPING"}
