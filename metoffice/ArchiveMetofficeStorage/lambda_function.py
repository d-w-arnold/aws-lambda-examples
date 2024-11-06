import json
import logging
import os
import sys
from datetime import datetime

import urllib3

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

# boto3.setup_default_session(profile_name="innovation")  # Enable to use alt AWS profile

ACCOUNT_OWNER_ID = "ACCOUNT_OWNER_ID"
BUCKET_NAME_SOURCE = "BUCKET_NAME_SOURCE"
BUCKET_NAME_DEST_PREFIX = "BUCKET_NAME_DEST_PREFIX"
STATE_PARAMETER = "STATE_PARAMETER"
ARCHIVE_BYTE_COUNT = "ARCHIVE_BYTE_COUNT"

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

http = urllib3.PoolManager()


# Define function to retrieve values from extension local HTTP server cache
def retrieve_extension_value(url: str) -> dict:
    url = f"http://localhost:{os.environ[PARAMETERS_SECRETS_EXTENSION_HTTP_PORT]}{url}"
    res = http.request("GET", url, headers={"X-Aws-Parameters-Secrets-Token": os.environ[AWS_SESSION_TOKEN]})
    return json.loads(res.data)


# Load Parameter Store values from extension
def retrieve_extension_value_param(name: str) -> str:
    logger.info(f"## Loading AWS Systems Manager Parameter Store value: {name}")
    ssm_res = retrieve_extension_value(f"/systemsmanager/parameters/get/?name={name}")
    logger.info(f"## SSM Get Parameter (extension) response: {ssm_res}")
    return ssm_res["Parameter"]["Value"]


def lambda_handler(event, context):
    env_keys = {
        ACCOUNT_OWNER_ID,
        BUCKET_NAME_SOURCE,
        BUCKET_NAME_DEST_PREFIX,
        STATE_PARAMETER,
        ARCHIVE_BYTE_COUNT,
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

    s3 = boto3.resource("s3", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to S3 via resource")

    logger.info(
        f"## Archiving S3 objects: (S3 bucket source) '{os.environ[BUCKET_NAME_SOURCE]}' -> "
        f"(S3 bucket dest) '{os.environ[BUCKET_NAME_DEST_PREFIX]}-%Y'"
    )
    total_archive_size: int = 0
    archived_objs: list[str] = []
    for obj in s3.Bucket(os.environ[BUCKET_NAME_SOURCE]).objects.all():
        obj_key = obj.key
        obj_key_props = obj_key.split(sep="_", maxsplit=2)
        try:
            dt = datetime.strptime(obj_key_props[0], "%Y%m%d%H")
        except ValueError as ex:
            logger.info(f"## Skipping ({ex}): '{obj_key}'")
            continue
        obj_key_dest = (
            f"MetOffice/{obj_key_props[1]}"
            f"/{'_'.join([i for i in obj_key_props[2].split('_') if 'grib' not in i and 'area' not in i.lower()])}"
            f"/{str(dt.month).zfill(2)}/{obj_key}"
        )
        s3_obj = s3.Object(f"{os.environ[BUCKET_NAME_DEST_PREFIX]}-{dt.year}", obj_key_dest)
        try:
            s3_obj.load()
            logger.info(f"## Skipping (The S3 object already exists): '{obj_key}'")
        except ClientError as ex:
            if ex.response["Error"]["Code"] != "404":
                # Cannot determine whether the S3 object does not exist.
                logger.error(f"## Skipping ({ex}): '{obj_key}'")
                continue
            s3_obj.copy(
                CopySource={"Bucket": obj.bucket_name, "Key": obj_key},
                ExtraArgs={
                    "ExpectedBucketOwner": os.environ[ACCOUNT_OWNER_ID],
                    "StorageClass": obj.storage_class if obj.storage_class != "STANDARD" else "STANDARD_IA",
                },
            )
        total_archive_size += obj.size
        obj.delete()
        archived_objs.append(obj_key)

    logger.info(f"## Archived S3 objects (count: {len(archived_objs)}): {archived_objs}")

    state_param = json.loads(retrieve_extension_value_param(name=os.environ[STATE_PARAMETER]))

    state_param[os.environ[ARCHIVE_BYTE_COUNT]] = str(
        int(state_param[os.environ[ARCHIVE_BYTE_COUNT]] if os.environ[ARCHIVE_BYTE_COUNT] in state_param else 0)
        + total_archive_size
    )

    logger.info(f"## State Meta: {state_param}")

    ssm = boto3.client("ssm", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to SSM via client")

    ssm_res = ssm.put_parameter(
        Name=os.environ[STATE_PARAMETER],
        # Description,  # Default to the existing description
        Value=json.dumps(state_param),
        Type="String",
        Overwrite=True,
        # TODO: (OPTIONAL) A regular expression used to validate the parameter value.
        # AllowedPattern=,
        Tier="Standard",
        DataType="text",
    )
    logger.info(f"## SSM Put Parameter response: {ssm_res}")
