import base64
import json
import logging
import os
import sys
import tempfile

import urllib3

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3
from botocore.exceptions import ClientError
from sih_lion import __version__ as sih_lion_version
from sih_lion.collectors.base import Collector
from sih_lion.source import SourceConfig

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")
logger.info(f"sih-lion version: {sih_lion_version}")

ACCOUNT_OWNER_ID = "ACCOUNT_OWNER_ID"
COLLECTOR_SECRET = "COLLECTOR_SECRET"
S3_PARAM_DATA_BUCKET_NAME = "S3_PARAM_DATA_BUCKET_NAME"
S3_PARAM_DATA_BUCKET_OBJ_PREFIX = "S3_PARAM_DATA_BUCKET_OBJ_PREFIX"
CHECKSUM_ALGORITHM = "CHECKSUM_ALGORITHM"
KMS_MASTER_KEY_ID = "KMS_MASTER_KEY_ID"
ENCRYPTION_CONTEXT_KEY = "ENCRYPTION_CONTEXT_KEY"
CDK_STACK_NAME = "CDK_STACK_NAME"
TAGS = "TAGS"
SOURCE_NAME = "SOURCE_NAME"
SOURCE_MODULE_NAME = "SOURCE_MODULE_NAME"
SOURCE_CLASS_NAME = "SOURCE_CLASS_NAME"
ATMOS_PARAMS = "ATMOS_PARAMS"

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

param_data_str = "param_data"
param_filename_ext = "nc"

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


def s3_put_object(s3, bucket_name: str, prefix_path: str, netcdf_fip: str) -> dict:
    s3_res = {}

    with open(netcdf_fip, "rb") as f:
        logger.info(f"## Creating a new S3 object: s3://{bucket_name}/{prefix_path}")
        try:
            s3_res["put_object"] = s3.put_object(
                ACL="bucket-owner-full-control",
                Body=f,
                Bucket=bucket_name,
                ChecksumAlgorithm=os.environ[CHECKSUM_ALGORITHM],
                Key=prefix_path,
                # TODO: (OPTIONAL) Add key-value pairs metadata for S3 object
                # Metadata={
                #     'string': 'string'
                # },
                ServerSideEncryption="aws:kms",
                StorageClass="STANDARD",
                SSEKMSKeyId=os.environ[KMS_MASTER_KEY_ID],
                SSEKMSEncryptionContext=base64.b64encode(
                    json.dumps({os.environ[ENCRYPTION_CONTEXT_KEY]: os.environ[CDK_STACK_NAME]}).encode("ascii")
                ).decode("ascii"),
                BucketKeyEnabled=True,
                # ObjectLockMode='GOVERNANCE' | 'COMPLIANCE',
                # ObjectLockRetainUntilDate=datetime(2015, 1, 1),
                # ObjectLockLegalHoldStatus='ON' | 'OFF',
                ExpectedBucketOwner=os.environ[ACCOUNT_OWNER_ID],
            )
            logger.info(f"## S3 Put Object response: {s3_res['put_object']}")
        except ClientError as ex:
            logger.error(f"## ERROR: {ex}")
            return status_failed(s3_res)

    try:
        s3_res["put_object_tagging"] = s3.put_object_tagging(
            Bucket=bucket_name,
            Key=prefix_path,
            ChecksumAlgorithm=os.environ[CHECKSUM_ALGORITHM],
            Tagging={"TagSet": json.loads(os.environ[TAGS])},
            ExpectedBucketOwner=os.environ[ACCOUNT_OWNER_ID],
        )
        logger.info(f"## S3 Put Object Tagging response: {s3_res['put_object_tagging']}")
    except ClientError as ex:
        logger.error(f"## ERROR: {ex}")
        return status_failed(s3_res)

    return s3_res


def status_failed(responses: dict) -> dict:
    return {status_str: "FAILED", responses_str: responses}


def lambda_handler(event, context):
    env_keys = {
        ACCOUNT_OWNER_ID,
        COLLECTOR_SECRET,
        S3_PARAM_DATA_BUCKET_NAME,
        S3_PARAM_DATA_BUCKET_OBJ_PREFIX,
        CHECKSUM_ALGORITHM,
        KMS_MASTER_KEY_ID,
        ENCRYPTION_CONTEXT_KEY,
        CDK_STACK_NAME,
        TAGS,
        SOURCE_NAME,
        SOURCE_MODULE_NAME,
        SOURCE_CLASS_NAME,
        ATMOS_PARAMS,
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

    os.environ[TMP_FOP] = tempfile.gettempdir()
    logger.info(f"Temporary folder path: {os.environ[TMP_FOP]}")

    for k, v in dict(json.loads(retrieve_extension_value_secret(secret_id=os.environ[COLLECTOR_SECRET]))).items():
        if k.startswith(os.environ[SOURCE_NAME].upper()):
            env_key = k.split(sep="_", maxsplit=1)[-1]
            logger.info(f"## Setting env var: {env_key}")
            os.environ[env_key] = v

    # Create source config.
    _config = SourceConfig()

    collector = Collector.from_config(_config, logger)
    if collector is None:
        logger.error(f"## No collector created: {_config.class_name}")
        raise RuntimeError(f"## No collector created: {_config.class_name}")

    source_data = collector.get_data(**{})

    s3_res = {param_data_str: {}}
    s3_put_object_failed = False

    # Write new param data file(s) to S3
    if (atmos_param_filenames := source_data.get("params")) is not None:
        s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
        logger.info("## Connected to S3 via client")

        for atmos_param_filename in atmos_param_filenames:
            s3_res[param_data_str][atmos_param_filename] = s3_put_object(
                s3,
                os.environ[S3_PARAM_DATA_BUCKET_NAME],
                f"{os.environ[S3_PARAM_DATA_BUCKET_OBJ_PREFIX]}/{atmos_param_filename}",
                os.path.join(os.environ[TMP_FOP], atmos_param_filename),
            )
            if status_str in s3_res[param_data_str][atmos_param_filename]:
                s3_put_object_failed = True
    else:
        logger.info("## No param data files found.")
        s3_put_object_failed = True

    return {status_str: "FAILED" if s3_put_object_failed else "SUCCEEDED", responses_str: s3_res}
