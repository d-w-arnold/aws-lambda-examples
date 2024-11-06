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

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

# boto3.setup_default_session(profile_name="innovation")  # Enable to use alt AWS profile

BUCKET_NAME = "BUCKET_NAME"
DAILY_THRESHOLD = "DAILY_THRESHOLD"
IAM_USER = "IAM_USER"
IAM_USER_GROUP = "IAM_USER_GROUP"
STATE_PARAMETER = "STATE_PARAMETER"
ARCHIVE_BYTE_COUNT = "ARCHIVE_BYTE_COUNT"

DATE = "date"
DAILY_START_BYTE_COUNT = "daily-start-byte-count"
LATEST_BYTE_COUNT = "latest-byte-count"

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


def reset_state(state_param: dict, today: str, bucket_total_size_bytes: int) -> None:
    state_param[DATE] = today
    state_param[DAILY_START_BYTE_COUNT] = str(bucket_total_size_bytes)
    state_param[LATEST_BYTE_COUNT] = str(bucket_total_size_bytes)
    state_param.pop(os.environ[ARCHIVE_BYTE_COUNT], None)


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
        BUCKET_NAME,
        DAILY_THRESHOLD,
        IAM_USER,
        IAM_USER_GROUP,
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

    iam = boto3.client("iam", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to IAM via client")

    iam_res = iam.get_group(GroupName=os.environ[IAM_USER_GROUP])
    logger.info(f"## IAM Get Group response: {iam_res}")

    logger.info(
        f"## Checking the '{os.environ[IAM_USER]}' IAM user is in the '{os.environ[IAM_USER_GROUP]}' IAM user group ..."
    )
    in_group: bool = False
    for i in iam_res["Users"]:
        if i["UserName"] == os.environ[IAM_USER]:
            in_group = True
            break

    if in_group:
        today = datetime.today().strftime("%Y-%m-%d")

        state_param = json.loads(retrieve_extension_value_param(name=os.environ[STATE_PARAMETER]))

        s3 = boto3.resource("s3", region_name=os.environ["AWS_REGION"])
        logger.info("## Connected to S3 via resource")

        logger.info(f"## Getting the total size (in bytes) for S3 bucket: '{os.environ[BUCKET_NAME]}'")
        bucket_total_size_bytes = sum(obj.size for obj in s3.Bucket(os.environ[BUCKET_NAME]).objects.all())
        logger.info(f"## The total size (in bytes): {bucket_total_size_bytes}")

        if state_param[DATE]:
            logger.info("## Found date in State Meta")
            daily_threshold_bytes = int(os.environ[DAILY_THRESHOLD]) * 1000 * 1024 * 1024
            logger.info(f"## The daily threshold (in bytes): {daily_threshold_bytes}")

            archive_byte_count = None
            if os.environ[ARCHIVE_BYTE_COUNT] in state_param:
                archive_byte_count = int(state_param[os.environ[ARCHIVE_BYTE_COUNT]])
                logger.info(f"## The total archive size (in bytes): {archive_byte_count}")
            else:
                logger.info("## Total archive size NOT found")

            if (bucket_total_size_bytes + (archive_byte_count if archive_byte_count is not None else 0)) > (
                int(state_param[DAILY_START_BYTE_COUNT]) + daily_threshold_bytes
            ):
                logger.warning(
                    f"## WARN: The daily threshold ({os.environ[DAILY_THRESHOLD]} GB) has been exceeded "
                    f"for S3 bucket: '{os.environ[BUCKET_NAME]}'"
                )
                logger.info(
                    f"## Removing '{os.environ[IAM_USER]}' IAM user from "
                    f"the '{os.environ[IAM_USER_GROUP]}' IAM user group ..."
                )
                iam_res = iam.remove_user_from_group(
                    GroupName=os.environ[IAM_USER_GROUP], UserName=os.environ[IAM_USER]
                )
                logger.info(f"## IAM Remove User From Group response: {iam_res}")

            if today > state_param[DATE]:
                logger.info("## New date found")
                reset_state(state_param, today, bucket_total_size_bytes)
            else:
                logger.info("## Same date found")
                state_param[LATEST_BYTE_COUNT] = str(bucket_total_size_bytes)
        else:
            logger.info("## Date NOT set in State Meta")
            reset_state(state_param, today, bucket_total_size_bytes)

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
    else:
        logger.warning(
            f"## WARN: The '{os.environ[IAM_USER]}' IAM user is NOT in the '{os.environ[IAM_USER_GROUP]}' "
            f"IAM user group, stopping Lambda function ..."
        )
