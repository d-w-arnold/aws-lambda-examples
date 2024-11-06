import json
import logging
import os
import sys

import urllib3

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

DEPLOY_ENV_UPPER = "DEPLOY_ENV"
DEPLOY_TAG = "DEPLOY_TAG"
VERSION_META_PARAMETER = "VERSION_META_PARAMETER"

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

BUILD_NO = "build-no"
CODE_VERSION = "code-version"
COMMIT_ID = "commit-id"
COMMIT_MSG = "commit-msg"
DEPLOY_ENV = "deploy-env"

STAGING = "staging"
MAIN = "main"
MAJOR = "major"
MINOR = "minor"
PATCH = "patch"
TAG = "tag"

http = urllib3.PoolManager()


def get_commit_msg_prefix(commit_msg: str) -> str:
    logger.info("## Finding commit msg prefix")
    open_br = False
    close_br = False
    for i in commit_msg:
        if open_br and close_br:
            return commit_msg.split(sep="[", maxsplit=1)[1].split(sep="]", maxsplit=1)[0].lower()
        if i == "[" and not close_br:
            open_br = True
        if i == "]" and open_br:
            close_br = True
    return MINOR.lower()


def get_version_meta(event, version_meta: dict) -> dict[str, str]:
    return {
        k: v
        for k, v in {
            BUILD_NO: event[BUILD_NO],
            CODE_VERSION: f"{version_meta[MAJOR]}.{version_meta[MINOR]}.{version_meta[PATCH]}",
            COMMIT_ID: version_meta[COMMIT_ID],
            DEPLOY_ENV: os.environ[DEPLOY_ENV_UPPER],
            TAG: event[TAG] if TAG in event else None,
        }.items()
        if v
    }


def increment(version_meta: dict, type_: str) -> None:
    logger.info(f"## Found commit msg prefix: {type_}")
    if version_meta[PATCH] == 0 and version_meta[MINOR] == 0 and version_meta[MAJOR] == 0:
        # If this is the first time we're updating the version meta
        version_meta[MAJOR] += 1
    elif type_ == PATCH:
        version_meta[PATCH] += 1
    elif type_ == MINOR:
        version_meta[PATCH] = 0
        version_meta[MINOR] += 1
    elif type_ == MAJOR:
        version_meta[PATCH] = 0
        version_meta[MINOR] = 0
        version_meta[MAJOR] += 1
    else:
        logger.error(
            f"## ERROR: Invalid version increment type, '{type_}' (must be one of: [{MAJOR}, {MINOR}, {PATCH}])"
        )
        sys.exit(1)


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
        DEPLOY_ENV_UPPER,
        DEPLOY_TAG,
        VERSION_META_PARAMETER,
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

    keys: list = [BUILD_NO, COMMIT_ID, COMMIT_MSG]
    if all(k in event for k in keys):
        logger.info(f"## EVENT: {event}")
    else:
        logger.error(f"## Not all {keys} in EVENT: {event}")
        sys.exit(1)

    param_version_meta = json.loads(retrieve_extension_value_param(name=os.environ[VERSION_META_PARAMETER]))

    if param_version_meta[COMMIT_ID] == event[COMMIT_ID]:
        return get_version_meta(event, param_version_meta)

    ssm = boto3.client("ssm", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to SSM via client")

    if any(k == os.environ[DEPLOY_ENV_UPPER] for k in [STAGING, MAIN]):
        commit_msg_prefix = get_commit_msg_prefix(event[COMMIT_MSG])
        if commit_msg_prefix == PATCH:
            increment(param_version_meta, PATCH)
        elif commit_msg_prefix == MINOR:
            increment(param_version_meta, MINOR)
        elif commit_msg_prefix == MAJOR:
            increment(param_version_meta, MAJOR)
        else:
            # If the commit msg prefix is unrecognised, defaults to MINOR change.
            logger.warning(
                f"## WARN: Invalid commit msg prefix, '{commit_msg_prefix}' "
                f"(must be one of: [{MAJOR}, {MINOR}, {PATCH}])"
            )
            increment(param_version_meta, MINOR)

        param_version_meta[COMMIT_ID] = event[COMMIT_ID]

        ssm_res = ssm.put_parameter(
            Name=os.environ[VERSION_META_PARAMETER],
            # Description,  # Default to the existing description
            Value=json.dumps(param_version_meta),
            Type="String",
            Overwrite=True,
            # TODO: (OPTIONAL) A regular expression used to validate the parameter value.
            # AllowedPattern=,
            Tier="Standard",
            DataType="text",
        )
        logger.info(f"## SSM Put Parameter response: {ssm_res}")

    elif json.loads(os.environ[DEPLOY_TAG]):
        ssm_res = ssm.get_parameter_history(Name=os.environ[VERSION_META_PARAMETER], WithDecryption=True)
        logger.debug(f"## SSM Get Parameter History response: {ssm_res}")

        found_in_history = False
        for j in reversed([json.loads(i["Value"]) for i in ssm_res["Parameters"]]):
            if event[COMMIT_ID] == j[COMMIT_ID]:
                param_version_meta = j
                found_in_history = True
        if not found_in_history:
            logger.info(f"## Could not find commit ID '{event[COMMIT_ID]}' in SSM Get Parameter History response.")

    return get_version_meta(event, param_version_meta)
