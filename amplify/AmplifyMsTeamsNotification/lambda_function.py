import json
import logging
import os
import sys
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

import urllib3
from botocore.exceptions import ClientError

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3
import dateutil.parser as dt

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

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


def amplify_list_apps(amplify) -> list[dict]:
    logger.info("## List all AWS Amplify apps")
    is_next_token: bool = True
    next_token: str = None
    amplify_list_apps_res_apps: list = []
    while is_next_token:
        amplify_list_apps_res = amplify.list_apps(
            **{
                k: v
                for k, v in {
                    "nextToken": next_token if next_token else None,
                    "maxResults": 100,  # Max 100. Default: 10
                }.items()
                if v
            }
        )
        if "nextToken" in amplify_list_apps_res:
            next_token = amplify_list_apps_res["nextToken"]
        else:
            is_next_token = False
        amplify_list_apps_res_apps += amplify_list_apps_res["apps"]
    return amplify_list_apps_res_apps


def get_fact(name: str, value: str) -> dict:
    return {"name": name + ":", "value": value}


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
        "MAPPING_PARAMETER",
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

    if "Records" in event and bool(event["Records"]):
        logger.info(f"## EVENT: {event}")
    else:
        logger.error(f"## No Records in EVENT: {event}")
        sys.exit(1)

    amplify = boto3.client("amplify", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to Amplify via clients")

    message_sns = event["Records"][0]["Sns"]
    sns_topic_arn_props = message_sns["TopicArn"].rsplit(sep=":", maxsplit=3)[1:]
    sns_topic_name = sns_topic_arn_props[2]
    amplify_app_id = sns_topic_name.split(sep="-", maxsplit=1)[-1].split(sep="_", maxsplit=1)[0]

    amplify_app_id_name = {i["appId"]: i["name"] for i in amplify_list_apps(amplify)}
    logger.info(f"## Amplify List Apps response (ID, Name): {amplify_app_id_name}")

    amplify_app_name = None
    for app_id, name in amplify_app_id_name.items():
        if app_id == amplify_app_id:
            amplify_app_name = name
            break
    if amplify_app_name is None:
        logger.error(f"## ERROR: Could not find AWS Amplify app ID (for AWS Amplify app name: '{amplify_app_name}')")
        sys.exit(1)
    logger.info(f"## AWS Amplify app name: '{amplify_app_name}' (for AWS Amplify app ID: '{amplify_app_id}')")

    sns_message_details = message_sns["Message"]
    git_branch = sns_message_details.rsplit(sep="/", maxsplit=2)[1:][0]
    pipeline_name = f"{amplify_app_name} : {git_branch}"
    pipeline_state = sns_message_details.split(sep=". ", maxsplit=2)[:2][-1].rsplit(sep=" ", maxsplit=1)[-1]
    dt_formatted = dt.isoparse(message_sns["Timestamp"]).strftime("%Y-%m-%d %H:%M:%S").split(" ")

    facts = [
        get_fact("AWS Account", sns_topic_arn_props[1]),
        get_fact("AWS Region", sns_topic_arn_props[0]),
        get_fact("SNS Topic", sns_topic_name),
        get_fact("SNS Message ID", message_sns["MessageId"]),
        get_fact("Build Pipeline Name", pipeline_name),
        get_fact("Build Pipeline State", pipeline_state),
    ]
    if pipeline_state == "STARTED":
        amplify_latest_job_meta = None
        try:
            amplify_latest_job_meta = amplify.list_jobs(appId=amplify_app_id, branchName=git_branch)["jobSummaries"][0]
            logger.info(f"## Amplify List Jobs response (latest job): {amplify_latest_job_meta}")
        except ClientError as ex:
            logger.error(
                f"## Amplify List Jobs ERROR: '{ex}', in trying to get the latest job for "
                f"AWS Amplify app '{amplify_app_name}'"
            )
        facts.append(
            get_fact(
                "Commit ID",
                amplify_latest_job_meta["commitId"] if amplify_latest_job_meta else "Cannot find commit ID.",
            )
        )
        facts.append(
            get_fact(
                "Commit Msg",
                (
                    (amplify_latest_job_meta["commitMessage"] if amplify_latest_job_meta else "Cannot find commit ID.")
                    if not git_branch.startswith("pr-")
                    else "[Preview changes before merging a pull request.] - DevOps"
                ),
            )
        )

    teams_message = {
        "@context": "https://schema.org/extensions",
        "@type": "MessageCard",
        "themeColor": "64a837" if pipeline_state != "FAILED" else "b12820",
        "title": f"Amplify App Build Pipeline {message_sns['Type']}",
        "text": f"`{pipeline_name}` build pipeline {pipeline_state} at **{dt_formatted[1]}** on **{dt_formatted[0]}**",
        "sections": [{"facts": facts}],
    }

    ssm_parameter_value = retrieve_extension_value_param(name=os.environ["MAPPING_PARAMETER"])

    webhook_url = json.loads(ssm_parameter_value)[pipeline_name.split(sep="-", maxsplit=1)[0]]
    logger.info(f"## Pipeline: {pipeline_name}, Webhook URL: {webhook_url}")

    try:
        with urlopen(
            url=Request(
                url=webhook_url,
                data=json.dumps(teams_message).encode("utf-8"),
                headers={"Content-Type": "application/json"},
            )
        ) as res:
            res.read()
    except HTTPError as err:
        logger.error(f"## Request failed: {err.code} {err.reason}")
        sys.exit(1)
    except URLError as err:
        logger.error(f"## Server connection failed: {err.reason}")
        sys.exit(1)
    logger.info(f"## Message posted to MS Teams: {res}")
