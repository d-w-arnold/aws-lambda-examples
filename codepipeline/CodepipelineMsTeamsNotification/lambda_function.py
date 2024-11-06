import json
import logging
import os
import sys
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

import urllib3

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

    codepipeline = boto3.client("codepipeline", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to CodePipeline via clients")

    message_sns = event["Records"][0]["Sns"]
    sns_topic_arn_props = message_sns["TopicArn"].rsplit(sep=":", maxsplit=3)[1:]
    sns_message_details = json.loads(message_sns["Message"])
    pipeline_name = sns_message_details["detail"]["pipeline"]
    pipeline_state = sns_message_details["detail"]["state"]
    execution_trigger_details = sns_message_details["detail"].get("execution-trigger")
    dt_formatted = dt.isoparse(sns_message_details["time"]).strftime("%Y-%m-%d %H:%M:%S").split(" ")

    codepipeline_res = {}
    artifact_revisions = False
    retries = 0
    max_retries = 5
    while not artifact_revisions and retries < max_retries:
        codepipeline_res = codepipeline.get_pipeline_execution(
            pipelineName=pipeline_name, pipelineExecutionId=sns_message_details["detail"]["execution-id"]
        )
        retries += 1
        artifact_revisions = bool(
            "artifactRevisions" in codepipeline_res["pipelineExecution"]
            and codepipeline_res["pipelineExecution"]["artifactRevisions"]
        )
    logger.info(f"## CodePipeline Get Pipeline Execution response: {codepipeline_res}")

    facts = [
        get_fact("AWS Account", sns_topic_arn_props[1]),
        get_fact("AWS Region", sns_topic_arn_props[0]),
        get_fact("SNS Topic", sns_topic_arn_props[2]),
        get_fact("SNS Message ID", message_sns["MessageId"]),
        get_fact("Pipeline Name", pipeline_name),
        get_fact("Pipeline State", pipeline_state),
    ]
    if execution_trigger_details:
        if execution_trigger_type := execution_trigger_details.get("trigger-type"):
            facts.append(get_fact("Pipeline Event Type", execution_trigger_type))
            if execution_trigger_type == "Webhook":
                facts.append(get_fact("Pipeline Execution ID", sns_message_details["detail"]["execution-id"]))
        if execution_trigger_details_detail := execution_trigger_details.get("trigger-detail"):
            facts.append(get_fact("Initiated By", execution_trigger_details_detail.rsplit(sep="/", maxsplit=1)[-1]))
        if artifact_revisions:
            facts.append(
                get_fact("Commit ID", codepipeline_res["pipelineExecution"]["artifactRevisions"][0]["revisionId"])
            )
            facts.append(
                get_fact(
                    "Commit Msg",
                    json.loads(codepipeline_res["pipelineExecution"]["artifactRevisions"][0]["revisionSummary"])[
                        "CommitMessage"
                    ],
                )
            )

    teams_message = {
        "@context": "https://schema.org/extensions",
        "@type": "MessageCard",
        "themeColor": "64a837" if pipeline_state != "FAILED" else "b12820",
        "title": f"{sns_message_details['detailType']} {message_sns['Type']}",
        "text": f"`{pipeline_name}` pipeline {pipeline_state} at **{dt_formatted[1]}** on **{dt_formatted[0]}**",
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
