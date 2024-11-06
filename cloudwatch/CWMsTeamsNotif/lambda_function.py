import json
import logging
import os
import sys
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3
import dateutil.parser as dt
from botocore.exceptions import ClientError

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

WEBHOOK_URL = "WEBHOOK_URL"


def check_log_group_exists(log_group_name: str) -> tuple[str, str]:
    errors_str: str = "Errors"
    if log_group_name != errors_str and (not log_group_name.endswith(errors_str)):
        logs = boto3.client("logs", region_name=os.environ["AWS_REGION"])
        logger.info("## Connected to Logs via client")

        try:
            logs_res = logs.describe_metric_filters(
                logGroupName=log_group_name,
            )
            return log_group_name, logs_res["metricFilters"][0]["filterPattern"]
        except ClientError as ex:
            logger.info(f"## Could not find log group: '{log_group_name}'")
            logger.debug(f"## Logs Describe Metric Filters ERROR: {ex}")

    return None, None


def get_fact(name: str, value: str) -> dict:
    return {"name": name + ":", "value": value}


def lambda_handler(event, context):
    env_keys = {WEBHOOK_URL}
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    if "Records" in event and bool(event["Records"]):
        logger.info(f"## EVENT: {event}")
    else:
        logger.error(f"## No Records in EVENT: {event}")
        sys.exit(1)

    message_sns = event["Records"][0]["Sns"]
    sns_topic_arn_props = message_sns["TopicArn"].rsplit(sep=":", maxsplit=3)[1:]
    sns_message_details = json.loads(message_sns["Message"])
    dt_formatted = dt.isoparse(sns_message_details["StateChangeTime"]).strftime("%Y-%m-%d %H:%M:%S").split(" ")

    log_group_name, filter_pattern = check_log_group_exists(log_group_name=sns_message_details["Trigger"]["MetricName"])

    teams_message = {
        "@context": "https://schema.org/extensions",
        "@type": "MessageCard",
        "themeColor": "b12820",
        "title": message_sns["Subject"],
        "text": f"`{sns_message_details['AlarmName']}` {sns_message_details['NewStateValue']} {message_sns['Type']} at **{dt_formatted[1]}** on **{dt_formatted[0]}**",
        "sections": [
            {
                "facts": [
                    get_fact(k, v)
                    for k, v in {
                        **{
                            "AWS Account": sns_topic_arn_props[1],
                            "AWS Region": sns_topic_arn_props[0],
                            "SNS Topic": sns_topic_arn_props[2],
                            "SNS Message ID": message_sns["MessageId"],
                        },
                        **{i: json.dumps(j) for i, j in sns_message_details.items() if j},
                        **{
                            "Metric Log Group": log_group_name,
                            "Metric Filter Pattern": filter_pattern,
                        },
                    }.items()
                    if v
                ]
            }
        ],
    }

    try:
        with urlopen(
            url=Request(
                url=os.environ[WEBHOOK_URL],
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
