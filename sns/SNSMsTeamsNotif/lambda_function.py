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

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

WEBHOOK_URL = "WEBHOOK_URL"


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
    is_asg = bool("AutoScalingGroupName" in sns_message_details)
    dt_formatted = (
        dt.isoparse(sns_message_details["Time"] if is_asg else message_sns["Timestamp"])
        .strftime("%Y-%m-%d %H:%M:%S")
        .split(" ")
    )
    sns_message_details_props = None if is_asg else list(sns_message_details.items())[0]
    sns_message_details_dest = f"{sns_message_details.get('Destination', 'AutoScalingGroup')} " if is_asg else ""

    teams_message = {
        "@context": "https://schema.org/extensions",
        "@type": "MessageCard",
        "themeColor": "64a837",
        "title": subject if (subject := message_sns.get("Subject")) else sns_message_details_props[0],
        "text": f"`{sns_message_details['AutoScalingGroupName'] if is_asg else sns_message_details_props[1]}` "
        f"{sns_message_details.get('Origin', sns_message_details.get('Service','EC2')) if is_asg else sns_message_details_props[0].split(sep=':', maxsplit=1)[0]} "
        f"{sns_message_details_dest}{message_sns['Type']} at **{dt_formatted[1]}** on **{dt_formatted[0]}**",
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
