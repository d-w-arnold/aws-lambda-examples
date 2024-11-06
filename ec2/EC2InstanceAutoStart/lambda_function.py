import json
import logging
import os
import sys
from datetime import datetime
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

BODY_KEY = "Body"
ENV_KEY = "Environment"
IDS_KEY = "InstanceIds"
MSG_KEY = "Message"
STATUS_CODE_KEY = "StatusCode"


def get_fact(name: str, value: str) -> dict:
    return {"name": name + ":", "value": value}


def get_message(status, body, ids=None):
    env = {"TAG_KEY": os.environ["TAG_KEY"], "TAG_VALUES": os.environ["TAG_VALUES"]}
    if "SEP" in os.environ:
        env["SEP"] = os.environ["SEP"]
    return {
        STATUS_CODE_KEY: status,
        "Headers": {"Content-Type": "application/json"},
        BODY_KEY: {
            **{ENV_KEY: env},
            **({IDS_KEY: ids, "StartInstancesResponse": body} if status == 200 else {MSG_KEY: body}),
        },
    }


def lambda_handler(event, context):
    env_keys = {"SNS_TOPIC", "TAG_KEY", "TAG_VALUES", "WEBHOOK_URL"}
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    ec2 = boto3.client("ec2", region_name=os.environ["AWS_REGION"])
    sns = boto3.client("sns", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to EC2 and SNS via clients")

    instance_filters = [
        {"Name": "instance-state-name", "Values": ["stopped"]},
        {
            "Name": f"tag:{os.environ['TAG_KEY'].strip()}",
            "Values": (
                [v.strip() for v in os.environ["TAG_VALUES"].split(os.environ["SEP"])]
                if "SEP" in os.environ
                else [os.environ["TAG_VALUES"]]
            ),
        },
    ]
    ec2_res = ec2.describe_instances(Filters=instance_filters)
    logger.info(f"## EC2 Describe Instances response: {ec2_res}")

    ids = {
        i[0]["InstanceId"]: [d["Value"] for d in i[0]["Tags"] if d["Key"] == "Name"][0]
        for i in [j["Instances"] for j in ec2_res["Reservations"]]
    }
    if ids:
        ec2_res = ec2.start_instances(InstanceIds=list(ids.keys()))
        logger.info(f"## EC2 Start Instances response: {ec2_res}")
    body = ec2_res if ids else "No EC2 instance IDs found."
    status = 200 if not isinstance(body, str) else 404
    subject = (
        f"Lambda: {os.environ['AWS_LAMBDA_FUNCTION_NAME']} started {f'{len(ids)}x' if ids else 'no'} EC2 instances"
    )
    message = get_message(status, body, ids=ids if status == 200 else None)
    sns_res = sns.publish(
        TopicArn=os.environ["SNS_TOPIC"],
        Subject=subject,
        Message=json.dumps(
            {
                "default": json.dumps(
                    message,
                    sort_keys=True,
                    default=str,
                )
            }
        ),
        MessageStructure="json",
    )
    logger.info(f"## SNS Publish response: {sns_res}")

    sns_topic_arn_props = os.environ["SNS_TOPIC"].rsplit(sep=":", maxsplit=3)[1:]
    dt_formatted = datetime.today().strftime("%Y-%m-%d %H:%M:%S").split(" ")

    try:
        with urlopen(
            url=Request(
                url=os.environ["WEBHOOK_URL"],
                data=json.dumps(
                    {
                        "@context": "https://schema.org/extensions",
                        "@type": "MessageCard",
                        "themeColor": "64a837" if status == 200 else "b12820",
                        "title": subject,
                        "text": f"`{os.environ['AWS_LAMBDA_FUNCTION_NAME']}` at **{dt_formatted[1]}** on **{dt_formatted[0]}**",
                        "sections": [
                            {
                                "facts": [
                                    get_fact(k, v)
                                    for k, v in {
                                        "AWS Account": sns_topic_arn_props[1],
                                        "AWS Region": sns_topic_arn_props[0],
                                        "SNS Topic": sns_topic_arn_props[2],
                                        "SNS Message ID": sns_res["MessageId"],
                                        STATUS_CODE_KEY: message[STATUS_CODE_KEY],
                                        ENV_KEY: json.dumps(message[BODY_KEY][ENV_KEY]),
                                        MSG_KEY: message[BODY_KEY][MSG_KEY] if MSG_KEY in message[BODY_KEY] else None,
                                    }.items()
                                    if v
                                ]
                                + (
                                    [get_fact(IDS_KEY, "")]
                                    + [get_fact(k, f"`{v}`") for k, v in message[BODY_KEY][IDS_KEY].items()]
                                    if IDS_KEY in message[BODY_KEY]
                                    else []
                                )
                            }
                        ],
                    }
                ).encode("utf-8"),
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
