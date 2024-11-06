import json
import logging
import os
import sys
from urllib.error import URLError, HTTPError
from urllib.request import Request, urlopen

from datetime import datetime

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

SNS_TOPIC = "SNS_TOPIC"
TAG_KEY = "TAG_KEY"
TAG_VALUES = "TAG_VALUES"
EC_REP_GROUP_FINAL_SNAPSHOT_ID = "EC_REP_GROUP_FINAL_SNAPSHOT_ID"
EC_REP_GROUP_ARN = "EC_REP_GROUP_ARN"
EC_REP_GROUP_ID = "EC_REP_GROUP_ID"
WEBHOOK_URL = "WEBHOOK_URL"

BODY_KEY = "Body"
ENV_KEY = "Environment"
MSG_KEY = "Message"
RESP_KEY = "DeleteReplicationGroupResponse"
STATUS_CODE_KEY = "StatusCode"


def check_final_snapshot_id_exists(elasticache, ec_rep_group_final_snapshot_id) -> bool:
    try:
        elasticache_res = elasticache.describe_snapshots(
            SnapshotName=ec_rep_group_final_snapshot_id, ShowNodeGroupConfig=True
        )
        logger.info(
            f"## ElastiCache Describe Snapshots response "
            f"(provided Snapshot Name: '{ec_rep_group_final_snapshot_id}'): {elasticache_res}"
        )
    except ClientError as ex:
        logger.error(f"## ElastiCache Describe Snapshots ERROR: '{ex}'")
        sys.exit(1)
    if elasticache_res["Snapshots"]:
        logger.error(
            f"## Found a Snapshot (of the same Final Snapshot ID) already exists: "
            f"'{elasticache_res['Snapshots'][0]['SnapshotName']}'"
        )
        sys.exit(1)
    return False


def contains_auto_tag(tag_list: list[dict], weekend: bool):
    tag_key = os.environ[TAG_KEY].strip()
    tag_val = None
    main_tag: bool = False
    weekend_tag: bool = bool(not weekend)
    main_tag_found: bool = False
    for tag in tag_list:
        if not main_tag and tag["Key"] == tag_key:
            main_tag_found = True
            if tag["Value"] in os.environ[TAG_VALUES]:
                main_tag = True
            else:
                tag_val = tag["Value"]
        if not weekend_tag and tag["Key"] == "auto_weekend":
            weekend_tag = True
        if main_tag and weekend_tag:
            return True
    msg = f"Found the tag: ('{tag_key}': '{tag_val}')" if main_tag_found else f"Could NOT find the tag: '{tag_key}'"
    logger.info(f"## {msg}, stopping Lambda function ...")
    return False


def get_fact(name: str, value: str) -> dict:
    return {"name": name + ":", "value": value}


def get_message(status: int, body):
    env = {
        TAG_KEY: os.environ[TAG_KEY],
        TAG_VALUES: os.environ[TAG_VALUES],
        EC_REP_GROUP_FINAL_SNAPSHOT_ID: os.environ[EC_REP_GROUP_FINAL_SNAPSHOT_ID],
    }
    return {
        STATUS_CODE_KEY: status,
        "Headers": {"Content-Type": "application/json"},
        BODY_KEY: {ENV_KEY: env, (RESP_KEY if status == 200 else MSG_KEY): body},
    }


def sns_publish(body, success: bool) -> None:
    sns = boto3.client("sns", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to SNS via clients")

    success_msg = "snapshot success & stopped Redis cache"
    failed_msg = "snapshot failed & NOT stopped Redis cache"

    status = 200 if success else 404
    subject = f"Lambda: {os.environ['AWS_LAMBDA_FUNCTION_NAME']} {success_msg if success else failed_msg}"
    message = get_message(status, body)
    sns_res = sns.publish(
        TopicArn=os.environ["SNS_TOPIC"],
        # Subject, Max 100 chars
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
                                        RESP_KEY.replace("Response", "", 1): (
                                            message[BODY_KEY][RESP_KEY]["ReplicationGroup"]["ReplicationGroupId"]
                                            if RESP_KEY in message[BODY_KEY]
                                            else None
                                        ),
                                    }.items()
                                    if v
                                ]
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


def lambda_handler(event, context):
    env_keys = {
        SNS_TOPIC,
        TAG_KEY,
        TAG_VALUES,
        EC_REP_GROUP_FINAL_SNAPSHOT_ID,
        EC_REP_GROUP_ARN,
        EC_REP_GROUP_ID,
        WEBHOOK_URL,
    }
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    elasticache = boto3.client("elasticache", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to ElastiCache via clients")

    try:
        elasticache_res = elasticache.list_tags_for_resource(ResourceName=os.environ[EC_REP_GROUP_ARN])
        logger.info(f"## ElastiCache List Tags For Resource response: {elasticache_res}")
    except ClientError as ex:
        logger.error(f"## ElastiCache List Tags For Resource ERROR: '{ex}'")
        sys.exit(1)

    ec_rep_group_final_snapshot_id = f"{os.environ[EC_REP_GROUP_FINAL_SNAPSHOT_ID]}-{datetime.now().strftime('%y%m%d')}"

    if not check_final_snapshot_id_exists(elasticache, ec_rep_group_final_snapshot_id) and contains_auto_tag(
        elasticache_res["TagList"], bool("weekend" in event)
    ):
        try:
            elasticache_res = elasticache.delete_replication_group(
                ReplicationGroupId=os.environ[EC_REP_GROUP_ID],
                RetainPrimaryCluster=False,
                FinalSnapshotIdentifier=ec_rep_group_final_snapshot_id,
            )
            logger.info(f"## ElastiCache Delete Replication Group response: {elasticache_res}")
            sns_publish(elasticache_res, success=True)
        except ClientError as ex:
            logger.error(f"## ElastiCache Delete Replication Group ERROR: '{ex}'")
            sns_publish(ex, success=False)
