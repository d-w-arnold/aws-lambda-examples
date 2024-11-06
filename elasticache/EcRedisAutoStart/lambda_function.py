import json
import logging
import os
import sys
from datetime import datetime
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

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

EC_REP_GROUP_FINAL_SNAPSHOT_ID = "EC_REP_GROUP_FINAL_SNAPSHOT_ID"
EC_REP_GROUP_KWARGS = "EC_REP_GROUP_KWARGS"
REDIS_PW_KEY = "REDIS_PW_KEY"
REDIS_PW_SECRET = "REDIS_PW_SECRET"
SNS_TOPIC = "SNS_TOPIC"
TAG_KEY = "TAG_KEY"
TAG_VALUES = "TAG_VALUES"
WEBHOOK_URL = "WEBHOOK_URL"

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

BODY_KEY = "Body"
ENV_KEY = "Environment"
MSG_KEY = "Message"
RESP_KEY = "Responses"
STATUS_CODE_KEY = "StatusCode"

EC_CREATE_REPLICATION_GROUP_KEY = "elasticache_create_replication_group"
EC_DELETE_SNAPSHOTS_KEY = "elasticache_delete_snapshots"

http = urllib3.PoolManager()


def get_latest_final_snapshot_id(elasticache) -> str:
    elasticache_res = elasticache_describe_final_snapshot_ids(elasticache)
    latest_timestamp = None
    latest_final_snapshot_id = None
    latest_final_snapshot_id_arn = None
    final_snapshot_ids = []
    for snapshot in elasticache_res["Snapshots"]:
        snapshot_name = snapshot["SnapshotName"]
        snapshot_name_props = snapshot_name.rsplit(sep="-", maxsplit=1)
        if os.environ[EC_REP_GROUP_FINAL_SNAPSHOT_ID] == snapshot_name_props[0]:
            timestamp = snapshot_name_props[-1]
            latest_timestamp = (
                timestamp if latest_timestamp is None or timestamp > latest_timestamp else latest_timestamp
            )
            if timestamp == latest_timestamp:
                latest_final_snapshot_id = snapshot_name
                latest_final_snapshot_id_arn = snapshot["ARN"]
            final_snapshot_ids.append(snapshot_name)
    if latest_final_snapshot_id is None:
        logger.error(
            f"## Could NOT find a Snapshot matching the Final Snapshot ID: '{os.environ[EC_REP_GROUP_FINAL_SNAPSHOT_ID]}'"
        )
        sys.exit(1)
    logger.info(f"## Latest Final Snapshot ID: '{latest_final_snapshot_id}'")
    return (
        latest_final_snapshot_id,
        latest_final_snapshot_id_arn,
        [i for i in final_snapshot_ids if not latest_final_snapshot_id_arn.endswith(i)],
    )


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


def get_kwarg_key_formatted(k):
    return "".join([i.capitalize() if i != "az" else i.upper() for i in k.split("_")])


def get_kwarg_val(v):
    return bool(v == "true") if v in {"true", "false"} else v


def get_fact(name: str, value: str) -> dict:
    return {"name": name + ":", "value": value}


def get_message(status: int, body, clear_up_failed: bool):
    env = {
        TAG_KEY: os.environ[TAG_KEY],
        TAG_VALUES: os.environ[TAG_VALUES],
        EC_REP_GROUP_FINAL_SNAPSHOT_ID: os.environ[EC_REP_GROUP_FINAL_SNAPSHOT_ID],
    }
    return {
        STATUS_CODE_KEY: status,
        "Headers": {"Content-Type": "application/json"},
        BODY_KEY: {ENV_KEY: env, (RESP_KEY if status == 200 or clear_up_failed else MSG_KEY): body},
    }


def elasticache_describe_final_snapshot_ids(elasticache, snapshot_name: str = None) -> dict:
    elasticache_describe_snapshots_kwargs = {
        k: v
        for k, v in {
            "SnapshotName": snapshot_name if snapshot_name else None,
            "ShowNodeGroupConfig": True,
        }.items()
        if v
    }
    try:
        elasticache_res = elasticache.describe_snapshots(**elasticache_describe_snapshots_kwargs)
        extra_detail = (
            f" (provided Snapshot Name: '{os.environ[EC_REP_GROUP_FINAL_SNAPSHOT_ID]}')" if snapshot_name else ""
        )
        logger.info(f"## ElastiCache Describe Snapshots response{extra_detail}: {elasticache_res}")
    except ClientError as ex:
        logger.error(f"## ElastiCache Describe Snapshots ERROR: '{ex}'")
        sys.exit(1)
    return elasticache_res


def log_delivery_configurations_format_helper(val):
    if isinstance(val, str):
        return val
    for k, v in val.items():
        return {get_kwarg_key_formatted(k): log_delivery_configurations_format_helper(v)}


def log_delivery_configurations_format(log_delivery_configurations):
    return [
        {
            get_kwarg_key_formatted(key): log_delivery_configurations_format_helper(val)
            for key, val in log_delivery_config.items()
        }
        for log_delivery_config in json.loads(log_delivery_configurations)
    ]


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


def sns_publish(body, success: bool, clear_up_failed: bool = False) -> None:
    sns = boto3.client("sns", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to SNS via clients")

    success_msg = "started Redis cache & deleted snapshots"
    clear_up_failed_msg = "started Redis cache & NOT deleted snapshots"
    failed_msg = "NOT started Redis cache & NOT deleted snapshots"

    status = 200 if success else 404
    subject = f"Lambda: {os.environ['AWS_LAMBDA_FUNCTION_NAME']} {success_msg if success else (clear_up_failed_msg if clear_up_failed else failed_msg)}"
    message = get_message(status, body, clear_up_failed)
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
                                        "".join(
                                            [i.capitalize() for i in EC_CREATE_REPLICATION_GROUP_KEY.split("_")[1:]]
                                        ): (
                                            message[BODY_KEY][RESP_KEY][EC_CREATE_REPLICATION_GROUP_KEY][
                                                "ReplicationGroup"
                                            ]["ReplicationGroupId"]
                                            if EC_CREATE_REPLICATION_GROUP_KEY in message[BODY_KEY][RESP_KEY]
                                            else None
                                        ),
                                        "".join([i.capitalize() for i in EC_DELETE_SNAPSHOTS_KEY.split("_")[1:]]): (
                                            (
                                                message[BODY_KEY][RESP_KEY][EC_DELETE_SNAPSHOTS_KEY]
                                                if isinstance(message[BODY_KEY][RESP_KEY][EC_DELETE_SNAPSHOTS_KEY], str)
                                                else ", ".join(
                                                    [
                                                        k
                                                        for k, v in message[BODY_KEY][RESP_KEY][
                                                            EC_DELETE_SNAPSHOTS_KEY
                                                        ].items()
                                                    ]
                                                )
                                            )
                                            if EC_DELETE_SNAPSHOTS_KEY in message[BODY_KEY][RESP_KEY]
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
        EC_REP_GROUP_FINAL_SNAPSHOT_ID,
        EC_REP_GROUP_KWARGS,
        REDIS_PW_KEY,
        REDIS_PW_SECRET,
        SNS_TOPIC,
        TAG_KEY,
        TAG_VALUES,
        WEBHOOK_URL,
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

    elasticache = boto3.client("elasticache", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to ElastiCache via clients")

    latest_final_snapshot_id, latest_final_snapshot_id_arn, old_final_snapshot_ids = get_latest_final_snapshot_id(
        elasticache
    )

    ec_rep_group_kwargs = json.loads(os.environ[EC_REP_GROUP_KWARGS])

    secretsmanager_secret_string = retrieve_extension_value_secret(secret_id=os.environ[REDIS_PW_SECRET])
    ec_rep_group_kwargs[os.environ[REDIS_PW_KEY]] = json.loads(secretsmanager_secret_string)["password"]
    logger.info("## Redis password set")

    try:
        elasticache_res = elasticache.list_tags_for_resource(ResourceName=latest_final_snapshot_id_arn)
        logger.info(f"## ElastiCache List Tags For Resource response: {elasticache_res}")
    except ClientError as ex:
        logger.error(f"## ElastiCache List Tags For Resource ERROR: '{ex}'")
        sys.exit(1)

    custom_arg_types = {
        "log_delivery_configurations": log_delivery_configurations_format,
        "num_node_groups": int,
        "port": int,
        "preferred_cache_cluster_a_zs": json.loads,
        "replicas_per_node_group": int,
        "security_group_ids": json.loads,
        "user_group_ids": json.loads,
    }

    if contains_auto_tag(elasticache_res["TagList"], bool("weekend" in event)):
        elasticache_res = {}
        try:
            elasticache_res[EC_CREATE_REPLICATION_GROUP_KEY] = elasticache.create_replication_group(
                SnapshotName=latest_final_snapshot_id,
                **{
                    get_kwarg_key_formatted(k): (
                        custom_arg_types[k](get_kwarg_val(v)) if k in custom_arg_types else get_kwarg_val(v)
                    )
                    for k, v in ec_rep_group_kwargs.items()
                },
            )
            logger.info(
                f"## ElastiCache Create Replication Group response: "
                f"{elasticache_res[EC_CREATE_REPLICATION_GROUP_KEY]}"
            )
            elasticache_delete_snapshots = {}
            delete_snapshots_success = True
            if old_final_snapshot_ids:
                for old_final_snapshot_id in old_final_snapshot_ids:
                    try:
                        elasticache_delete_snapshots[old_final_snapshot_id] = elasticache.delete_snapshot(
                            SnapshotName=old_final_snapshot_id
                        )
                    except ClientError as ex:
                        elasticache_delete_snapshots[old_final_snapshot_id] = ex
                        delete_snapshots_success = False
                elasticache_res[EC_DELETE_SNAPSHOTS_KEY] = elasticache_delete_snapshots
            else:
                elasticache_res[EC_DELETE_SNAPSHOTS_KEY] = "No old Snapshots found ..."
            logger.info(f"## ElastiCache Delete Snapshot response(s): {elasticache_res[EC_DELETE_SNAPSHOTS_KEY]}")
            sns_publish(
                elasticache_res, success=delete_snapshots_success, clear_up_failed=bool(not delete_snapshots_success)
            )
        except ClientError as ex:
            logger.error(f"## ElastiCache Create Replication Group ERROR: '{ex}'")
