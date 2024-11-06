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
from sih_lion import __version__ as sih_lion_version
from sih_lion.file_utils import FileMetaReader, FileMetaReaderSettings

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")
logger.info(f"sih-lion version: {sih_lion_version}")

ACCOUNT_OWNER_ID = "ACCOUNT_OWNER_ID"
S3_SAT_DATA_BUCKET_NAME = "S3_SAT_DATA_BUCKET_NAME"
EVENT_META_KEY = "EVENT_META_KEY"
SAT_DATA_ORG = "SAT_DATA_ORG"
SOURCE_NAME = "SOURCE_NAME"
SAT_DATA_SERVICES = "SAT_DATA_SERVICES"
LATEST_START_TIME_PARAMETER = "LATEST_START_TIME_PARAMETER"
FILENAMES_INFO = "FILENAMES_INFO"

status_str = "status"
event_str = "event"

age_cut_off: int = 60  # 60 minutes

http = urllib3.PoolManager()


def get_latest_start_times_old(ssm) -> dict:
    logger.info("## Getting latest start time (old)")
    return json.loads(ssm.get_parameter(Name=os.environ[LATEST_START_TIME_PARAMETER])["Parameter"]["Value"])


def get_valid_timestamps(s3, data_service: str, latest_start_time_old: str, reader: FileMetaReader) -> list[int]:
    bucket_name: str = os.environ[S3_SAT_DATA_BUCKET_NAME]
    prefix_path: str = f"{os.environ[SAT_DATA_ORG]}/{os.environ[SOURCE_NAME]}/{data_service}"
    logger.info(f"## Getting new latest start time: '{bucket_name}/{prefix_path}'")
    is_truncated = True
    next_continuation_token = None
    valid_timestamps = set()
    while is_truncated:
        s3_res = s3_list_objects(s3, bucket_name, prefix_path, continuation_token=next_continuation_token)
        is_truncated = s3_res["IsTruncated"]
        if "Contents" in s3_res:
            valid_timestamps = valid_timestamps.union(
                {
                    timestamp
                    for i in s3_res["Contents"]
                    if (timestamp := valid_timestamp(i, latest_start_time_old, data_service, reader))
                }
            )
        if is_truncated and "NextContinuationToken" in s3_res:
            next_continuation_token = s3_res["NextContinuationToken"]
    return list(valid_timestamps) if valid_timestamps else None


def s3_list_objects(s3, bucket_name: str, prefix_path: str, continuation_token: str = None) -> dict:
    logger.info(f"## Listing S3 objects in: s3://{bucket_name}/{prefix_path}")
    list_objects_v2_kwargs = {
        k: v
        for k, v in {
            "Bucket": bucket_name,
            "MaxKeys": 1000,
            "Prefix": prefix_path,
            "ExpectedBucketOwner": os.environ[ACCOUNT_OWNER_ID],
            "ContinuationToken": continuation_token if continuation_token else None,
        }.items()
        if v
    }
    s3_res = s3.list_objects_v2(**list_objects_v2_kwargs)
    logger.debug(f"## S3 List Objects V2 response: {s3_res}")
    return s3_res


def set_latest_start_times(ssm, latest_start_times_new: dict[str, str]) -> None:
    logger.info(f"## Setting the new latest start time SSM parameter to: '{latest_start_times_new}'")
    ssm_res = ssm.put_parameter(
        Name=os.environ[LATEST_START_TIME_PARAMETER],
        # Description,  # Default to the existing description
        Value=json.dumps(latest_start_times_new),
        Type="String",
        Overwrite=True,
        # TODO: (OPTIONAL) A regular expression used to validate the parameter value.
        # AllowedPattern=,
        Tier="Standard",
        DataType="text",
    )
    logger.info(f"## SSM Put Parameter response: {ssm_res}")


def status_failed(responses: dict, msg: str) -> dict:
    logger.info(msg)
    return {status_str: "FAILED", "msg": msg, event_str: responses}


def valid_timestamp(i, latest_start_time_old: str, data_service: str, reader: FileMetaReader) -> int:
    obj_key = i["Key"]
    logger.debug(
        f"## Checking for valid timestamp: '{obj_key}' (data service: '{data_service}', "
        f"old latest start time: '{latest_start_time_old}')"
    )
    meta = reader.find_filename_meta(os.environ[SOURCE_NAME], obj_key, file_types=[data_service])
    logger.debug(f"## File Meta: '{meta}'")
    if meta is not None:
        timestamp = meta["start_time"].strftime("%Y%m%d%H%M")
        logger.debug(f"## Timestamp: '{timestamp}'")
        if int(timestamp) > int(latest_start_time_old):
            return timestamp
    return None


def lambda_handler(event, context):
    env_keys = {
        ACCOUNT_OWNER_ID,
        S3_SAT_DATA_BUCKET_NAME,
        EVENT_META_KEY,
        SAT_DATA_ORG,
        SOURCE_NAME,
        SAT_DATA_SERVICES,
        LATEST_START_TIME_PARAMETER,
        FILENAMES_INFO,
    }
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    logger.info(f"## EVENT: {event}")

    s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
    ssm = boto3.client("ssm", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to S3 and SSM via client")

    latest_start_times: dict[str, str] = get_latest_start_times_old(ssm)

    sat_data_services = [k for k, _ in dict(json.loads(os.environ[SAT_DATA_SERVICES])).items()]

    valid_timestamps_list: list[tuple[str, list[int]]] = []
    settings = FileMetaReaderSettings()
    logger.info(f"## FileMetaReaderSettings: '{settings.filenames_info}'")
    file_meta_reader = FileMetaReader(settings)
    for data_service in sat_data_services:
        valid_timestamps = get_valid_timestamps(
            s3,
            data_service,
            latest_start_times[data_service] if data_service in latest_start_times else "0",
            file_meta_reader,
        )
        if valid_timestamps is None:
            return status_failed(
                event,
                f"## Could NOT find any files newer than: '{latest_start_times[data_service]}' (for data service: '{data_service}')",
            )
        valid_timestamps_list.append((data_service, valid_timestamps))

    if len(valid_timestamps_list) > 1:
        if not (common_valid_timestamps := set(valid_timestamps_list[0][1]).intersection(valid_timestamps_list[1][1])):
            return status_failed(
                event, f"## Could NOT find any common valid timestamps newer than: '{latest_start_times}'"
            )
        latest_common_valid_timestamp: str = str(sorted(list(common_valid_timestamps))[-1])
    else:
        latest_common_valid_timestamp: str = str(sorted(valid_timestamps_list[0][1])[-1])

    logger.info(f"## Latest common valid timestamp: '{latest_common_valid_timestamp}'")

    if latest_common_valid_timestamp in {v for _, v in latest_start_times.items()}:
        return status_failed(
            event,
            f"## Latest common valid timestamp: '{latest_common_valid_timestamp}', "
            f"is NOT newer than the old latest start times: '{latest_start_times}'",
        )

    logger.info(
        f"## Latest common valid timestamp: '{latest_common_valid_timestamp}', "
        f"is newer than the old latest start times: '{latest_start_times}'"
    )

    if (datetime.now() - datetime.strptime(latest_common_valid_timestamp, "%Y%m%d%H%M")).seconds > (age_cut_off * 60):
        return status_failed(
            event,
            f"## The new latest common valid timestamp would've been older than {age_cut_off} minutes: '{latest_common_valid_timestamp}'",
        )

    set_latest_start_times(ssm, {data_service: latest_common_valid_timestamp for data_service in sat_data_services})

    return {
        status_str: "SUCCEEDED",
        event_str: event,
        os.environ[EVENT_META_KEY]: {
            "data_service_s3_uri_props": {
                data_service: (
                    f"s3://{os.environ[S3_SAT_DATA_BUCKET_NAME]}/{os.environ[SAT_DATA_ORG]}/"
                    f"{os.environ[SOURCE_NAME]}/{data_service}/",
                    latest_common_valid_timestamp,
                )
                for data_service in sat_data_services
            },
            "csa_timestamp": latest_common_valid_timestamp,
        },
    }
