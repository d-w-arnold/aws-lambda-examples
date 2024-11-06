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
import s3fs
from satpy.readers import find_files_and_readers
from sih_lion import __version__ as sih_lion_version
from sih_lion.file_utils import FileMetaReader, FileMetaReaderSettings

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")
logger.info(f"sih-lion version: {sih_lion_version}")

ACCOUNT_OWNER_ID = "ACCOUNT_OWNER_ID"
EVENT_META_KEY = "EVENT_META_KEY"
SOURCE_NAME = "SOURCE_NAME"
SAT_DATA_SERVICES = "SAT_DATA_SERVICES"
LATEST_AVAILABLE_PARAMETER = "LATEST_AVAILABLE_PARAMETER"
FILENAMES_INFO = "FILENAMES_INFO"

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

status_str = "status"
event_str = "event"

sat_data_source_num_files = "num-files"
sat_data_source_reader = "reader"

http = urllib3.PoolManager()


def get_latest_available_start_times(data_service: str) -> dict:
    logger.info(f"## Getting latest available start time (for data service: '{data_service}')")
    return json.loads(retrieve_extension_value_param(name=os.environ[LATEST_AVAILABLE_PARAMETER]))


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


def set_latest_available_start_times(ssm, latest_available_start_times_new: dict[str, str]) -> None:
    logger.info(
        f"## Setting the new latest available start time SSM parameter to: '{latest_available_start_times_new}'"
    )
    ssm_res = ssm.put_parameter(
        Name=os.environ[LATEST_AVAILABLE_PARAMETER],
        # Description,  # Default to the existing description
        Value=json.dumps(latest_available_start_times_new),
        Type="String",
        Overwrite=True,
        # TODO: (OPTIONAL) A regular expression used to validate the parameter value.
        # AllowedPattern=,
        Tier="Standard",
        DataType="text",
    )
    logger.info(f"## SSM Put Parameter response: {ssm_res}")


def status_failed(responses: dict) -> dict:
    return {status_str: "FAILED", event_str: responses}


def valid_timestamp(i, latest_start_time: str, data_service: str, reader: FileMetaReader) -> int:
    obj_key = i["Key"]
    logger.debug(
        f"## Checking for valid timestamp: '{obj_key}' (data service: '{data_service}', "
        f"old latest start time: '{latest_start_time}')"
    )
    meta = reader.find_filename_meta(os.environ[SOURCE_NAME], obj_key, file_types=[data_service])
    logger.info(f"## File Meta: '{meta}'")
    if meta is not None:
        timestamp = meta["start_time"].strftime("%Y%m%d%H%M")
        logger.info(f"## Timestamp: '{timestamp}'")
        if int(timestamp) == int(latest_start_time):
            return timestamp
    return None


def lambda_handler(event, context):
    env_keys = {
        ACCOUNT_OWNER_ID,
        EVENT_META_KEY,
        SOURCE_NAME,
        SAT_DATA_SERVICES,
        LATEST_AVAILABLE_PARAMETER,
        FILENAMES_INFO,
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

    logger.info(f"## EVENT: {event}")

    # Retrieve old event object, for repeat invocation circumstances, in a Step Functions state machines
    if os.environ[EVENT_META_KEY] not in event and event_str in event:
        tmp_event = event[event_str]
        event = tmp_event

    s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
    ssm = boto3.client("ssm", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to S3 and SSM via client")

    s3fs_ = s3fs.S3FileSystem(anon=False)  # Default: Will use boto3's credential resolver

    data_service_files: dict[str, dict[str, list[str]]] = {}
    new_latest_available_start_times: dict[str, str] = {}
    settings = FileMetaReaderSettings()
    logger.info(f"## FileMetaReaderSettings: '{settings.filenames_info}'")
    file_meta_reader = FileMetaReader(settings)
    for data_service, s3_uri_props in event[os.environ[EVENT_META_KEY]]["data_service_s3_uri_props"].items():
        sat_data_source_meta = json.loads(os.environ[SAT_DATA_SERVICES])[data_service]
        num_files_required = int(sat_data_source_meta[sat_data_source_num_files])
        s3_uri_prefix: str = s3_uri_props[0]
        latest_start_time: str = s3_uri_props[1]
        if (
            (latest_available_start_times := get_latest_available_start_times(data_service))
            and data_service in latest_available_start_times
            and int(latest_start_time) < int(latest_available_start_times[data_service])
        ):
            # Bomb out if there are newer s3 objects, which are already available.
            return {status_str: "BOMB-OUT", event_str: event}
        new_latest_available_start_times[data_service] = latest_start_time
        s3_uri_full: str = f"{s3_uri_prefix}*{latest_start_time}*"

        logger.info(f"## Getting Files (for S3 URI): '{s3_uri_full}'")
        if sat_data_source_reader in sat_data_source_meta and (
            reader_val := sat_data_source_meta[sat_data_source_reader]
        ):
            logger.info(f"## Using Reader '{reader_val}' (for S3 URI): '{s3_uri_full}'")
            start_end_time = datetime.strptime(latest_start_time, "%Y%m%d%H%M")
            try:
                files_and_readers_mapping = find_files_and_readers(
                    base_dir=s3_uri_prefix,
                    fs=s3fs_,
                    reader=reader_val,
                    start_time=start_end_time,
                    end_time=start_end_time,
                )
                files = files_and_readers_mapping[reader_val]
            except ValueError as ex:
                logger.debug(
                    f"## Using Reader '{reader_val}' (for data service: '{data_service}'), did NOT work ... : {ex}"
                )
                return status_failed(event)
            logger.info(f"## Files: '{files}'")
            num_files_found = len(files)
            if num_files_found:
                logger.info(
                    f"## Using Reader '{reader_val}' (for data service: '{data_service}'), found: {num_files_found}"
                )
        else:
            logger.info(f"## Fallback - Manually Getting Files (for S3 URI): '{s3_uri_full}'")
            s3_uri = (s3_uri_prefix if s3_uri_prefix[-1] != "/" else s3_uri_prefix[:-1]).rsplit(sep="/", maxsplit=3)
            bucket_name = s3_uri[0].replace("s3://", "", 1)
            prefix_path = "/".join(s3_uri[-3:])
            files = []
            is_truncated = True
            next_continuation_token = None
            while is_truncated:
                s3_res = s3_list_objects(s3, bucket_name, prefix_path, continuation_token=next_continuation_token)
                is_truncated = s3_res["IsTruncated"]
                if "Contents" in s3_res:
                    files += [
                        f"{bucket_name}/{i['Key']}"
                        for i in s3_res["Contents"]
                        if valid_timestamp(i, latest_start_time, data_service, file_meta_reader)
                    ]
                if is_truncated and "NextContinuationToken" in s3_res:
                    next_continuation_token = s3_res["NextContinuationToken"]
            logger.info(f"## Files: '{files}'")
            num_files_found = len(files)
            logger.info(f"## Manual reading (for data service: '{data_service}'), found: {num_files_found}")

        if num_files_found < num_files_required:
            s3fs_.invalidate_cache(path=s3_uri_prefix)
            logger.info(
                f"## Did NOT find the expected {num_files_required} files (for data service: '{data_service}') ..."
            )
            return status_failed(event)
        data_service_files[data_service] = files

    set_latest_available_start_times(ssm, new_latest_available_start_times)

    logger.info(f"## Data service files: '{data_service_files}'")

    return {
        status_str: "SUCCEEDED",
        event_str: event,
        os.environ[EVENT_META_KEY]: {
            "data_service_files": data_service_files,
            "csa_timestamp": event[os.environ[EVENT_META_KEY]]["csa_timestamp"],
        },
    }
