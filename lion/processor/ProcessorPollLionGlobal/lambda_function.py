import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

ACCOUNT_OWNER_ID = "ACCOUNT_OWNER_ID"
BUCKET_NAME_SOURCE = "BUCKET_NAME_SOURCE"
BUCKET_NAME_DEST = "BUCKET_NAME_DEST"
SAT_DATA_ORG = "SAT_DATA_ORG"
SOURCE_NAME = "SOURCE_NAME"
SAT_DATA_SERVICES = "SAT_DATA_SERVICES"

sat_data_source_folder = "folder"
sat_data_source_filter = "filter"
padding_day_of_year = 3
padding_hour = 2


def get_obj_prefix_sources(sat_data_service_meta: dict) -> list[str]:
    today = datetime.now(timezone.utc)
    if os.environ[SOURCE_NAME].startswith("himawari"):
        min_interval: int = 10
        last_timestamp = today.replace(minute=(today.minute // min_interval) * min_interval)
        obj_prefix_sources: list[str] = [
            (
                f"{sat_data_service_meta[sat_data_source_folder]}/"
                f"{today.strftime('%Y/%m/%d')}/"
                f"{hours_mins_minus_some_mins.strftime('%H%M')}"
            )
            for i in range(int(60 / min_interval))
            if (hours_mins_minus_some_mins := last_timestamp - timedelta(minutes=i * min_interval))
        ]
    else:
        hours_to_check: int = 2  # The number of hours to check sat data for (from today's available sat data).
        obj_prefix_sources: list[str] = [
            (
                f"{sat_data_service_meta[sat_data_source_folder]}/"
                f"{today_minus_some_hours.year}/"
                f"{str(today_minus_some_hours.timetuple().tm_yday).zfill(padding_day_of_year)}/"
                f"{str(today_minus_some_hours.hour).zfill(padding_hour)}"
            )
            for i in range(hours_to_check)
            if (today_minus_some_hours := today - timedelta(hours=i))
        ]
    return obj_prefix_sources


def lambda_handler(event, context):
    env_keys = {
        ACCOUNT_OWNER_ID,
        BUCKET_NAME_SOURCE,
        BUCKET_NAME_DEST,
        SAT_DATA_ORG,
        SOURCE_NAME,
        SAT_DATA_SERVICES,
    }
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    s3 = boto3.resource("s3", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to S3 via resource")

    logger.info(
        f"## Moving S3 objects: (S3 bucket source) '{os.environ[BUCKET_NAME_SOURCE]}' -> "
        f"(S3 bucket dest) '{os.environ[BUCKET_NAME_DEST]}-%Y'"
    )

    total_moved: int = 0
    total_skipped: int = 0
    moved_objs: list[str] = []
    skipped_objs: list[str] = []
    for sat_data_service, sat_data_service_meta in dict(json.loads(os.environ[SAT_DATA_SERVICES])).items():
        for obj_prefix_source in get_obj_prefix_sources(sat_data_service_meta):
            for obj in s3.Bucket(os.environ[BUCKET_NAME_SOURCE]).objects.filter(Prefix=obj_prefix_source):
                obj_key = str(obj.key).rsplit(sep="/", maxsplit=1)[-1]
                if (
                    sat_data_source_filter in sat_data_service_meta
                    and sat_data_service_meta[sat_data_source_filter] is not None
                    and sat_data_service_meta[sat_data_source_filter] not in obj_key
                ):
                    continue
                obj_prefix_dest = f"{os.environ[SAT_DATA_ORG]}/{os.environ[SOURCE_NAME]}/{sat_data_service}/{obj_key}"
                s3_obj = s3.Object(os.environ[BUCKET_NAME_DEST], obj_prefix_dest)
                try:
                    s3_obj.load()
                    logger.info(f"## Skipping (The S3 object already exists): '{obj_prefix_dest}'")
                    total_skipped += 1
                    skipped_objs.append(obj_prefix_dest)
                except ClientError as ex:
                    if ex.response["Error"]["Code"] != "404":
                        # Cannot determine whether the S3 object does not exist.
                        logger.error(f"## Skipping ({ex}): '{obj_prefix_dest}'")
                        continue
                    s3_obj.copy(
                        CopySource={
                            "Bucket": obj.bucket_name,
                            "Key": obj.key,
                        },
                        ExtraArgs={
                            # "ExpectedBucketOwner": os.environ[ACCOUNT_OWNER_ID],  # ONLY use if we own the source S3 bucket.
                            # "StorageClass": "STANDARD",  # ONLY use if in the destination S3 bucket the new object should have a non-STANDARD storage class
                        },
                    )
                    total_moved += 1
                    moved_objs.append(obj_prefix_dest)

    logger.info(f"## Moved S3 objects (count: {total_moved}): {moved_objs}")
    logger.info(f"## Skipped S3 objects (count: {total_skipped}): {skipped_objs}")
