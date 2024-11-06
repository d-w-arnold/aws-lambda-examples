import logging
import os
import sys
from datetime import timedelta, date as datetime_date

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
DEPLOY_ENV = "DEPLOY_ENV"
SOURCE_NAME = "SOURCE_NAME"
STORAGE_CLASS = "STORAGE_CLASS"

csa_str = "csa"
yesterday_str = "yesterday"


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


def lambda_handler(event, context):
    env_keys = {ACCOUNT_OWNER_ID, BUCKET_NAME_SOURCE, BUCKET_NAME_DEST, DEPLOY_ENV, SOURCE_NAME}
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    s3_client = boto3.client("s3", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to S3 via client")

    s3_resource = boto3.resource("s3", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to S3 via resource")

    dt_yesterday = (datetime_date.today() - timedelta(days=1)).isoformat()
    dt_meta = {yesterday_str: dt_yesterday}
    logger.info(f"## Date Meta: '{dt_meta}'")

    prefix_path = f"{csa_str}/{os.environ[DEPLOY_ENV]}/{os.environ[SOURCE_NAME]}/{dt_yesterday}"
    s3_res_list = []
    csa_files = set()
    is_truncated = True
    next_continuation_token = None
    while is_truncated:
        s3_res = s3_list_objects(
            s3_client, os.environ[BUCKET_NAME_SOURCE], prefix_path, continuation_token=next_continuation_token
        )
        s3_res_list.append(s3_res)
        is_truncated = s3_res["IsTruncated"]
        if "Contents" in s3_res:
            csa_files = csa_files.union({i["Key"].rsplit(sep="/", maxsplit=1)[-1] for i in s3_res["Contents"]})
        if is_truncated and "NextContinuationToken" in s3_res:
            next_continuation_token = s3_res["NextContinuationToken"]
    if csa_files_list := sorted(list(csa_files), reverse=True):
        obj_key = f"{prefix_path}/{csa_files_list[0]}"
        logger.info(f"## Found latest CSA file (for {yesterday_str}): {obj_key}")
        s3_obj = s3_resource.Object(os.environ[BUCKET_NAME_DEST], obj_key)
        try:
            s3_obj.load()
            logger.info(f"## Skipping (The S3 object already exists): '{obj_key}'")
        except ClientError as ex:
            if ex.response["Error"]["Code"] != "404":
                # Cannot determine whether the S3 object does not exist.
                logger.error(f"## Skipping ({ex}): '{obj_key}'")
            else:
                s3_obj.copy(
                    CopySource={"Bucket": os.environ[BUCKET_NAME_SOURCE], "Key": obj_key},
                    ExtraArgs={
                        "ExpectedBucketOwner": os.environ[ACCOUNT_OWNER_ID],
                        "StorageClass": os.getenv(STORAGE_CLASS, "GLACIER_IR"),
                    },
                )
                logger.info(f"## Archived the latest CSA file (for {yesterday_str}): {obj_key}")
    else:
        logger.info(f"## No latest CSA file (for {yesterday_str}) found..")
