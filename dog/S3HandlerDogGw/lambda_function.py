import json
import logging
import os
import sys

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

task_str = "task"
bucket_name_str = "bucket_name"

error_str = "Error"
responses_str = "SuccessResponses"
sep_str = "@@"


def client_error_to_str(ex) -> str:
    return str(ex)


def s3_delete_bucket(s3_res: dict, bucket_name: str, s3_bucket=None) -> dict:
    if s3_bucket is None:
        s3 = boto3.resource("s3", region_name=os.environ["AWS_REGION"])
        logger.info("## Connected to S3 via resource")

        s3_bucket = s3.Bucket(bucket_name)
        logger.info(f"## Looking at S3 bucket: {bucket_name}")

    logger.info(f"## S3 Deleting Bucket: {bucket_name}")
    try:
        s3_res["delete_bucket"] = s3_bucket.delete()
        logger.info(f"## S3 Delete Bucket response: {s3_res['delete_bucket']}")
    except ClientError as ex:
        logger.error(f"## S3 Delete Bucket ERROR: '{ex}'")
        return status_failed(client_error_to_str(ex), s3_res)

    return None


def status_failed(error: str, responses: dict) -> dict:
    return {error_str: error, responses_str: responses}


def status_success(responses: dict) -> dict:
    return {responses_str: responses}


def lambda_handler(event, context):
    env_keys = {"ACCOUNT_OWNER_ID", "CHECKSUM_ALGORITHM", "KMS_MASTER_KEY_ID", "TAGS"}
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    keys: list = [task_str, bucket_name_str]
    if all(k in event for k in keys):
        logger.info(f"## EVENT: {event}")
    else:
        logger.error(f"## Not all {keys} in EVENT: {event}")
        sys.exit(1)

    event_task = event[task_str]
    event_bucket_name = event[bucket_name_str]

    s3_res = {}

    if event_task == "create_s3_bucket":
        s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
        logger.info("## Connected to S3 via client")

        logger.info(f"## S3 Creating Bucket: {event_bucket_name}")
        try:
            s3_res["create_bucket"] = s3.create_bucket(
                ACL="private",
                Bucket=event_bucket_name,
                CreateBucketConfiguration={"LocationConstraint": os.environ["AWS_REGION"]},
                ObjectLockEnabledForBucket=False,
                ObjectOwnership="ObjectWriter",
            )
            logger.info(f"## S3 Create Bucket response: {s3_res['create_bucket']}")
        except ClientError as ex:
            logger.error(f"## S3 Create Bucket ERROR: '{ex}'")
            return status_failed(client_error_to_str(ex), s3_res)

        status = None

        public_access_block_config: dict = {
            "BlockPublicAcls": True,
            "IgnorePublicAcls": True,
            "BlockPublicPolicy": True,
            "RestrictPublicBuckets": True,
        }
        logger.info(
            f"## S3 Putting Public Access Block: {'ALL' if all(v for _, v in public_access_block_config.items()) else ' '.join([k for k, v in public_access_block_config.items() if v])}"
        )
        try:
            s3_res["put_public_access_block"] = s3.put_public_access_block(
                Bucket=event_bucket_name,
                ChecksumAlgorithm=os.environ["CHECKSUM_ALGORITHM"],
                PublicAccessBlockConfiguration=public_access_block_config,
                ExpectedBucketOwner=os.environ["ACCOUNT_OWNER_ID"],
            )
            logger.info(f"## S3 Put Public Access Block response: {s3_res['put_public_access_block']}")
        except ClientError as ex:
            logger.error(f"## S3 Put Public Access Block ERROR: '{ex}'")
            status = client_error_to_str(ex)

        if status is None:
            logger.info(f"## S3 Putting Bucket Encryption, using KMS Master Key ID: {os.environ['KMS_MASTER_KEY_ID']}")
            try:
                s3_res["put_bucket_encryption"] = s3.put_bucket_encryption(
                    Bucket=event_bucket_name,
                    ChecksumAlgorithm=os.environ["CHECKSUM_ALGORITHM"],
                    ServerSideEncryptionConfiguration={
                        "Rules": [
                            {
                                "ApplyServerSideEncryptionByDefault": {
                                    "SSEAlgorithm": "aws:kms",
                                    "KMSMasterKeyID": os.environ["KMS_MASTER_KEY_ID"],
                                },
                                "BucketKeyEnabled": True,
                            },
                        ]
                    },
                    ExpectedBucketOwner=os.environ["ACCOUNT_OWNER_ID"],
                )
                logger.info(f"## S3 Put Bucket Encryption response: {s3_res['put_bucket_encryption']}")
            except ClientError as ex:
                logger.error(f"## S3 Put Bucket Encryption ERROR: '{ex}'")
                status = client_error_to_str(ex)

        if status is None:
            bucket_policy = json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Deny",
                            "Principal": "*",
                            "Action": "s3:PutObject",
                            "Resource": f"arn:aws:s3:::{event_bucket_name}/*",
                            "Condition": {
                                "StringNotEquals": {"s3:x-amz-server-side-encryption": "aws:kms"},
                                "StringEquals": {"s3:x-amz-acl": "bucket-owner-full-control"},
                            },
                        }
                    ],
                },
                default=str,
            )
            logger.info(f"## S3 Putting Bucket Policy, using S3 bucket policy: {bucket_policy}")
            try:
                s3_res["put_bucket_policy"] = s3.put_bucket_policy(
                    Bucket=event_bucket_name,
                    ChecksumAlgorithm=os.environ["CHECKSUM_ALGORITHM"],
                    ConfirmRemoveSelfBucketAccess=True,
                    Policy=bucket_policy,
                    ExpectedBucketOwner=os.environ["ACCOUNT_OWNER_ID"],
                )
                logger.info(f"## S3 Put Bucket Policy response: {s3_res['put_bucket_policy']}")
            except ClientError as ex:
                logger.error(f"## S3 Put Bucket Policy ERROR: '{ex}'")
                status = client_error_to_str(ex)

        if status is None:
            tag_set = json.loads(os.environ["TAGS"])
            logger.info(f"## S3 Putting Bucket Tagging, using tags: {tag_set}")
            try:
                s3_res["put_bucket_tagging"] = s3.put_bucket_tagging(
                    Bucket=event_bucket_name,
                    ChecksumAlgorithm=os.environ["CHECKSUM_ALGORITHM"],
                    Tagging={"TagSet": tag_set},
                    ExpectedBucketOwner=os.environ["ACCOUNT_OWNER_ID"],
                )
                logger.info(f"## S3 Put Bucket Tagging response: {s3_res['put_bucket_tagging']}")
            except ClientError as ex:
                logger.error(f"## S3 Put Bucket Tagging ERROR: '{ex}'")
                status = client_error_to_str(ex)

        if status is not None:
            return status_failed(
                sep_str.join(
                    ([status] + res[error_str].split(sep=sep_str))
                    if (res := s3_delete_bucket(s3_res, event_bucket_name))
                    else [status, "Successfully deleted (cleaned-up) created S3 bucket"]
                ),
                s3_res,
            )

        return status_success(s3_res)

    if event_task == "delete_s3_bucket":
        s3 = boto3.resource("s3", region_name=os.environ["AWS_REGION"])
        logger.info("## Connected to S3 via resource")

        s3_bucket = s3.Bucket(event_bucket_name)
        logger.info(f"## Looking at S3 bucket: {event_bucket_name}")

        # Deleting objects
        s3_bucket_objs_res = []
        try:
            s3_bucket_objs_res = list(s3_bucket.objects.all())
            logger.info(f"## All S3 bucket objects response: {s3_bucket_objs_res}")
        except ClientError as ex:
            logger.error(f"## All S3 bucket objects ERROR: '{ex}'")

        for s3_obj in s3_bucket_objs_res:
            logger.info(f"## Deleting object: {s3_obj}")
            try:
                s3_obj.delete()
            except ClientError as ex:
                logger.error(f"## S3 Delete object ERROR: {ex}")

        # Deleting objects versions, if S3 versioning enabled
        s3_bucket_objs_vers_res = []
        try:
            s3_bucket_objs_vers_res = list(s3_bucket.objects.all())
            logger.info(f"## All S3 bucket objects versions response: {s3_bucket_objs_vers_res}")
        except ClientError as ex:
            logger.error(f"## All S3 bucket objects versions ERROR: {ex}")

        for s3_obj_ver in s3_bucket_objs_vers_res:
            logger.info(f"## Deleting object versions: {s3_obj_ver}")
            try:
                s3_obj_ver.delete()
            except ClientError as ex:
                logger.error(f"## S3 Delete object versions ERROR: {ex}")

        if res := s3_delete_bucket(s3_res, event_bucket_name, s3_bucket=s3_bucket):
            return res

        return status_success(s3_res)

    logger.error(f"## Event {task_str} did not match any known {task_str}: {event}")
    sys.exit(1)
