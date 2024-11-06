import base64
import json
import logging
import os
import shutil
import sys
import tempfile
import traceback
import warnings
from datetime import date as datetime_date, timedelta
from pathlib import Path

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3
from botocore.exceptions import ClientError
from sih_lion import __version__ as sih_lion_version
from sih_lion.processors.base import Processor
from sih_lion.source import SourceConfig

# TODO: (OPTIONAL) Remove ignoring of Python warnings
warnings.filterwarnings("ignore")

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")
logger.info(f"sih-lion version: {sih_lion_version}")

ACCOUNT_OWNER_ID = "ACCOUNT_OWNER_ID"
S3_SAT_DATA_BUCKET_NAME = "S3_SAT_DATA_BUCKET_NAME"
PYPI_PACKAGE_S3_BUCKET_NAME = "PYPI_PACKAGE_S3_BUCKET_NAME"
PYPI_PACKAGE_S3_BUCKET_BRANCH = "PYPI_PACKAGE_S3_BUCKET_BRANCH"
EVENT_META_KEY = "EVENT_META_KEY"
DEPLOY_ENV = "DEPLOY_ENV"
SOURCE_NAME = "SOURCE_NAME"
S3_PARAM_DATA_BUCKET_NAME = "S3_PARAM_DATA_BUCKET_NAME"
S3_PARAM_DATA_BUCKET_OBJ_PREFIX = "S3_PARAM_DATA_BUCKET_OBJ_PREFIX"
CHECKSUM_ALGORITHM = "CHECKSUM_ALGORITHM"
KMS_MASTER_KEY_ID = "KMS_MASTER_KEY_ID"
ENCRYPTION_CONTEXT_KEY = "ENCRYPTION_CONTEXT_KEY"
CDK_STACK_NAME = "CDK_STACK_NAME"
TAGS = "TAGS"
SOURCE_MODULE_NAME = "SOURCE_MODULE_NAME"
SOURCE_CLASS_NAME = "SOURCE_CLASS_NAME"
SOURCE_SYSTEM_OBJS = "SOURCE_SYSTEM_OBJS"
FILENAMES_INFO = "FILENAMES_INFO"

TMP_FOP = "TMP_FOP"

status_str = "status"
responses_str = "responses"

static_grids_str = "static_grids"
csa_str = "csa"
csa_filename_ext = "npy"
param_data_str = "param_data"
param_filename_ext = "nc"
yesterday_str = "yesterday"
today_str = "today"


def get_sat_data(s3, data_service_files: dict[str, list[str]]) -> dict:
    logger.info("## Getting sat data")
    for data_service, files in dict(data_service_files).items():
        local_file_paths = []
        for f in files:
            obj_props = f.split(sep="/", maxsplit=1)  # Gets: [bucket_name, obj_name]
            local_file_path = os.path.join(os.environ[TMP_FOP], f)
            s3_download_fileobj(s3, bucket_name=obj_props[0], obj_key=obj_props[1], filename=local_file_path)
            local_file_paths.append(local_file_path)
        data_service_files[data_service] = local_file_paths
    logger.info(f"## Sat data: {data_service_files}")
    return data_service_files


def s3_download_fileobj(s3, bucket_name: str, obj_key: str, filename: str = None) -> str:
    if filename is None:
        filename = os.path.join(os.environ[TMP_FOP], bucket_name, obj_key)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "wb") as f:
        s3.download_fileobj(bucket_name, obj_key, f)
    return filename


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


def s3_put_object(s3, bucket_name: str, prefix_path: str, fip: str) -> dict:
    s3_res = {}

    with open(fip, "rb") as f:
        logger.info(f"## Creating a new S3 object: s3://{bucket_name}/{prefix_path}")
        try:
            s3_res["put_object"] = s3.put_object(
                ACL="bucket-owner-full-control",
                Body=f,
                Bucket=bucket_name,
                ChecksumAlgorithm=os.environ[CHECKSUM_ALGORITHM],
                Key=prefix_path,
                # TODO: (OPTIONAL) Add key-value pairs metadata for S3 object
                # Metadata={
                #     'string': 'string'
                # },
                ServerSideEncryption="aws:kms",
                StorageClass="STANDARD",
                SSEKMSKeyId=os.environ[KMS_MASTER_KEY_ID],
                SSEKMSEncryptionContext=base64.b64encode(
                    json.dumps({os.environ[ENCRYPTION_CONTEXT_KEY]: os.environ[CDK_STACK_NAME]}).encode("ascii")
                ).decode("ascii"),
                BucketKeyEnabled=True,
                # ObjectLockMode='GOVERNANCE' | 'COMPLIANCE',
                # ObjectLockRetainUntilDate=datetime(2015, 1, 1),
                # ObjectLockLegalHoldStatus='ON' | 'OFF',
                ExpectedBucketOwner=os.environ[ACCOUNT_OWNER_ID],
            )
            logger.info(f"## S3 Put Object response: {s3_res['put_object']}")
        except ClientError as ex:
            logger.error(f"## ERROR: {ex}")
            return status_failed(s3_res)

    try:
        s3_res["put_object_tagging"] = s3.put_object_tagging(
            Bucket=bucket_name,
            Key=prefix_path,
            ChecksumAlgorithm=os.environ[CHECKSUM_ALGORITHM],
            Tagging={"TagSet": json.loads(os.environ[TAGS])},
            ExpectedBucketOwner=os.environ[ACCOUNT_OWNER_ID],
        )
        logger.info(f"## S3 Put Object Tagging response: {s3_res['put_object_tagging']}")
    except ClientError as ex:
        logger.error(f"## ERROR: {ex}")
        return status_failed(s3_res)

    return s3_res


def status_failed(responses: dict) -> dict:
    return {status_str: "FAILED", responses_str: responses}


def valid_timestamp(i, latest_start_time_old: str) -> int:
    obj_key = i["Key"]
    logger.debug(f"## Checking for valid timestamp: '{obj_key}' (old latest start time: '{latest_start_time_old}')")
    timestamp = int(obj_key.rsplit(sep="-", maxsplit=2)[-2])
    return timestamp if timestamp > int(latest_start_time_old) else None


def clear_tmp_directory(tmp_fop: str):
    # pylint: disable=expression-not-assigned
    tmp_directory = Path(tmp_fop)
    # Remove all files
    [fp.unlink() for fp in tmp_directory.glob("*") if fp.is_file() or fp.is_symlink()]
    # Remove all directories
    [shutil.rmtree(dp) for dp in tmp_directory.glob("*") if dp.is_dir()]


def lambda_handler(event, context):
    env_keys = {
        ACCOUNT_OWNER_ID,
        S3_SAT_DATA_BUCKET_NAME,
        PYPI_PACKAGE_S3_BUCKET_NAME,
        PYPI_PACKAGE_S3_BUCKET_BRANCH,
        EVENT_META_KEY,
        DEPLOY_ENV,
        SOURCE_NAME,
        S3_PARAM_DATA_BUCKET_NAME,
        S3_PARAM_DATA_BUCKET_OBJ_PREFIX,
        CHECKSUM_ALGORITHM,
        KMS_MASTER_KEY_ID,
        ENCRYPTION_CONTEXT_KEY,
        CDK_STACK_NAME,
        TAGS,
        SOURCE_MODULE_NAME,
        SOURCE_CLASS_NAME,
        SOURCE_SYSTEM_OBJS,
        FILENAMES_INFO,
    }
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    logger.info(f"## EVENT: {event}")

    os.environ[TMP_FOP] = tempfile.gettempdir()
    logger.info(f"Temporary folder path: {os.environ[TMP_FOP]}")

    for k, v in json.loads(os.environ[SOURCE_SYSTEM_OBJS]).items():
        os.environ[k] = json.dumps(v)
        logger.info(f"Set env var {k}: {os.environ[k]}")

    s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to S3 via client")

    process_input = {
        **get_sat_data(s3, dict(event[os.environ[EVENT_META_KEY]]["data_service_files"])),
        **{
            static_grids_str: {
                i: s3_download_fileobj(
                    s3,
                    bucket_name=os.environ[PYPI_PACKAGE_S3_BUCKET_NAME],
                    obj_key=[
                        obj["Key"]
                        for obj in s3_list_objects(
                            s3,
                            os.environ[PYPI_PACKAGE_S3_BUCKET_NAME],
                            f"{os.environ[PYPI_PACKAGE_S3_BUCKET_BRANCH]}/sih_lion/"
                            f"{static_grids_str}/satellite/{os.environ[SOURCE_NAME]}/{i}",
                        )["Contents"]
                        if str(obj["Key"]).endswith(".npy")
                    ][0],
                )
                for i in ["geolocation", "scan_time_offset", "vaa", "vza"]
            },
            csa_str: {},
        },
    }

    today = datetime_date.today()
    dt_yesterday = (today - timedelta(days=1)).isoformat()
    dt_today = today.isoformat()
    dt_meta = {today_str: dt_today, yesterday_str: dt_yesterday}
    logger.info(f"## Date Meta: '{dt_meta}'")

    for key, date in dt_meta.items():
        bucket_name = os.environ[S3_SAT_DATA_BUCKET_NAME]
        prefix_path = f"{csa_str}/{os.environ[DEPLOY_ENV]}/{os.environ[SOURCE_NAME]}/{date}"
        s3_res_list = []
        csa_files = set()
        is_truncated = True
        next_continuation_token = None
        while is_truncated:
            s3_res = s3_list_objects(s3, bucket_name, prefix_path, continuation_token=next_continuation_token)
            s3_res_list.append(s3_res)
            is_truncated = s3_res["IsTruncated"]
            if "Contents" in s3_res:
                csa_files = csa_files.union({i["Key"].rsplit(sep="/", maxsplit=1)[-1] for i in s3_res["Contents"]})
            if is_truncated and "NextContinuationToken" in s3_res:
                next_continuation_token = s3_res["NextContinuationToken"]
        process_input[csa_str][key] = (
            s3_download_fileobj(s3, bucket_name=bucket_name, obj_key=f"{prefix_path}/{csa_files_list[0]}")
            if (csa_files_list := sorted(list(csa_files), reverse=True))
            else None
        )
        logger.info(f"## Found CSA file (for {key}): {process_input[csa_str][key]}")

    logger.info(f"## Process Input: '{process_input}'")

    # Create source config.
    _config = SourceConfig()

    processor = Processor.from_config(_config, logger)
    if processor is None:
        logger.error(f"## No processor created: {_config.CLASS}")
        raise RuntimeError(f"## No processor created: {_config.CLASS}")

    source_data = None
    try:
        source_data = processor.get_data(**process_input)
    except OSError as ex:
        logger.info(f"## Could not get processor source data: {ex}")
        traceback.print_exc()  # Prints the traceback for debugging

    if (csa_filename := source_data.get("csa")) is None:
        logger.error("## No CSA file found.")
        return {status_str: "FAILED"}

    s3_res = {param_data_str: {}}
    s3_put_object_failed = False

    # Write new CSA file to S3
    s3_res[csa_str] = s3_put_object(
        s3,
        os.environ[S3_SAT_DATA_BUCKET_NAME],
        f"{csa_str}/{os.environ[DEPLOY_ENV]}/{processor.name}/{dt_today}/{csa_filename}",
        os.path.join(os.environ[TMP_FOP], csa_filename),
    )
    if status_str in s3_res[csa_str]:
        s3_put_object_failed = True

    # Write new param data file(s) to S3
    if (atmos_param_filenames := source_data.get("params")) is not None:
        for atmos_param_filename in atmos_param_filenames:
            s3_res[param_data_str][atmos_param_filename] = s3_put_object(
                s3,
                os.environ[S3_PARAM_DATA_BUCKET_NAME],
                f"{os.environ[S3_PARAM_DATA_BUCKET_OBJ_PREFIX]}/{atmos_param_filename}",
                os.path.join(os.environ[TMP_FOP], atmos_param_filename),
            )
            if status_str in s3_res[param_data_str][atmos_param_filename]:
                s3_put_object_failed = True
    else:
        logger.info("## No param data files found.")

    logger.info(f"## Clear the temporary folder path: {os.environ[TMP_FOP]}")
    clear_tmp_directory(os.environ[TMP_FOP])
    logger.info("## Temporary folder path cleared")

    return {status_str: "FAILED" if s3_put_object_failed else "SUCCEEDED", responses_str: s3_res}
