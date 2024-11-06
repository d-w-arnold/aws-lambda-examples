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

BUCKET_NAME = "BUCKET_NAME"
BUCKET_OBJ_KEY = "BUCKET_OBJ_KEY"
LAMBDA_FUNCTION_EXTRACT = "LAMBDA_FUNCTION_EXTRACT"
LAMBDA_LAYER_NAME = "LAMBDA_LAYER_NAME"
LAMBDA_LAYER_DESC = "LAMBDA_LAYER_DESC"
LAMBDA_LAYER_RUNTIMES = "LAMBDA_LAYER_RUNTIMES"
LAMBDA_LAYER_ARCHITECTURES = "LAMBDA_LAYER_ARCHITECTURES"

status_str = "status"
responses_str = "responses"


def status_failed(responses: dict) -> dict:
    return {status_str: "FAILED", responses_str: responses}


def lambda_handler(event, context):
    env_keys = {
        BUCKET_NAME,
        BUCKET_OBJ_KEY,
        LAMBDA_FUNCTION_EXTRACT,
        LAMBDA_LAYER_NAME,
        LAMBDA_LAYER_DESC,
        LAMBDA_LAYER_RUNTIMES,
        LAMBDA_LAYER_ARCHITECTURES,
    }
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    lambda_ = boto3.client("lambda", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to lambda via client")

    lambda_res = {}

    logger.info(
        f"## Creating a Lambda layer from .zip archive file: "
        f"s3://{os.environ[BUCKET_NAME]}/{os.environ[BUCKET_OBJ_KEY]}"
    )
    try:
        lambda_res["publish_layer_version"] = lambda_.publish_layer_version(
            LayerName=os.environ[LAMBDA_LAYER_NAME],
            Description=os.environ[LAMBDA_LAYER_DESC],
            Content={
                "S3Bucket": os.environ[BUCKET_NAME],
                "S3Key": os.environ[BUCKET_OBJ_KEY],
            },
            CompatibleRuntimes=[i.strip() for i in os.environ[LAMBDA_LAYER_RUNTIMES].split(os.getenv("SEP", ","))],
            CompatibleArchitectures=[
                i.strip() for i in os.environ[LAMBDA_LAYER_ARCHITECTURES].split(os.getenv("SEP", ","))
            ],
        )
        logger.info(f"## Lambda Publish Layer Version response: {lambda_res['publish_layer_version']}")
    except ClientError as ex:
        logger.error(f"## ERROR: {ex}")
        return status_failed(lambda_res)

    for lambda_func_name in json.loads(os.environ[LAMBDA_FUNCTION_EXTRACT]):
        lambda_res[lambda_func_name] = {}

        try:
            lambda_res[lambda_func_name]["get_function_configuration"] = lambda_.get_function_configuration(
                FunctionName=lambda_func_name,
            )
            logger.info(
                f"## Lambda Get Function Configuration response: {lambda_res[lambda_func_name]['get_function_configuration']}"
            )
        except ClientError as ex:
            logger.error(f"## ERROR: {ex}")
            return status_failed(lambda_res)

        try:
            lambda_res[lambda_func_name]["update_function_configuration"] = lambda_.update_function_configuration(
                FunctionName=lambda_func_name,
                Layers=[
                    i["Arn"]
                    for i in lambda_res[lambda_func_name]["get_function_configuration"]["Layers"]
                    if lambda_res["publish_layer_version"]["LayerArn"] not in i["Arn"]
                ]
                + [lambda_res["publish_layer_version"]["LayerVersionArn"]],
            )
            logger.info(
                f"## Lambda Update Function Configuration response: {lambda_res[lambda_func_name]['update_function_configuration']}"
            )
        except ClientError as ex:
            logger.error(f"## ERROR: {ex}")
            return status_failed(lambda_res)

    return {status_str: "SUCCEEDED", responses_str: lambda_res}
