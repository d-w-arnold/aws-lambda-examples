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

IMAGE_URI = "image-uri"
LAMBDA_FUNC_NAMES = "lambda-func-names"

status_str = "status"
responses_str = "responses"


def lambda_handler(event, context):
    keys: list = [IMAGE_URI, LAMBDA_FUNC_NAMES]
    if all(k in event for k in keys):
        logger.info(f"## EVENT: {event}")
    else:
        logger.error(f"## Not all {keys} in EVENT: {event}")
        sys.exit(1)

    lambda_ = boto3.client("lambda", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to lambda via client")

    lambda_res = {}

    for lambda_func_name in json.loads(event[LAMBDA_FUNC_NAMES]):
        lambda_res[lambda_func_name] = {}

        try:
            lambda_res[lambda_func_name]["update_function_code"] = lambda_.update_function_code(
                FunctionName=lambda_func_name,
                ImageUri=event[IMAGE_URI],
            )
            logger.info(
                f"## Lambda Update Function Code response: {lambda_res[lambda_func_name]['update_function_code']}"
            )
        except ClientError as ex:
            logger.error(f"## ERROR: {ex}")
            return {status_str: "FAILED", responses_str: lambda_res}

    return {status_str: "SUCCEEDED", responses_str: lambda_res}
