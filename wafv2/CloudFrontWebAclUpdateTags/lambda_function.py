import json
import logging
import os
import sys

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")


def lambda_handler(event, context):
    env_keys = {"TAGS"}
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    keys: list = ["arn"]
    if all(k in event for k in keys):
        logger.info(f"## EVENT: {event}")
    else:
        logger.error(f"## Not all {keys} in EVENT: {event}")
        sys.exit(1)

    wafv2 = boto3.client("wafv2", region_name="us-east-1")
    logger.info("## Connected to WAFv2 via client")

    wafv2_res = {}

    arn: str = event["arn"]

    wafv2_res["list_tags_for_resource_before"] = wafv2.list_tags_for_resource(ResourceARN=arn)
    logger.info(f"## WAFv2 List Tags For Resource response: {wafv2_res['list_tags_for_resource_before']}")

    wafv2_res["tag_resource"] = wafv2.tag_resource(ResourceARN=arn, Tags=json.loads(os.environ["TAGS"]))
    logger.info(f"## WAFv2 Tag Resource response: {wafv2_res['tag_resource']}")

    # wafv2_res["untag_resource"] = wafv2.untag_resource(
    #     ResourceARN=arn,
    #     TagKeys=["cost-centre", "project-product", "project-product-service"]
    # )
    # logger.info(f"## WAFv2 Un-Tag Resource response: {wafv2_res['untag_resource']}")

    wafv2_res["list_tags_for_resource_after"] = wafv2.list_tags_for_resource(ResourceARN=arn)
    logger.info(f"## WAFv2 List Tags For Resource response: {wafv2_res['list_tags_for_resource_after']}")

    return wafv2_res
