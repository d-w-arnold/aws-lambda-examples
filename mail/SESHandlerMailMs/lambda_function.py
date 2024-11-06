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

SES_CONFIG_SET_MAPPING = "SES_CONFIG_SET_MAPPING"
SES_EMAIL_IDENTITY_ARN_MAPPING = "SES_EMAIL_IDENTITY_ARN_MAPPING"
SES_EMAIL_TEMPLATE_ARN_FORMAT = "SES_EMAIL_TEMPLATE_ARN_FORMAT"
SES_EMAIL_TEMPLATE_MAPPING = "SES_EMAIL_TEMPLATE_MAPPING"
SES_EMAIL_TEMPLATE_NAME_PREFIX = "SES_EMAIL_TEMPLATE_NAME_PREFIX"

SES_FEEDBACK_FORWARDING_EMAIL = "SES_FEEDBACK_FORWARDING_EMAIL"  # Optional
SES_FEEDBACK_FORWARDING_EMAIL_IDENTITY_ARN = "SES_FEEDBACK_FORWARDING_EMAIL_IDENTITY_ARN"  # Optional

from_email_address_str = "from_email_address"
to_email_addresses_str = "to_email_addresses"
reply_to_addresses_str = "reply_to_addresses"
template_name_str = "template_name"
template_data_str = "template_data"
bulk_str = "bulk"  # Optional


def lambda_handler(event, context):
    env_keys = {
        SES_CONFIG_SET_MAPPING,
        SES_EMAIL_IDENTITY_ARN_MAPPING,
        SES_EMAIL_TEMPLATE_ARN_FORMAT,
        SES_EMAIL_TEMPLATE_MAPPING,
        SES_EMAIL_TEMPLATE_NAME_PREFIX,
    }
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    is_bulk = bool(bulk_str in event and event[bulk_str])

    keys: list = [
        from_email_address_str,  # str
        to_email_addresses_str,  # list[str]
        reply_to_addresses_str,  # list[str]
        template_name_str,  # str
        template_data_str,  # dict - e.g. {to_email_address: {name: 'John Doe'}} if is_bulk else {name: 'John Doe'}
    ]
    if all(k in event for k in keys):
        logger.info(f"## EVENT: {event}")
    else:
        logger.error(f"## Not all {keys} in EVENT: {event}")
        sys.exit(1)

    ses_res = {}

    ses = boto3.client("sesv2", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to SES via client")

    from_email_address = event[from_email_address_str]
    from_email_address_identity_arn = json.loads(os.environ[SES_EMAIL_IDENTITY_ARN_MAPPING])[from_email_address]
    to_email_addresses = event[to_email_addresses_str]
    reply_to_addresses = event[reply_to_addresses_str]
    template_name = f"{os.environ[SES_EMAIL_TEMPLATE_NAME_PREFIX]}{event[template_name_str]}"
    template_arn = f"{os.environ[SES_EMAIL_TEMPLATE_ARN_FORMAT][:-1]}{template_name}"
    configuration_set_name = json.loads(os.environ[SES_CONFIG_SET_MAPPING])[from_email_address]

    send_email_args = (
        {
            "FeedbackForwardingEmailAddress": os.environ[SES_FEEDBACK_FORWARDING_EMAIL],
            "FeedbackForwardingEmailAddressIdentityArn": os.environ[SES_FEEDBACK_FORWARDING_EMAIL_IDENTITY_ARN],
        }
        if all(k in os.environ for k in [SES_FEEDBACK_FORWARDING_EMAIL, SES_FEEDBACK_FORWARDING_EMAIL_IDENTITY_ARN])
        else {}
    )

    if not is_bulk or (is_bulk and len(to_email_addresses) == 1):
        try:
            ses_res["send_email"] = ses.send_email(
                FromEmailAddress=from_email_address,
                FromEmailAddressIdentityArn=from_email_address_identity_arn,
                Destination={
                    "ToAddresses": to_email_addresses,
                    # 'CcAddresses': [
                    #     'string',
                    # ],
                    # 'BccAddresses': [
                    #     'string',
                    # ]
                },
                ReplyToAddresses=reply_to_addresses,
                Content={
                    "Template": {
                        "TemplateName": template_name,
                        "TemplateArn": template_arn,
                        "TemplateData": json.dumps(
                            event[template_data_str][to_email_addresses[0]] if is_bulk else event[template_data_str]
                        ),
                    }
                },
                # TODO: (OPTIONAL) Add a list of tags, that correspond to characteristics of the email that
                #  you define, so that you can publish email sending events.
                # EmailTags=[
                #     {
                #         'Name': 'string',
                #         'Value': 'string'
                #     },
                # ],
                ConfigurationSetName=configuration_set_name,
                **send_email_args,
            )
            logger.info(f"## SES Send Email response: {ses_res['send_email']}")
        except ClientError as ex:
            logger.error(f"## SES Send Email ERROR: '{ex}'")
    else:
        try:
            ses_res["send_bulk_email"] = ses.send_bulk_email(
                FromEmailAddress=from_email_address,
                FromEmailAddressIdentityArn=from_email_address_identity_arn,
                ReplyToAddresses=reply_to_addresses,
                # DefaultEmailTags=[
                #     {
                #         'Name': 'string',
                #         'Value': 'string'
                #     },
                # ],
                DefaultContent={
                    "Template": {
                        "TemplateName": template_name,
                        "TemplateArn": template_arn,
                        "TemplateData": json.dumps({}),
                    }
                },
                BulkEmailEntries=[
                    {
                        "Destination": {
                            "ToAddresses": [to_email_address],
                            # 'CcAddresses': [
                            #     'string',
                            # ],
                            # 'BccAddresses': [
                            #     'string',
                            # ]
                        },
                        # TODO: (OPTIONAL) Add a list of tags, that correspond to characteristics of the email that
                        #  you define, so that you can publish email sending events.
                        # 'ReplacementTags': [
                        #     {
                        #         'Name': 'string',
                        #         'Value': 'string'
                        #     },
                        # ],
                        "ReplacementEmailContent": {
                            "ReplacementTemplate": {
                                "ReplacementTemplateData": json.dumps(event[template_data_str][to_email_address])
                            }
                        },
                    }
                    for to_email_address in to_email_addresses
                ],
                ConfigurationSetName=configuration_set_name,
                **send_email_args,
            )
            logger.info(f"## SES Send Bulk Email response: {ses_res['send_bulk_email']}")
        except ClientError as ex:
            logger.error(f"## SES Send Bulk Email ERROR: '{ex}'")

    return ses_res
