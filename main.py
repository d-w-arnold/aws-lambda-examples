import json
import os
from pprint import pprint

import amplify.AmplifyMsTeamsNotification.lambda_function as amplify_ms_teams
import codepipeline.CodepipelineMsTeamsNotification.lambda_function as codepipeline_ms_teams
import ec2.EC2InstanceAutoStart.lambda_function as ec2_start
import ec2.EC2InstanceAutoStop.lambda_function as ec2_stop
import lion.extractor.ExtractorLayerLionMs.lambda_function as extractor_layer_lion_ms
import lion.processor.ProcessorArchiveLionGlobal.lambda_function as processor_archive_lion_global
import lion.processor.ProcessorPollLionGlobal.lambda_function as processor_poll_lion_global
import elasticache.EcRedisAutoStart.lambda_function as elasticache_redis_auto_start
import elasticache.EcRedisAutoStop.lambda_function as elasticache_redis_auto_stop
import mail.SESHandlerMailMs.lambda_function as ses_handler_mail_ms
import mail.cloudwatch.CWMsTeamsNotif.lambda_function as cloudwatch_ms_teams
import metoffice.ArchiveMetofficeStorage.lambda_function as metoffice_storage_archive
import metoffice.MonitorMetofficeStorage.lambda_function as metoffice_storage_monitor
import rds.RDSInstanceAutoStart.lambda_function as rds_start
import rds.RDSInstanceAutoStop.lambda_function as rds_stop
import rds.mysql.init.lambda_function as mysql_init
import dog.S3HandlerDogGw.lambda_function as s3_handler_dog_gw
import sns.SNSMsTeamsNotif.lambda_function as sns_ms_teams
import version.VersionMeta.lambda_function as version_meta
import wafv2.CloudFrontWebAclUpdateTags.lambda_function as cloudfront_web_acl_update_tags
import weatherapi.DownloadWeatherAPIStorage.lambda_function as weatherapi_storage_download


# from amplify.mock_events import (
#     event_started as amplify_event_started,
#     event_started_pr as amplify_event_started_pr,
#     event_succeeded as amplify_event_succeeded,
# )
# from mail.cloudwatch.mock_events import (
#     bounce as cloudwatch_bounce,
#     complaint as cloudwatch_complaint,
#     ec_redis_auto as cloudwatch_ec_redis_auto,
#     rds_auto as cloudwatch_rds_auto,
#     ecs_errors_spdt_stag as cloudwatch_ecs_errors_spdt_stag,
#     ecs_errors_spdt_sihd as cloudwatch_ecs_errors_spdt_sihd,
#     ecs_errors_s4h_stag as cloudwatch_ecs_errors_s4h_stag
# )
# from codepipeline.mock_events import (
#     event_started as codepipeline_event_started,
#     event_succeeded as codepipeline_event_succeeded,
#     event_failed as codepipeline_event_failed,
# )
# from mail.sns.mock_events import (
#     verify as sns_verify,
#     send as sns_send,
#     rendering_failure as sns_rendering_failure,
#     reject as sns_reject,
#     delivery as sns_delivery,
#     bounce as sns_bounce,
#     complaint as sns_complaint,
#     delivery_delay as sns_delivery_delay,
#     subscription as sns_subscription,
#     proxy_stop as sns_proxy_stop,
#     vpn_start as sns_vpn_start,
#     vpn_stop as sns_vpn_stop,
#     pypi_start as sns_pypi_start,
#     pypi_stop as sns_pypi_stop,
#     ec_redis_snapshot_comp as sns_ec_redis_snapshot_comp,
#     ec_redis_delete_comp as sns_ec_redis_delete_comp
#     sns_mob_push as sns_mob_push
# )


def run_amplify_ms_teams_notification(event_obj: dict):
    os.environ["MAPPING_PARAMETER"] = "/AmplifyCiCd/webhook-url-mappings"
    pprint(amplify_ms_teams.lambda_handler(event_obj, {}))


def run_cloudwatch_ms_teams_notification(event_obj: dict):
    os.environ[
        "WEBHOOK_URL"
    ] = "https://foobarcouk.webhook.office.com/webhookb2/c1e5b2c2-0321-42aa-bd97-d850530acf11@f670d7ce-50c9-4c74-b98f-49edf7903c82/IncomingWebhook/1c0d49000bfc49cebc56fdd0630c82fd/71b0843c-25cd-4d4a-be5c-b5e954aad055"
    pprint(cloudwatch_ms_teams.lambda_handler(event_obj, {}))


def run_codepipeline_ms_teams_notification(event_obj: dict):
    os.environ["MAPPING_PARAMETER"] = "/CodepipelineCiCd/webhook-url-mappings"
    pprint(codepipeline_ms_teams.lambda_handler(event_obj, {}))


def run_ec2_instance_auto_start():
    os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "EC2InstanceAutoStart"
    os.environ["SNS_TOPIC"] = "arn:aws:sns:eu-west-2:123456789123:LambdaEc2InstanceAuto_EC2InstanceAutoStart"
    os.environ["TAG_KEY"] = "auto-start"
    os.environ["TAG_VALUES"] = "1"
    os.environ[
        "WEBHOOK_URL"
    ] = "https://foobarcouk.webhook.office.com/webhookb2/c1e5b2c2-0321-42aa-bd97-d850530acf11@f670d7ce-50c9-4c74-b98f-49edf7903c82/IncomingWebhook/41133474ca4d4f5e8cb91f75759deb5c/71b0843c-25cd-4d4a-be5c-b5e954aad055"
    # os.environ["SEP"] = ","  # Optional
    pprint(ec2_start.lambda_handler({}, {}))


def run_ec2_instance_auto_stop():
    os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "EC2InstanceAutoStop"
    os.environ["SNS_TOPIC"] = "arn:aws:sns:eu-west-2:123456789123:LambdaEc2InstanceAuto_EC2InstanceAutoStop"
    os.environ["TAG_KEY"] = "auto-stop"
    os.environ["TAG_VALUES"] = "-1"  # Not set to either [0,1] to avoid accidental turn-offs of running EC2 instances
    os.environ[
        "WEBHOOK_URL"
    ] = "https://foobarcouk.webhook.office.com/webhookb2/c1e5b2c2-0321-42aa-bd97-d850530acf11@f670d7ce-50c9-4c74-b98f-49edf7903c82/IncomingWebhook/41133474ca4d4f5e8cb91f75759deb5c/71b0843c-25cd-4d4a-be5c-b5e954aad055"
    # os.environ["SEP"] = ","  # Optional
    pprint(ec2_stop.lambda_handler({}, {}))


def run_lion_extractor_layer_lion_ms():
    os.environ["BUCKET_NAME"] = "sihlion-eu-west-2"
    os.environ["BUCKET_OBJ_KEY"] = "layers/extractor/py_layer.zip"
    os.environ["LAMBDA_FUNCTION_EXTRACT"] = json.dumps(
        ["ExtractorNaLionMsStaging", "ExtractorEuLionMsStaging", "ExtractorAsiLionMsStaging"]
    )
    os.environ["LAMBDA_LAYER_ARCHITECTURES"] = "x86_64"
    os.environ["LAMBDA_LAYER_DESC"] = "Lambda layer that contains: sih-lion py modules."
    os.environ["LAMBDA_LAYER_NAME"] = "ExtractorLayerLionMsStaging-sih-lion-py-layer"
    os.environ["LAMBDA_LAYER_RUNTIMES"] = "python3.9"
    pprint(extractor_layer_lion_ms.lambda_handler({}, {}))


def run_lion_processor_poll_lion_global():
    os.environ["ACCOUNT_OWNER_ID"] = "123456789123"
    os.environ["BUCKET_NAME_SOURCE"] = "noaa-goes16"
    os.environ["BUCKET_NAME_DEST"] = "lion-sat-data"
    os.environ["SAT_DATA_ORG"] = "eumetsat-public"
    os.environ["SOURCE_NAME"] = "goes16"
    os.environ["SAT_DATA_SERVICES"] = json.dumps(
        {"rad": {"folder": "ABI-L1b-RadF", "filter": "M6C02"}, "clm": {"folder": "ABI-L2-ACMF", "filter": None}}
    )
    pprint(processor_poll_lion_global.lambda_handler({}, {}))


def run_lion_processor_archive_lion_global():
    os.environ["ACCOUNT_OWNER_ID"] = "123456789123"
    os.environ["BUCKET_NAME_DEST"] = "lion-sat-data"
    os.environ["BUCKET_NAME_SOURCE"] = "sihlion-eu-west-2"
    os.environ["SOURCE_NAME"] = "msg0deg"
    pprint(processor_archive_lion_global.lambda_handler({}, {}))


def run_elasticache_redis_auto_start():
    os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "EcRedisAutoStartDogGwPerform"
    os.environ["EC_REP_GROUP_FINAL_SNAPSHOT_ID"] = "dog-gw-perform-ec-redis-rep-final-snapshot"
    os.environ["EC_REP_GROUP_KWARGS"] = json.dumps(
        {
            "replication_group_description": "ElastiCache Redis cluster replication group for Dog Gateway PERFORM.",
            "at_rest_encryption_enabled": "true",
            "auth_token": "empty",
            "automatic_failover_enabled": "true",
            "auto_minor_version_upgrade": "true",
            "cache_node_type": "cache.t3.medium",
            "cache_parameter_group_name": "default.redis6.x",
            "cache_subnet_group_name": "dog-gw-perform-ec-redis-sub",
            "engine": "Redis",
            "engine_version": "6.x",
            "kms_key_id": "b5fee42d-d60f-4975-b84b-d2af196c96bb",
            "log_delivery_configurations": '[{"destination_details": {"cloud_watch_logs_details": {"log_group": "/aws/elasticache/dog-gw-perform/slow-log"}}, "destination_type": "cloudwatch-logs", "log_format": "json", "log_type": "slow-log"}, {"destination_details": {"cloud_watch_logs_details": {"log_group": "/aws/elasticache/dog-gw-perform/engine-log"}}, "destination_type": "cloudwatch-logs", "log_format": "json", "log_type": "engine-log"}]',
            "multi_az_enabled": "false",
            "notification_topic_arn": "arn:aws:sns:eu-west-2:123456789123:DogCachePerform_ec-redis-sns",
            "num_node_groups": "1",
            "port": "6379",
            "preferred_cache_cluster_a_zs": '["eu-west-2a", "eu-west-2b"]',
            "preferred_maintenance_window": "mon:02:00-mon:04:00",
            "replicas_per_node_group": "1",
            "replication_group_id": "dog-gw-perform-ec-redis-rep",
            "security_group_ids": '["sg-096deeef419426ee0"]',
            "snapshot_window": "00:00-01:00",
            "transit_encryption_enabled": "true",
        }
    )
    os.environ["REDIS_PW_KEY"] = "auth_token"
    os.environ[
        "REDIS_PW_SECRET"
    ] = "arn:aws:secretsmanager:eu-west-2:123456789123:secret:DogCachePerform/ec-redis-auth-YrkTxz"
    os.environ[
        "SNS_TOPIC"
    ] = "arn:aws:sns:eu-west-2:123456789123:DogCachePerform_SubsEcRedisAutoStartDogGwPerform"
    os.environ["TAG_KEY"] = "auto-start"
    os.environ["TAG_VALUES"] = "1"
    os.environ[
        "WEBHOOK_URL"
    ] = "https://foobarcouk.webhook.office.com/webhookb2/c1e5b2c2-0321-42aa-bd97-d850530acf11@f670d7ce-50c9-4c74-b98f-49edf7903c82/IncomingWebhook/9ecb464ef52a40f698f83efcae010750/71b0843c-25cd-4d4a-be5c-b5e954aad055"
    pprint(elasticache_redis_auto_start.lambda_handler({}, {}))


def run_elasticache_redis_auto_stop():
    os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "EcRedisAutoStopDogGwPerform"
    os.environ[
        "EC_REP_GROUP_ARN"
    ] = "arn:aws:elasticache:eu-west-2:123456789123:replicationgroup:dog-gw-perform-ec-redis-rep"
    os.environ["EC_REP_GROUP_FINAL_SNAPSHOT_ID"] = "dog-gw-perform-ec-redis-rep-final-snapshot"
    os.environ["EC_REP_GROUP_ID"] = "dog-gw-perform-ec-redis-rep"
    os.environ[
        "SNS_TOPIC"
    ] = "arn:aws:sns:eu-west-2:123456789123:DogCachePerform_SubsEcRedisAutoStopDogGwPerform"
    os.environ["TAG_KEY"] = "auto-stop"
    os.environ["TAG_VALUES"] = "-1"  # Not set to either [0,1] to avoid accidental turn-offs of running EC2 instances
    os.environ[
        "WEBHOOK_URL"
    ] = "https://foobarcouk.webhook.office.com/webhookb2/c1e5b2c2-0321-42aa-bd97-d850530acf11@f670d7ce-50c9-4c74-b98f-49edf7903c82/IncomingWebhook/9ecb464ef52a40f698f83efcae010750/71b0843c-25cd-4d4a-be5c-b5e954aad055"
    pprint(elasticache_redis_auto_stop.lambda_handler({}, {}))


def run_mysql_init():
    os.environ["ACCOUNT_OWNER_ID"] = "123456789123"
    os.environ[
        "ADMIN_SECRET"
    ] = "arn:aws:secretsmanager:eu-west-2:123456789123:secret:DogDatabaseDevStaging/rds-mysql-admin-a4yKg8"
    os.environ[
        "USER_SECRET"
    ] = "arn:aws:secretsmanager:eu-west-2:123456789123:secret:DogDatabaseDevStaging/rds-mysql-api-user-DfGbas"
    os.environ["DB_PORT"] = "9001"
    os.environ["DB_SCHEMAS"] = "dog_gw_dev,dog_gw_staging"
    os.environ["HOST"] = "127.0.0.1"
    # os.environ["USER"] = "root"  # Specify the user to log in to the database server with
    # os.environ["PW"] = "password"  # Specify the password to log in to the database server with
    os.environ["PROJECT_NAME"] = "doggw"
    pprint(
        mysql_init.lambda_handler(
            {"ACTION": "INCREMENT", "DB_SCHEMAS": "dog_gw_perform", "SQL_FILENAME": "dog_create_tables.sql"},
            {},
        )
    )


def run_metoffice_storage_archive():
    os.environ["ACCOUNT_OWNER_ID"] = "123456789124"
    os.environ["BUCKET_NAME_SOURCE"] = "foobar-metofficestorage-eu-west-2"
    os.environ["BUCKET_NAME_DEST_PREFIX"] = "archive-test"
    os.environ["STATE_PARAMETER"] = "/CdkMetofficeStorageStack/test"
    os.environ["ARCHIVE_BYTE_COUNT"] = "archive-byte-count"
    pprint(metoffice_storage_archive.lambda_handler({}, {}))


def run_metoffice_storage_monitor():
    os.environ["BUCKET_NAME"] = "foobar-metofficestorage-eu-west-2"
    os.environ["DAILY_THRESHOLD"] = "1"
    os.environ["IAM_USER"] = "metoffice"
    os.environ["IAM_USER_GROUP"] = "Metoffice_Storage"
    os.environ["STATE_PARAMETER"] = "/CdkMetofficeStorageStack/test"
    os.environ["ARCHIVE_BYTE_COUNT"] = "archive-byte-count"
    pprint(metoffice_storage_monitor.lambda_handler({}, {}))


def run_rds_instance_auto_start():
    os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "RDSInstanceAutoStart"
    os.environ["SNS_TOPIC"] = "arn:aws:sns:eu-west-2:123456789123:LambdaRdsInstanceAuto_RDSInstanceAutoStart"
    os.environ["TAG_KEY"] = "auto-start"
    os.environ["TAG_VALUES"] = "1"
    os.environ[
        "WEBHOOK_URL"
    ] = "https://foobarcouk.webhook.office.com/webhookb2/c1e5b2c2-0321-42aa-bd97-d850530acf11@f670d7ce-50c9-4c74-b98f-49edf7903c82/IncomingWebhook/41133474ca4d4f5e8cb91f75759deb5c/71b0843c-25cd-4d4a-be5c-b5e954aad055"
    # os.environ["SEP"] = ","  # Optional
    pprint(rds_start.lambda_handler({}, {}))


def run_rds_instance_auto_stop():
    os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "RDSInstanceAutoStop"
    os.environ["SNS_TOPIC"] = "arn:aws:sns:eu-west-2:123456789123:LambdaRdsInstanceAuto_RDSInstanceAutoStop"
    os.environ["TAG_KEY"] = "auto-stop"
    os.environ["TAG_VALUES"] = "-1"  # Not set to either [0,1] to avoid accidental turn-offs of RDS database instances
    os.environ[
        "WEBHOOK_URL"
    ] = "https://foobarcouk.webhook.office.com/webhookb2/c1e5b2c2-0321-42aa-bd97-d850530acf11@f670d7ce-50c9-4c74-b98f-49edf7903c82/IncomingWebhook/41133474ca4d4f5e8cb91f75759deb5c/71b0843c-25cd-4d4a-be5c-b5e954aad055"
    # os.environ["SEP"] = ","  # Optional
    pprint(rds_stop.lambda_handler({}, {}))


def run_s3_handler_dog_gw(event_obj: dict):
    os.environ["ACCOUNT_OWNER_ID"] = "123456789123"
    os.environ["CHECKSUM_ALGORITHM"] = "SHA256"
    os.environ["KMS_MASTER_KEY_ID"] = "arn:aws:kms:eu-west-2:123456789123:key/886dd52e-2caa-4f2b-b362-4b050f6a9571"
    os.environ["TAGS"] = json.dumps(
        [
            {"Key": "company", "Value": "foobar"},
            {"Key": "project-name", "Value": "Dog"},
            {"Key": "custom", "Value": "None"},
            {"Key": "env-type", "Value": "INTERNAL"},
            {"Key": "component", "Value": "Gateway"},
            {"Key": "deploy-env", "Value": "dev"},
        ]
    )
    event_obj["bucket_name"] = "doggwdev-eu-west-2-inst-yyy"
    pprint(s3_handler_dog_gw.lambda_handler(event_obj, {}))


def run_ses_handler_mail_ms(event_obj: dict):
    os.environ["SES_CONFIG_SET_MAPPING"] = json.dumps(
        {
            "no-reply@bkg.sihgnpwtbf.com": "mail-ms-bkg-sihgnpwtbf-com-domain-config-set",
            "no-reply@dog.com": "mail-ms-dog-com-domain-config-set",
            "no-reply@cat.com": "mail-ms-cat-com-domain-config-set",
            "no-reply@bird.com": "mail-ms-bird-com-domain-config-set",
            "no-reply@fish.com": "mail-ms-fish-com-domain-config-set",
        }
    )
    os.environ["SES_EMAIL_IDENTITY_ARN_MAPPING"] = json.dumps(
        {
            "no-reply@bkg.sihgnpwtbf.com": "arn:aws:ses:eu-west-2:123456789123:identity/no-reply@bkg.sihgnpwtbf.com",
            "no-reply@dog.com": "arn:aws:ses:eu-west-2:123456789123:identity/no-reply@dog.com",
            "no-reply@cat.com": "arn:aws:ses:eu-west-2:123456789123:identity/no-reply@cat.com",
            "no-reply@bird.com": "arn:aws:ses:eu-west-2:123456789123:identity/no-reply@bird.com",
            "no-reply@fish.com": "arn:aws:ses:eu-west-2:123456789123:identity/no-reply@fish.com",
        }
    )
    os.environ["SES_EMAIL_TEMPLATE_ARN_FORMAT"] = "arn:aws:ses:eu-west-2:123456789123:template/*"
    os.environ["SES_EMAIL_TEMPLATE_MAPPING"] = json.dumps(
        {
            "bkg": [],
            "dog": [
                "dog_treatment_success",
                "dog_treatment_cancelled",
                "dog_patient_photo",
                "dog_100_pdt_dose",
                "dog_weather_change",
                "dog_100_sunburn_risk",
                "dog_70_sunburn_risk",
            ],
            "cat": [],
            "bird": [],
            "fish": [],
        }
    )
    os.environ["SES_EMAIL_TEMPLATE_NAME_PREFIX"] = "mail-ms-staging_"
    pprint(ses_handler_mail_ms.lambda_handler(event_obj, {}))


def run_sns_ms_teams_notification(event_obj: dict):
    os.environ[
        "WEBHOOK_URL"
    ] = "https://foobarcouk.webhook.office.com/webhookb2/c1e5b2c2-0321-42aa-bd97-d850530acf11@f670d7ce-50c9-4c74-b98f-49edf7903c82/IncomingWebhook/906fbec59286455ebec07d8b5c81793c/71b0843c-25cd-4d4a-be5c-b5e954aad055"
    pprint(sns_ms_teams.lambda_handler(event_obj, {}))


def run_update_cloudfront_web_acl():
    os.environ["TAGS"] = json.dumps(
        [
            {"Key": "company", "Value": "foobar"},
            {"Key": "component", "Value": "CloudFront"},
            {"Key": "custom", "Value": "None"},
            {"Key": "deploy-env", "Value": "staging"},
            {"Key": "env-type", "Value": "INTERNAL"},
            {"Key": "project-name", "Value": "Cat"},
        ]
    )
    event_obj = {
        "arn": "arn:aws:wafv2:us-east-1:123456789123:global/webacl/DogCloudFrontDev_cloudfront-waf/8343ae110-850b-4ddb-bdd6-0124722b818f"
    }
    pprint(cloudfront_web_acl_update_tags.lambda_handler(event_obj, {}))


def run_version_meta():
    for _, i in enumerate(["dev", "staging", "perform", "prod", "sihp", "sihd"]):
        os.environ["DEPLOY_ENV"] = i
        os.environ["DEPLOY_TAG"] = json.dumps(bool(not bool(i in {"dev", "staging", "perform"})))
        os.environ["VERSION_META_PARAMETER"] = "/cat-gw/version/meta"
        event_obj = {
            "build-no": "12",
            "commit-id": "f10f71f91d3b44f33370b2b945261b64e6dcbefe",
            "commit-msg": "Merged in dev (pull request #124) [Patch] added relativ eendtimes to sunscreens ep Approved-by: David Arnold",
        }
        # pprint(version_meta.lambda_handler(event_obj, {}))
        pprint({i: version_meta.lambda_handler(event_obj, {})})


def run_weatherapi_storage_download():
    os.environ["ACCOUNT_OWNER_ID"] = "123456789124"
    os.environ["BUCKET_NAME_DEST_PREFIX"] = "archive-test"
    os.environ[
        "WEATHERAPI_KEY_SECRET"
    ] = "arn:aws:secretsmanager:eu-west-2:123456789124:secret:weatherapi-storage/lambda-function-secret-mee7QH"
    pprint(weatherapi_storage_download.lambda_handler({}, {}))


def main():
    os.environ["AWS_REGION"] = "eu-west-2"
    # for i in [amplify_event_started, amplify_event_started_pr, amplify_event_succeeded]:
    #     run_amplify_ms_teams_notification(i)
    # for i in [
    #     cloudwatch_bounce,
    #     cloudwatch_complaint,
    #     cloudwatch_ec2_auto,
    #     cloudwatch_ec_redis_auto,
    #     cloudwatch_rds_auto,
    #     cloudwatch_ecs_errors_spdt_stag,
    #     cloudwatch_ecs_errors_spdt_sihd,
    #     cloudwatch_ecs_errors_s4h_stag,
    # ]:
    #     run_cloudwatch_ms_teams_notification(i)
    # for i in [
    #     codepipeline_event_started,
    #     codepipeline_event_succeeded,
    #     codepipeline_event_failed,
    # ]:
    #     run_codepipeline_ms_teams_notification(i)
    # run_ec2_instance_auto_start()
    # run_ec2_instance_auto_stop()
    # run_lion_extractor_layer_lion_ms()
    # run_lion_processor_poll_lion_global()
    # run_lion_processor_archive_lion_global()
    # run_elasticache_redis_auto_start()
    # run_elasticache_redis_auto_stop()
    # run_mysql_init()
    # run_metoffice_storage_archive()
    # run_metoffice_storage_monitor()
    # run_rds_instance_auto_start()
    # run_rds_instance_auto_stop()
    # run_s3_handler_dog_gw({"task": "create_s3_bucket"})
    # run_s3_handler_dog_gw({"task": "delete_s3_bucket"})
    # run_ses_handler_mail_ms(
    #     {
    #         "from_email_address": "no-reply@dog.com",
    #         "to_email_addresses": ["david.arnold@foobar.co.uk"],
    #         "reply_to_addresses": ["support@dog.com"],
    #         "template_name": "dog_patient_photo",
    #         "template_data": {
    #             "inst_name": "1",
    #             "patient_username": "dav532",
    #             "portal_url": "hi.com",
    #             "support_mail_username": "support@dog.com",
    #         },
    #     }
    # )
    # for i in [
    #     sns_verify,
    #     sns_send,
    #     sns_rendering_failure,
    #     sns_reject,
    #     sns_delivery,
    #     sns_bounce,
    #     sns_complaint,
    #     sns_delivery_delay,
    #     sns_subscription,
    # ]:
    #     run_sns_ms_teams_notification(i)
    # run_update_cloudfront_web_acl()
    # run_version_meta()
    # run_weatherapi_storage_download()


if __name__ == "__main__":
    main()
