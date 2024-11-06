import json
import logging
import os
import sys

import urllib3

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3
import pymysql
from botocore.exceptions import ClientError

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

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

http = urllib3.PoolManager()


def check_event_key(event, key):
    if key in event and bool(event[key]):
        logger.info(f"## {key}: {event[key]}")
    else:
        error_msg: str = f"## No {key} in EVENT: {event}"
        logger.error(error_msg)
        return error_res(error_msg)
    return None


def error_res(msg):
    return {"Error": msg}


def execute_mysql_command(cur, description, command):
    cur.execute(command)
    logger.info(f"## SQL {description} command: {command}")


def execute_mysql_create_db_schema_command(cur, db_schema):
    execute_mysql_command(
        cur,
        "Create DB schema",
        f"CREATE DATABASE `{db_schema}` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;",
    )


def execute_mysql_flush_privileges_command(cur):
    execute_mysql_command(cur, "Flush Privileges", "FLUSH PRIVILEGES;")


def execute_mysql_commit(mysql_cnx):
    mysql_cnx.commit()
    logger.info("## SQL commit")


def get_branch(deploy_env):
    preview_demo: str = "PREVIEW_DEMO"
    is_demo: bool = False
    if os.getenv(preview_demo):
        preview_demo_meta: dict[str, str] = json.loads(os.environ[preview_demo])
        is_demo = bool(deploy_env in preview_demo_meta and preview_demo_meta[deploy_env] == "demo")
    return "prod" if deploy_env == "prod" or is_demo else ("main" if deploy_env != "dev" else "dev")


def get_mysql_commands(sql_file):
    sql_commands: list[str] = [line.strip() for line in sql_file.split("\n") if line and line[:2] != "--"]
    i: int = 0
    while i < len(sql_commands):
        if sql_commands[i].endswith(";"):
            i += 1
        elif i + 1 < len(sql_commands):
            sql_commands[i] = " ".join([sql_commands[i], sql_commands[i + 1]])
            sql_commands.pop(i + 1)
        else:
            continue
    logger.info(f"## SQL command count: {len(sql_commands)}")
    return sql_commands


def get_s3_object(s3_client, bucket_name, obj_key) -> str:
    logger.debug(f"## S3 Bucket Name: {bucket_name}")
    try:
        s3_res = s3_client.head_object(
            Bucket=bucket_name,
            Key=obj_key,
            ExpectedBucketOwner=os.environ["ACCOUNT_OWNER_ID"],
            # ChecksumMode="ENABLED",
        )
        logger.info(f"## S3 Head Object response: {s3_res}")
    except ClientError as ex:
        logger.error(f"## S3 Head Object ERROR '{bucket_name}/{obj_key}': {ex}")
        return None
    s3_obj_etag = s3_res["ETag"].strip('"')
    logger.info(f"## S3 Object ETag: {s3_obj_etag}")
    try:
        s3_res = s3_client.get_object(
            Bucket=bucket_name,
            IfMatch=s3_obj_etag,
            Key=obj_key,
            ExpectedBucketOwner=os.environ["ACCOUNT_OWNER_ID"],
            # ChecksumMode="ENABLED",
        )
        logger.info(f"## S3 Get Object response: {s3_res}")
        s3_obj_body = s3_res["Body"].read()
        if isinstance(s3_obj_body, bytes):
            s3_obj_body = s3_obj_body.decode(encoding="utf-8")
        logger.info(f"## S3 Object Body: {s3_obj_body}")
        return s3_obj_body
    except ClientError as ex:
        logger.error(f"## S3 Get Object ERROR '{bucket_name}/{obj_key}': {ex}")
    return None


# Define function to retrieve values from extension local HTTP server cache
def retrieve_extension_value(url: str) -> dict:
    url = f"http://localhost:{os.environ[PARAMETERS_SECRETS_EXTENSION_HTTP_PORT]}{url}"
    res = http.request("GET", url, headers={"X-Aws-Parameters-Secrets-Token": os.environ[AWS_SESSION_TOKEN]})
    return json.loads(res.data)


# Load Secrets Manager values from extension
def retrieve_extension_value_secret(secret_id: str) -> str:
    logger.info(f"## Loading AWS Secrets Manager value: {secret_id}")
    secretsmanager_res = retrieve_extension_value(f"/secretsmanager/get?secretId={secret_id}")
    secretsmanager_res_obs = {k: v if k != "SecretString" else "****" for k, v in dict(secretsmanager_res).items()}
    logger.info(f"## Secrets Manager Get Secret Value (extension) response: {secretsmanager_res_obs}")
    return secretsmanager_res["SecretString"]


def lambda_handler(event, context):
    env_keys = {
        "ACCOUNT_OWNER_ID",
        "ADMIN_SECRET",
        "DB_PORT",
        "DB_SCHEMAS",
        "PROJECT_NAME",
        "USER_SECRET",
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

    project_name: str = "PROJECT_NAME"
    s3_bucket_name_prefix: str = (
        event[project_name].replace("-", "").lower() if project_name in event else os.environ[project_name]
    )
    s3_bucket_name = f"{s3_bucket_name_prefix}-{os.environ['AWS_REGION']}"[:63]

    user_secret = json.loads(retrieve_extension_value_secret(secret_id=os.environ["USER_SECRET"]))

    action_event_key: str = "ACTION"
    db_schema_event_key: str = "DB_SCHEMAS"
    sql_filename_event_key: str = "SQL_FILENAME"

    # Check event keys exist
    for event_key in [action_event_key, db_schema_event_key, sql_filename_event_key]:
        event_key_exists = check_event_key(event, event_key)
        if event_key_exists is not None:
            return event_key_exists

    increment_action: str = "INCREMENT"
    reset_action: str = "RESET"

    # Check ACTION event key is valid
    valid_actions: list[str] = [increment_action, reset_action]
    if event[action_event_key] in valid_actions:
        action: str = event[action_event_key]
    else:
        error_msg: str = f"## Invalid {action_event_key} in EVENT, valid options include: {valid_actions}"
        logger.error(error_msg)
        return error_res(error_msg)

    # Check DB_SCHEMA event key specifies only valid DB schemas, and populate list of DB schemas to modify
    valid_db_schemas: list[str] = os.environ["DB_SCHEMAS"].split(",")
    db_schemas_to_modify: list[str] = []
    for i in str(event[db_schema_event_key]).split(","):
        if i in valid_db_schemas:
            db_schemas_to_modify.append(i)
        else:
            error_msg: str = (
                f"## Invalid {db_schema_event_key} '{i}' in EVENT, valid options include: {valid_db_schemas}"
            )
            logger.error(error_msg)
            return error_res(error_msg)

    sql_filename: str = event[sql_filename_event_key]

    secretsmanager = boto3.client("secretsmanager", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to Secrets Manager via client")
    secretsmanager_res = secretsmanager.get_secret_value(SecretId=os.environ["ADMIN_SECRET"])
    secretsmanager_res_obs = {k: v if k != "SecretString" else "****" for k, v in dict(secretsmanager_res).items()}
    logger.info(f"## Secrets Manager Get Secret Value response: {secretsmanager_res_obs}")
    admin_secret = json.loads(secretsmanager_res["SecretString"])
    try:
        mysql_cnx = pymysql.connect(
            user=admin_secret["username"],
            password=admin_secret["password"],
            host=admin_secret["host"],
            port=int(os.environ["DB_PORT"]),
        )
        logger.info("## Connection to RDS MySQL instance succeeded.")
    except pymysql.MySQLError as e:
        logger.error(f"## Unexpected error: Could not connect to MySQL instance: {e}")
        sys.exit(1)

    with mysql_cnx.cursor() as cur:
        if action == reset_action:
            # Drop API user
            try:
                execute_mysql_command(cur, "Drop API User", f"DROP USER '{user_secret['username']}'@'%';")
            except Exception as ex:
                logger.info(f"## Exception - API user cannot be dropped, it likely does not exist: {ex}")
            execute_mysql_flush_privileges_command(cur)

        # Create API user
        try:
            execute_mysql_command(
                cur,
                "Create API User",
                f"CREATE USER '{user_secret['username']}'@'%' IDENTIFIED BY '{user_secret['password']}';",
            )
        except Exception as ex:
            logger.info(f"## Exception - API user cannot be created, it likely already exists: {ex}")
        execute_mysql_commit(mysql_cnx)

        error_msgs: list[str] = []

        s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
        logger.info("## Connected to S3 via client")

        for db_schema in valid_db_schemas:
            # Add permissions for API user to each valid DB schema
            execute_mysql_command(
                cur,
                "Grant API User Perms",
                f"GRANT SELECT, INSERT, UPDATE, DELETE ON {db_schema}.* TO '{user_secret['username']}'@'%';",
            )
            execute_mysql_flush_privileges_command(cur)
            execute_mysql_commit(mysql_cnx)

            if db_schema in db_schemas_to_modify:
                # Retrieve from S3 bucket, the SQL file to apply to the DB schema
                s3_obj_key = f"{get_branch(db_schema.rsplit(sep='_', maxsplit=1)[-1].lower())}/{sql_filename}"
                sql_file: str = get_s3_object(s3, s3_bucket_name, s3_obj_key)
                if not sql_file:
                    error_msg: str = (
                        f"## Cannot get S3 object '{s3_bucket_name}/{s3_obj_key}' in order to "
                        f"apply '{action}' to DB schema: '{db_schema}'. See 's3-upload' directory "
                        f"in 'aws-scripts' repo for help with uploading a file "
                        f"from a git repo to an S3 bucket."
                    )
                    logger.error(error_msg)
                    error_msgs.append(error_msg)
                    continue
                logger.info(
                    f"## Retrieved S3 object '{s3_bucket_name}/{s3_obj_key}' in order to "
                    f"apply '{action}' to DB schema: '{db_schema}'."
                )

                if action == increment_action:
                    logger.info("## Increment DB schema - just create DB schema if it doesn't already exist")
                    try:
                        execute_mysql_create_db_schema_command(cur, db_schema)
                    except Exception as ex:
                        logger.info(f"## Exception - DB schema cannot be created, it likely already exists: {ex}")
                else:
                    logger.info("## Non-Increment (Reset) DB schema - drop and recreate DB schema")
                    try:
                        logger.info("## Starting DB schema drop")
                        execute_mysql_command(cur, "Drop DB schema", f"DROP DATABASE `{db_schema}`;")
                        logger.info("## Finished DB schema drop")
                    except Exception as ex:
                        logger.info(f"## Exception - DB schema cannot be dropped, it likely does not exist: {ex}")
                    execute_mysql_create_db_schema_command(cur, db_schema)
                execute_mysql_commit(mysql_cnx)

                logger.info("## Specify DB schema to change focus to")
                execute_mysql_command(cur, "Use DB schema", f"USE `{db_schema}`;")

                logger.info("## Run the retrieved SQL file against the DB schema")
                for command in get_mysql_commands(sql_file):
                    execute_mysql_command(cur, f"{db_schema} DB schema", command)
                execute_mysql_commit(mysql_cnx)

    mysql_cnx.close()

    return error_res(error_msgs) if error_msgs else {"status": "OK"}
