import json
import logging
import os
import shutil
import sys
import tempfile

if "LAMBDA_TASK_ROOT" in os.environ:
    sys.path.insert(0, os.environ["LAMBDA_TASK_ROOT"])

# pylint: disable=wrong-import-position
import boto3
import pandas as pd
import requests

logger = logging.getLogger()
# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)  # Enable to log to stdout, and comment line below.
logger.setLevel(logging.INFO)
logger.info(f"Boto3 version: {boto3.__version__}")

# boto3.setup_default_session(profile_name="innovation")  # Enable to use alt AWS profile

ACCOUNT_OWNER_ID = "ACCOUNT_OWNER_ID"
BUCKET_NAME_DEST_PREFIX = "BUCKET_NAME_DEST_PREFIX"
WEATHERAPI_KEY_SECRET = "WEATHERAPI_KEY_SECRET"

WEATHERAPI_KEY = "WEATHERAPI_KEY"
TMP_FOP = "TMP_FOP"

PROJECT_NAME = "weather-validation-downloader"
PROJECT_BRANCH = "main"

BASEURL = "http://api.weatherapi.com/v1"
RAD_STATIONS_FN = "all_stations_active_2022-09-14.csv"
CITIES_FN = "cities_selection.csv"
NCEI_STATIONS_FN = "ncei_station_validation_selection_2022-06-10.csv"
COMBINED_VALIDATION_LOCATIONS_FN = "wapi_vldation_all_locs.csv"

ORGANISATION = "WeatherAPI"
DATASET = "Forecast"


def meta_to_fn(meta):
    """
    Filename from metadata
    """
    fn = (
        f'{meta["dt"].strftime("%Y-%m-%d-%H%M")}_{meta["loc_name"]}_'
        f'{meta["lat"]:.2f}_{meta["lon"]:.2f}_{meta["country"]}.json'
    )
    return fn


def s3_download_fileobj(s3, bucket_name: str, obj_key: str, filename: str = None) -> str:
    if filename is None:
        filename = os.path.join(os.environ[TMP_FOP], bucket_name, obj_key)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "wb") as f:
        s3.download_fileobj(bucket_name, obj_key, f)
    return filename


def weather_api_request(lat, lon):
    """
    WeatherAPI request. Throw errors for various kinds of location problems
    """

    out = requests.get(
        BASEURL + "/forecast.json",
        params={"key": os.environ[WEATHERAPI_KEY], "q": f"{lat},{lon}", "days": 14, "aqi": "yes", "alerts": "yes"},
        timeout=60,
    )
    # raise 4xx type errors
    out.raise_for_status()
    out = out.json()

    if "error" in out:
        raise RuntimeError(f'WeatherAPI request for {lat},{lon} returned error {out["error"]}')

    retlat, retlon = out["location"]["lat"], out["location"]["lon"]
    if abs(retlat - lat) > 0.1:
        raise RuntimeError(f"WeatherAPI request for {lat},{lon} returned wrong location {retlat}, {retlon}")
    if abs(retlon - lon) > 0.1:
        raise RuntimeError(f"WeatherAPI request for {lat},{lon} returned wrong location {retlat}, {retlon}")

    return out


def lambda_handler(event, context):
    env_keys = {
        ACCOUNT_OWNER_ID,
        BUCKET_NAME_DEST_PREFIX,
        WEATHERAPI_KEY_SECRET,
    }
    if not all(k in os.environ for k in env_keys):
        logger.error(f"## One or more of {env_keys} is not set in ENVIRONMENT VARIABLES: {os.environ}")
        sys.exit(1)

    secretsmanager = boto3.client("secretsmanager", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to Secrets Manager via client")
    secretsmanager_res = secretsmanager.get_secret_value(SecretId=os.environ[WEATHERAPI_KEY_SECRET])
    secretsmanager_res_obs = {k: v if k != "SecretString" else "****" for k, v in dict(secretsmanager_res).items()}
    logger.info(f"## Secrets Manager Get Secret Value response: {secretsmanager_res_obs}")

    os.environ[WEATHERAPI_KEY] = json.loads(secretsmanager_res["SecretString"])[WEATHERAPI_KEY]

    s3 = boto3.client("s3", region_name=os.environ["AWS_REGION"])
    logger.info("## Connected to S3 via client")

    os.environ[TMP_FOP] = tempfile.gettempdir()
    logger.info(f"Temporary folder path: {os.environ[TMP_FOP]}")

    s3_bucket_name_static_prefix = PROJECT_NAME.replace("-", "").lower()
    s3_bucket_name_static = f"{s3_bucket_name_static_prefix}-{os.environ['AWS_REGION']}"[:63]
    s3_obj_key_static_prefix = f"{PROJECT_BRANCH}/weatherapi/static"

    now = pd.Timestamp.utcnow()

    logger.info("## Get the validation locations")
    locs = pd.read_csv(
        s3_download_fileobj(
            s3,
            bucket_name=s3_bucket_name_static,
            obj_key=f"{s3_obj_key_static_prefix}/{COMBINED_VALIDATION_LOCATIONS_FN}",
        ),
        index_col=0,
        comment="#",
    )

    with tempfile.TemporaryDirectory() as tmpdir_meta:
        logger.info(f"Temporary folder (meta) path: {tmpdir_meta}")

        logger.info("## Validation collection start")
        for i, (loc_name, loc) in enumerate(locs.iterrows()):
            logger.info(f"## Request {i + 1}/{len(locs)}")
            try:
                out = weather_api_request(loc.iloc[0], loc.iloc[1])
                # remove spaces from country
                try:
                    country = "".join(out["location"]["country"].split(" "))
                except KeyError:
                    country = "Unknown"
                now = pd.Timestamp.utcnow()  # timestamp of request, used for file name
                meta = {"dt": now, "loc_name": loc_name, "lat": loc.iloc[0], "lon": loc.iloc[1], "country": country}
                with open(os.path.join(tmpdir_meta, meta_to_fn(meta)), "w", encoding="utf-8") as f:
                    json.dump(out, f)
            except Exception as e:
                logger.info(f"## Exception: {e}")
        logger.info("## Validation collection end")

        with tempfile.TemporaryDirectory() as tmpdir_zip:
            logger.info(f"Temporary folder (zip) path: {tmpdir_zip}")

            logger.info("## Now zip up collection and send to S3")
            zip_fn = f'{now.strftime("%Y-%m-%d-%H")}.zip'
            shutil.make_archive(os.path.join(tmpdir_zip, os.path.splitext(zip_fn)[0]), "zip", tmpdir_meta)

            logger.info("## Upload zip to S3")
            s3_bucket_name_dest = f'{os.environ[BUCKET_NAME_DEST_PREFIX]}-{pd.Timestamp.utcnow().strftime("%Y")}'
            s3_obj_key_dest = f"{ORGANISATION}/{DATASET}/{zip_fn}"
            logger.info(f"## Creating a new S3 object: s3://{s3_bucket_name_dest}/{s3_obj_key_dest}")
            s3.upload_file(
                os.path.join(tmpdir_zip, zip_fn),
                s3_bucket_name_dest,
                s3_obj_key_dest,
                ExtraArgs={"ExpectedBucketOwner": os.environ[ACCOUNT_OWNER_ID], "StorageClass": "INTELLIGENT_TIERING"},
            )
            logger.info("## Upload complete")
