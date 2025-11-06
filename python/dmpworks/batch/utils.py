import json
import logging
import pathlib
import subprocess
import time
import urllib.request
from typing import Optional
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

from dmpworks.utils import run_process

log = logging.getLogger(__name__)
TOKEN_URL = "http://169.254.169.254/latest/api/token"
IDENTITY_URL = "http://169.254.169.254/latest/dynamic/instance-identity/document"


def s3_uri(bucket_name: str, dataset: str, task_id: str, stage: str) -> str:
    return f"s3://{bucket_name}/{dataset}/{task_id}/{stage}/"


def local_path(dataset: str, task_id: str, stage: str) -> pathlib.Path:
    return pathlib.Path("/") / "data" / dataset / task_id / stage


def clean_s3_prefix(s3_uri: str):
    log.info(f"Checking and cleaning S3 URI: {s3_uri}")
    if s3_uri_has_files(s3_uri):
        log.info(f"Objects found at {s3_uri}, deleting...")
        run_process(["s5cmd", "rm", f"{s3_uri}*"])
    else:
        log.info(f"No objects found at {s3_uri}")


def upload_to_s3(local_dir: pathlib.Path, s3_uri: str, glob_pattern: str = "*"):
    log.info(f"Uploading from {local_dir}/{glob_pattern} to {s3_uri}")
    run_process(["s5cmd", "cp", f"{local_dir}/{glob_pattern}", s3_uri])


def download_from_s3(source_uri: str, target_dir: pathlib.Path):
    log.info(f"Downloading from {source_uri} to {target_dir}")
    run_process(["s5cmd", "cp", source_uri, f"{target_dir}/"])


def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError(f"Invalid S3 URI: {s3_uri}")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    return bucket, prefix


def s3_uri_has_files(
    s3_uri: str,
    *,
    s3_client: Optional[boto3.client] = None,
) -> bool:
    if s3_client is None:
        s3_client = boto3.client("s3")

    bucket, prefix = parse_s3_uri(s3_uri)

    try:
        resp = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1,
        )
    except ClientError as err:
        raise RuntimeError(f"Unable to list {s3_uri}: {err}")

    return "Contents" in resp


def get_instance_outgoing_ip():
    result = subprocess.check_output(["curl", "-s", "https://checkip.amazonaws.com"])
    return result.decode().strip()


def associate_elastic_ip(
    *,
    instance_id: str,
    allocation_id: str,
    region_name: str,
    max_wait_time_seconds: int = 300,
):
    ec2_client = boto3.client("ec2", region_name=region_name)
    try:
        current_ip = get_instance_outgoing_ip()
        log.info(f"Current IP is {current_ip}")
        response = ec2_client.associate_address(
            AllocationId=allocation_id,
            InstanceId=instance_id,
            AllowReassociation=True,
        )
        log.info(f"Successfully associated Elastic IP {allocation_id} with instance {instance_id}.")
        log.info(f"Association ID: {response.get('AssociationId')}")
    except Exception as e:
        msg = f"Error associating Elastic IP: {e}"
        log.error(msg)
        raise Exception(msg)

    log.info(f"Waiting for IP {current_ip} to change...")
    start_time = time.time()
    while True:
        elapsed = time.time() - start_time
        if elapsed > max_wait_time_seconds:
            msg = f"IP did not change within {max_wait_time_seconds} seconds."
            log.error(msg)
            raise TimeoutError(msg)

        new_ip = get_instance_outgoing_ip()
        if new_ip != current_ip:
            current_ip = new_ip
            log.info(f"Elastic IP {current_ip} is being used for outgoing traffic.")
            break

        log.info(f"Current IP is still {current_ip}. Waiting...")
        time.sleep(5)


def get_ec2_instance_info(token_ttl_seconds: int = 300):
    try:
        # Get IMDSv2 token
        req_token = urllib.request.Request(
            TOKEN_URL,
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": str(token_ttl_seconds)},
        )
        with urllib.request.urlopen(req_token) as token_response:
            token = token_response.read().decode()

        # Get instance identity document (includes region and instanceId)
        req_identity = urllib.request.Request(
            IDENTITY_URL,
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(req_identity) as identity_response:
            identity = json.loads(identity_response.read().decode())
            instance_id = identity["instanceId"]
            region = identity["region"]

        log.info(f"Found instance ID: {instance_id}, region: {region}")
        return instance_id, region

    except Exception as e:
        msg = f"Error retrieving instance metadata: {e}"
        log.error(msg)
        raise Exception(msg)
