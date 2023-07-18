from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import os
from pathlib import Path
from typing import Iterable
from loguru import logger

import boto3
from botocore.client import Config

from tqdm import tqdm
from zarr.storage import NestedDirectoryStore

from .logger import init_logging

init_logging()


@dataclass
class ZarrArchiveFile:
    """
    A file path, size, and md5 checksum, ready to be added to a ZarrChecksumTree.

    This class differs from the `ZarrChecksum` class, for the following reasons:
    * Field order does not matter
    * This class is not serialized in any manner
    * The `path` field is relative to the root of the zarr archive, while the `name` field of
    `ZarrChecksum` is just the final component of said path
    """

    path: Path
    size: int
    digest: str

    __str__ = lambda self: f"{self.path} ({self.size} bytes, md5={self.digest})"


FileGenerator = Iterable[ZarrArchiveFile]


@dataclass
class S3ClientOptions:
    region_name: str = "us-east-1"
    api_version: str | None = None
    use_ssl: bool = True
    verify: bool | None = None
    endpoint_url: str | None = None
    aws_access_key_id: str | None = None
    aws_secret_access_key: str | None = None
    aws_session_token: str | None = None
    config: Config | None = None


def yield_files_s3(
    bucket: str,
    prefix: str = "",
    client_options: S3ClientOptions | None = None,
    *,
    excluded_files: list[str] = [],
    ignore_hidden=False,
) -> FileGenerator:
    if client_options is None:
        client_options = S3ClientOptions()

    # Construct client
    client = boto3.client("s3", **asdict(client_options))
    continuation_token = None
    options = {"Bucket": bucket, "Prefix": prefix}

    if excluded_files:
        logger.info(f"Excluding the following files: {excluded_files}")
    if ignore_hidden:
        logger.info("Ignoring hidden files")

    logger.info(f"Retrieving files from s3://{bucket}/{prefix}")

    # Test that url is fully qualified path by appending slash to prefix and listing objects
    test_resp = client.list_objects_v2(Bucket=bucket, Prefix=os.path.join(prefix, ""))
    if "Contents" not in test_resp:
        logger.warning(
            f"No files found under prefix: {prefix}, please check that you have provided the fully qualified path to the zarr root."
        )
        yield from []
        return

    # Iterate until all files found
    while True:
        if continuation_token is not None:
            options["ContinuationToken"] = continuation_token

        # Fetch
        res = client.list_objects_v2(**options)

        for obj in res.get("Contents", []):
            filename = os.path.basename(Path(obj["Key"]))
            if filename in excluded_files:
                continue
            if ignore_hidden and filename.startswith("."):
                continue
            yield ZarrArchiveFile(
                path=Path(obj["Key"]).relative_to(prefix),
                size=obj["Size"],
                digest=obj["ETag"].strip('"'),
            )

        # If all files fetched, end
        continuation_token = res.get("NextContinuationToken", None)
        if continuation_token is None:
            break


ignored_files = []
included_files = []


def yield_files_local(
    directory: str | Path,
    *,
    excluded_files: list[str] = [],
    ignore_hidden: bool = False,
    show_progress: bool = True,
) -> FileGenerator:
    root_path = Path(os.path.expandvars(directory)).expanduser()
    if not root_path.exists():
        raise Exception("Path does not exist")

    if excluded_files:
        logger.info(f"Excluding the following files: {excluded_files}")
    if ignore_hidden:
        logger.info("Ignoring hidden files")

    logger.info("Discovering files...")
    store = NestedDirectoryStore(root_path)
    file_list = tqdm(list(store.keys())) if show_progress else list(store.keys())
    for file in file_list:
        path = Path(file)
        filename = os.path.basename(path)

        if excluded_files:
            if filename in excluded_files:
                logger.debug(f"Excluding file: {filename} with path '{path}'")
                continue

        if ignore_hidden:
            if filename.startswith("."):
                logger.debug(f"Ignoring file: {filename} with path '{path}'")
                continue

        logger.trace(f"Including file: {filename} with path '{path}'")

        absolute_path = root_path / path
        size = absolute_path.stat().st_size

        # Compute md5sum of file
        md5sum = hashlib.md5()
        with open(absolute_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5sum.update(chunk)
        digest = md5sum.hexdigest()

        zarr_archive_file = ZarrArchiveFile(path=path, size=size, digest=digest)

        logger.trace(f"Finished processing file: {zarr_archive_file}")

        yield zarr_archive_file
