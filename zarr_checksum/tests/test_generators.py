import boto3
from pathlib import Path
import tempfile
import os
from moto import mock_s3

from zarr_checksum.generators import ZarrArchiveFile, yield_files_local, yield_files_s3


def test_yield_files_local(tmp_path):
    # Create file tree like so
    #           . (root)
    #          / \
    #         a  c
    #        /
    #       b
    c = tempfile.mkstemp(dir=tmp_path)[1]

    a = tempfile.mkdtemp(dir=tmp_path)
    b = tempfile.mkstemp(dir=a)[1]

    # Test files yielded
    files = list(yield_files_local(tmp_path))
    assert (
        ZarrArchiveFile(
            path=Path(c).relative_to(tmp_path),
            size=0,
            digest="d41d8cd98f00b204e9800998ecf8427e",
        )
        in files
    )
    assert (
        ZarrArchiveFile(
            path=Path(b).relative_to(tmp_path),
            size=0,
            digest="d41d8cd98f00b204e9800998ecf8427e",
        )
        in files
    )


def test_yield_files_local_exclude_file(tmp_path):
    sub_dir = tempfile.mkdtemp(dir=tmp_path)

    with open(f"{sub_dir}/include_me.txt", "w", encoding="UTF-8") as fp:
        pass

    with open(f"{sub_dir}/exclude_me.txt", "w", encoding="UTF-8") as fp:
        pass

    files = list(yield_files_local(tmp_path, excluded_files=["exclude_me.txt"]))

    assert len(files) == 1
    assert os.path.basename(files[0].path) == "include_me.txt"


def test_yield_files_local_exclude_hidden_files(tmp_path):
    sub_dir = tempfile.mkdtemp(dir=tmp_path)

    with open(f"{sub_dir}/.hidden", "w", encoding="UTF-8") as fp:
        pass

    with open(f"{sub_dir}/not_hidden.txt", "w", encoding="UTF-8") as fp:
        pass

    files = list(yield_files_local(tmp_path, ignore_hidden=True))

    assert len(files) == 1
    assert os.path.basename(files[0].path) == "not_hidden.txt"


def test_yield_files_local_no_empty_dirs(tmp_path):
    """Ensure no empty directories are yielded."""
    # Create a nested file
    filename = tempfile.mkstemp(dir=tempfile.mkdtemp(dir=tmp_path))[1]

    # Create a bunch of empty directories
    tempfile.mkdtemp(dir=tmp_path)
    tempfile.mkdtemp(dir=tmp_path)

    # Create a nest of empty directories
    tempfile.mkdtemp(dir=tempfile.mkdtemp(dir=tempfile.mkdtemp(dir=tmp_path)))

    files = list(yield_files_local(tmp_path))
    assert len(files) == 1
    assert files[0].path == Path(filename).relative_to(tmp_path)


# TODO: Add tests for yield_files_s3
@mock_s3
def test_yield_files_s3():
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="mybucket")

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(Bucket="mybucket", Key="root/c", Body="c file")
    s3.put_object(Bucket="mybucket", Key="root/a/b", Body="b file")

    files = list(yield_files_s3("mybucket", "root"))

    assert (
        ZarrArchiveFile(
            path=Path("root/a/b").relative_to("root"),
            size=6,
            digest="2922c85ff581cb436b3082bb16a072e2",
        )
        in files
    )
    assert (
        ZarrArchiveFile(
            path=Path("root/c").relative_to("root"),
            size=6,
            digest="a760f80cbf448dfd87da899a89c93011",
        )
        in files
    )


@mock_s3
def test_yield_files_s3_exclude_file():
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="mybucket")

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(Bucket="mybucket", Key="root/a/include_me.txt", Body="Include me")
    s3.put_object(Bucket="mybucket", Key="root/a/exclude_me.txt", Body="Exclude me")

    files = list(yield_files_s3("mybucket", "root", excluded_files=["exclude_me.txt"]))

    assert len(files) == 1
    assert (
        ZarrArchiveFile(
            path=Path("root/a/include_me.txt").relative_to("root"),
            size=10,
            digest="3fd64aa3921fb8a6e3d59479cc3dd62e",
        )
        in files
    )


@mock_s3
def test_yield_files_s3_exclude_hidden_files():
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="mybucket")

    s3 = boto3.client("s3", region_name="us-east-1")
    s3.put_object(Bucket="mybucket", Key="root/a/.hidden", Body="Hidden file")
    s3.put_object(Bucket="mybucket", Key="root/a/not_hidden.txt", Body="Not hidden file")

    files = list(yield_files_s3("mybucket", "root", ignore_hidden=True))

    assert len(files) == 1
    assert (
        ZarrArchiveFile(
            path=Path("root/a/not_hidden.txt").relative_to("root"),
            size=15,
            digest="8a15af589829a54b791084e3ada8f5b7",
        )
        in files
    )
