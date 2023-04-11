from s3fs.tests.test_s3fs import s3, s3_base, get_boto3_client, endpoint_uri, test_bucket_name

import rfsspec


def test_oneshot_roundtrip(s3):

    fs = rfsspec.RustyS3FileSystem(endpoint_url=endpoint_uri)
    fn = f"{test_bucket_name}/rusty1"
    bs = 5 * 2**20

    with fs.open(fn, mode="wb", blocksize=bs) as f:
        f.write(b"0" * (bs-1))  # no flush

    out = fs.cat(fn)
    assert out == b"0" * (bs-1)


def test_multipart_roundtrip(s3):

    fs = rfsspec.RustyS3FileSystem(endpoint_url=endpoint_uri)
    fn = f"{test_bucket_name}/rusty1"
    bs = 5 * 2**20

    # one-shot
    with fs.open(fn, mode="wb", blocksize=bs) as f:
        f.write(b"0" * (bs + 1))  # init and first flush
        f.write(b"0" * (bs - 1))  # refill buffer, flushed on close

    out = fs.cat(fn)
    assert out == b"0" * (bs * 2)
