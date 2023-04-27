from s3fs.tests.test_s3fs import s3, s3_base, get_boto3_client, endpoint_uri, test_bucket_name

import rfsspec


def test_pipe_cat(s3):
    fs = rfsspec.RustyS3FileSystem(endpoint_url=endpoint_uri)
    fn = f"{test_bucket_name}/rusty1"
    bs = b"0" * 50_000_000
    fs.pipe(fn, bs)
    assert fs.cat(fn) == bs


def test_oneshot_roundtrip(s3):

    fs = rfsspec.RustyS3FileSystem(endpoint_url=endpoint_uri)
    fn = f"{test_bucket_name}/rusty1"
    bs = 50

    with fs.open(fn, mode="wb", blocksize=bs) as f:
        f.write(b"0" * (bs-1))  # no flush

    out = fs.cat(fn)
    assert out == b"0" * (bs-1)


def test_multipart_roundtrip(s3):

    fs = rfsspec.RustyS3FileSystem(endpoint_url=endpoint_uri)
    fn = f"{test_bucket_name}/rusty1"
    bs = 10 * 2**20

    # one-shot
    with fs.open(fn, mode="wb", block_size=bs) as f:
        f.write(b"0" * (bs + 1))  # init and first flush
        assert f.mpu
        assert f.parts
        assert s3.list_multipart_uploads(test_bucket_name)
        f.write(b"0" * (bs - 1))  # refill buffer, flushed on close

    out = fs.cat(fn)
    assert out == b"0" * (bs * 2)
