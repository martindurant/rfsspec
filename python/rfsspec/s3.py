from functools import lru_cache
from rfsspec.rfsspec import (s3_cat_ranges, s3_info, s3_find, s3_pipe, s3_init_upload,
                             s3_upload_chunk, s3_complete_upload)

from fsspec.spec import AbstractFileSystem, AbstractBufferedFile


class RustyS3FileSystem(AbstractFileSystem):
    """

    """

    def __init__(
            self,
            profile=None,
            endpoint_url=None,
            requester_pays=False,
            region="us-east-1",
            anon=False,
            default_cache_type="readahead",
            **storage_options,
    ):
        """
        """
        self.kwargs = dict(profile=profile, endpoint_url=endpoint_url, requester_pays=requester_pays,
                           region=region, anon=anon)
        self.default_cache_type = default_cache_type
        super().__init__(self, **storage_options)

    def cat_file(self, url, start=None, end=None, **kwargs):
        return s3_cat_ranges([url], start=[start or 0], end=[end or 0], **self.kwargs)[0]

    def cat(self, path, recursive=False, on_error="raise", start=0, end=0, **kwargs):
        paths = [path] if isinstance(path, str) else path
        if (
                len(paths) > 1
                or isinstance(path, list)
                or paths[0] != self._strip_protocol(path)
        ):
            out = {p: _ for p, _ in zip(paths, s3_cat_ranges(
                paths, start=[start or 0] * len(path), end=[end or 0] * len(path), **self.kwargs))}
            return out
        else:
            return s3_cat_ranges(paths, start=[start or 0], end=[end or 0], **self.kwargs)[0]

    def cat_ranges(self, urls, starts, ends, **kwargs):
        return s3_cat_ranges(urls, start=starts, end=ends, **self.kwargs)

    def info(self, path):
        info = s3_info(path, **self.kwargs)
        info["type"] = "file"
        info["name"] = path
        return info

    def pipe(self, path, value=None):
        # pipes only just one for now, for use bu the file-like API
        if isinstance(path, str):
            path = {path: value}
        kw = self.kwargs.copy()
        kw.pop("anon")
        kw.pop("requester_pays")
        s3_pipe(path, **kw)

    def _open(self, path, mode="rb", **kwargs):
        size = int(self.info(path)["size"]) if "r" in mode else None
        if "cache_type" not in kwargs:
            kwargs["cache_type"] = self.default_cache_type
        return RustyS3File(self, path, mode=mode, size=size, **kwargs)

    def find(self, path):
        return s3_find(path, **self.kwargs)


class RustyS3File(AbstractBufferedFile):
    DEFAULT_BLOCK_SIZE = 50*2**20  # TODO: enforce 5MB minimum?
    mpu = None

    def _fetch_range(self, start, end):
        return self.fs.cat_file(self.path, start=start, end=end)

    def _upload_chunk(self, final=False):
        kw = self.fs.kwargs.copy()
        kw.pop("requester_pays")
        kw.pop("anon")
        if final:
            if self.mpu is None:
                # one-shot upload
                self.fs.pipe(self.path, self.buffer.getvalue())
            else:
                part = len(self.parts) + 1
                self.parts[part] = s3_upload_chunk(self.path, self.mpu, self.buffer.getbuffer(), part, **kw)
                s3_complete_upload(self.path, self.mpu, self.parts, **kw)
        elif self.buffer.tell() > self.blocksize:
            if self.mpu is None:
                self.mpu = s3_init_upload(self.path, **kw)
                self.parts = {1: s3_upload_chunk(self.path, self.mpu, self.buffer.getbuffer(), 1, **kw)}
            else:
                part = len(self.parts) + 1
                self.parts[part] = s3_upload_chunk(self.path, self.mpu,self.buffer.getbuffer(), part, **kw)
        else:
            # nothing to do
            return False

        return True


@lru_cache()
def get_bucket_region(bucket):
    import requests
    return requests.head(f"https://{bucket}.s3.amazonaws.com").headers["x-amz-bucket-region"]