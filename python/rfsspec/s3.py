from __future__ import absolute_import, division, print_function

import logging
import re
from copy import copy

import fsspec.utils
from rfsspec.rfsspec import s3_cat_ranges

from fsspec.spec import AbstractFileSystem
from fsspec.utils import DEFAULT_BLOCK_SIZE


class RustyS3FileSystem(AbstractFileSystem):
    """

    """

    sep = "/"

    def __init__(
            self,
            profile=None,
            endpoint_url=None,
            requester_pays=False,
            region="us-east-1",
            **storage_options,
    ):
        """
        """
        self.kwargs = dict(profile=profile, endpoint_url=endpoint_url, requester_pays=requester_pays,
                           region=region)
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
            return self.cat_file(paths[0], start=start, end=end, **kwargs)

    def cat_ranges(self, urls, starts, ends, **kwargs):
        return s3_cat_ranges(urls, start=starts, end=ends, **self.kwargs)
