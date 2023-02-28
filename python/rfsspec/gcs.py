from __future__ import absolute_import, division, print_function

from rfsspec.rfsspec import gcs_cat_ranges

from fsspec.spec import AbstractFileSystem


class RustyGCSFileSystem(AbstractFileSystem):
    """

    """

    def __init__(
            self,
            anon=False,
            **storage_options,
    ):
        """
        """
        self.kwargs = dict(anon=anon)
        super().__init__(self, **storage_options)

    def cat_file(self, url, start=None, end=None, **kwargs):
        return gcs_cat_ranges([url], start=[start or 0], end=[end or 0], **self.kwargs)[0]

    def cat(self, path, recursive=False, on_error="raise", start=0, end=0, **kwargs):
        paths = [path] if isinstance(path, str) else path
        if (
                len(paths) > 1
                or isinstance(path, list)
                or paths[0] != self._strip_protocol(path)
        ):
            out = {p: _ for p, _ in zip(paths, gcs_cat_ranges(
                paths, start=[start or 0] * len(path), end=[end or 0] * len(path), **self.kwargs))}
            return out
        else:
            return self.cat_file(paths[0], start=start, end=end, **kwargs)

    def cat_ranges(self, urls, starts, ends, **kwargs):
        return gcs_cat_ranges(urls, start=starts, end=ends, **self.kwargs)
