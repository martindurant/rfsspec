import rfsspec
import fsspec
from conftest import data


def test_instance():
    fs = rfsspec.RustyHTTPFileSystem()
    assert isinstance(fs, fsspec.spec.AbstractFileSystem)


def test_one(server):
    fs = rfsspec.RustyHTTPFileSystem()
    url = server + "/index/realfile"
    assert fs.cat(url) == data
