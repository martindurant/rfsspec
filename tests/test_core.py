import rfsspec
import fsspec
from conftest import data


def test_instance():
    fs = rfsspec.RustyHTTPFileSystem()
    assert isinstance(fs, fsspec.spec.AbstractFileSystem)


def test_cat_one(server):
    fs = rfsspec.RustyHTTPFileSystem()
    url = server + "/index/realfile"
    assert fs.cat(url) == data


def test_method_header(server):
    fs = rfsspec.RustyHTTPFileSystem()
    out = fs.cat(server, method="PATCH", headers={"test": "True"})
    assert b"test: True" in out


def test_get_one(server, tmpdir):
    fs = rfsspec.RustyHTTPFileSystem()
    url = server + "/index/realfile"
    lpath = f"{tmpdir}/afile"
    fs.get(url, lpath)
    with open(lpath, "rb") as f:
        assert f.read() == data
