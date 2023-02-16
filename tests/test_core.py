import threading

import rfsspec
import fsspec
from conftest import data


def test_instance():
    fs = rfsspec.RustyHTTPFileSystem()
    assert isinstance(fs, fsspec.spec.AbstractFileSystem)


def test_cat_one(server):
    fs = rfsspec.RustyHTTPFileSystem()
    url = server + "/index/realfile"
    out = fs.cat(url)
    assert len(out) == len(data)
    assert out == data


def test_cat_many(server):
    fs = rfsspec.RustyHTTPFileSystem()
    url = server + "/index/realfile"
    starts = list(range(1, 5))
    ends = list(range(41, 45))
    out = fs.cat_ranges([url] * len(starts), starts, ends)
    assert len(out) == len(starts)
    for start, end, o in zip(starts, ends, out):
        assert o == data[start:end]


def test_other_threads(server):
    fs = rfsspec.RustyHTTPFileSystem()
    url = server + "/index/realfile"
    out = []
    threads = []

    def target():
        out.append(fs.cat(url))

    for i in range(5):
        th = threading.Thread(target=target)
        th.start()
        threads.append(th)
    [th.join() for th in threads]
    assert out == [data] * 5


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
        out = f.read()
    assert len(out) == len(data)
    assert out == data
