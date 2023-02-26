import time

import fsspec
import numpy as np

import rfsspec


N = 50
R = 10
starts = np.random.randint(0, 50, size=N).tolist()
ends = np.random.randint(100, 150, size=N).tolist()

u = 'https://noaa-nwm-retrospective-2-1-zarr-pds.s3.amazonaws.com/rtout.zarr/.zmetadata'
u2 = "noaa-nwm-retrospective-2-1-zarr-pds/rtout.zarr/.zmetadata"


fs = rfsspec.RustyHTTPFileSystem()
fs.cat_ranges([u] * N, starts, ends)  # warm up
t0 = time.time()
for _ in range(R):
    fs.cat_ranges([u] * N, starts, ends)
print("Rust http:", np.round((time.time() - t0) / R, 4))

fs = rfsspec.RustyS3FileSystem()
fs.cat_ranges([u2] * N, starts, ends)  # warm up
t0 = time.time()
for _ in range(R):
    fs.cat_ranges([u2] * N, starts, ends)
print("Rust s3:", np.round((time.time() - t0) / R, 4))

fs = fsspec.filesystem("http")
fs.cat_ranges([u] * N, starts, ends)  # warm up
t0 = time.time()
for _ in range(R):
    fs.cat_ranges([u] * N, starts, ends)
print("fsspec http:", np.round((time.time() - t0) / R, 4))

fs = fsspec.filesystem("s3")
fs.cat_ranges([u2] * N, starts, ends)  # warm up
t0 = time.time()
for _ in range(R):
    fs.cat_ranges([u2] * N, starts, ends)
print("fsspec s3:", np.round((time.time() - t0) / R, 4))
