import time

import fsspec
import numpy as np

import rfsspec


starts = np.random.randint(0, 50, size=100)
ends = np.random.randint(100, 150, size=100)

u = 'https://noaa-nwm-retrospective-2-1-zarr-pds.s3.amazonaws.com/rtout.zarr/.zmetadata'
N = 100


fs = rfsspec.RustyHTTPFileSystem()
t0 = time.time()
for _ in range(N):
    fs.cat_ranges([u] * 100, starts, ends)
print("Rust:", np.round((time.time() - t0) / N, 4))


fs = fsspec.filesystem("http")
t0 = time.time()
for _ in range(N):
    fs.cat_ranges([u] * 100, starts, ends)
print("asyncio:", np.round((time.time() - t0) / N, 4))