# rfsspec
Rust python FSs

Implements s3, gcs, azure blob and HTTP backends for fsspec using Rust.

Respects concurrency of many simultaneous requests as made by
fsspec, but 
- does not need python asyncio, 
- releases the GIL, 
- can safely be called from multiple threads
- is probably *NOT* fork-safe

#### Limitations

Currently only the methods `cat_ranges`, `cat` and `cat_file` are supported, enough
to open a (consolidated) zarr dataset for reading.

#### Implementations

The http backend supports:
- headers
- method
- ranges

The s3 implementation supports configuration by environment variables and .aws files,
and options
- profile
- endpoint_url
- anon
- region
- ranges
- requester-pays

The GCS backend supports:
- anon
- ranges
- requester-pays
- assumes credentials via env variables and gcloud CLI

The Azure blob backend supports
- anon
- ranges
- account/key auth (account always required)

### Installation

```commandline
> pip install -i https://pypi.anaconda.org/mdurant/simple rfsspec
```

### Benchmarks

Latest results from examples/script.py:
```commandline
Rust http: 0.1335
Rust s3: 0.1163
Rust anon s3: 0.0921
fsspec http: 0.1611
fsspec s3: 0.4758
fsspec anon s3: 0.4208
```

(multi-threaded benchmarks should be more meaningful)
