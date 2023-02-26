# rfsspec
Rust python FS

This HTTP getter respects concurrency of many simultaneous requests as made by
fsspec, but 
- does not need python asyncio, 
- releases the GIL, 
- can safely be called from multiple threads.

Latest results from examples/script.py:
```commandline
Rust http: 0.0932
Rust s3: 0.1017
fsspec http: 0.0978
fsspec s3: 0.224
```
