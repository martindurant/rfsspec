# rfsspec
Rust python FS

This HTTP getter respects concurrency of many simultaneous requests as made by
fsspec, but 
- does not need python asyncio, 
- releases the GIL, 
- can safely be called from multiple threads.

Latest results from examples/script.py:
```commandline
Rust: 0.129
asyncio: 0.2486
```
