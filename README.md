This is a wrapper of [minio-py](https://github.com/minio/minio-py).

## Features

easy-minio supports following features:

1. Unix like path to locate objects, e.g. "bucket/p1/p2/p3/file", instead of get(bucket, p1/p2/p3/file) in minio-py.
2. Python native cache feature, without setup minio gateway.
3. Easy to use open(path, mode) operation to modify file directly.
4. Parallel objects downloading based on multiprocessing.
5. Upload or download multiple objects in one function call.

## Install
```
python -m pip install git+https://github.com/ThyrixYang/easy_minio.git
```
## Usage

```python
ss

```


---

TODO:
1. Write documents.