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
python -m pip install git+git://github.com/ThyrixYang/easy_minio.git@main
```
## Usage

There are two approaches to set your accounts:
1. Setup environment variables, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT.
2. Pass to MinioClient, if keys are provided in the arguments, the environment variables will be ignored.
3. You need to setup a cache_path using EASY_MINIO_CACHE environment variable or cache_path argument.

```python
from easy_minio import MinioClient
import pickle

mc = MinioClient() # If environment variables have been set.
mc = MinioClient(endpoint=..., access_key=..., secret_key=..., cache_path=...) # set arguments in constructor.

file_path = "bucket_name/dir1/dir2/test.txt"

with mc.open(file_path, "w") as f:
    f.write("test string")

with mc.open(file_path, "r") as f:
    f.readlines()

# The file can be operated as usual, thus we can also use pickle
with mc.open(file_path, "wb") as f:
    pickle.dump("test", f)

# Check if a object exists
res = mc.object_exists(file_path)

```


---

TODO:
1. Write documents.