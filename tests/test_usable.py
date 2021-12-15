import pathlib
import pickle
import time
import os

import numpy as np

from easy_minio import MinioClient
from easy_minio import S3KV


test_bucket_name = "easy-minio-test"
test_object = {"a": 1, "b": 2}


def test_construct():
    mc = MinioClient()
    mc.make_bucket(test_bucket_name)


def test_open_write():
    mc = MinioClient()
    mc.make_bucket(test_bucket_name)
    file_path = pathlib.PurePosixPath(test_bucket_name) / "text_test.txt"
    with mc.open(file_path, "w") as f:
        f.write("test string")


def test_open_read():
    mc = MinioClient()
    file_path = pathlib.PurePosixPath(test_bucket_name) / "text_test.txt"
    with mc.open(file_path, "r") as f:
        lines = f.readlines()
    assert len(lines) == 1
    assert lines[0] == "test string"


def test_open_append():
    mc = MinioClient()
    file_path = pathlib.PurePosixPath(test_bucket_name) / "text_test.txt"
    with mc.open(file_path, "a") as f:
        f.write(" appended string")

    with mc.open(file_path, "r") as f:
        lines = f.readlines()

    assert len(lines) == 1
    assert lines[0] == "test string appended string"


def test_open_wb_rb():
    mc = MinioClient()
    file_path = pathlib.PurePosixPath(test_bucket_name) / "text_test.pkl"
    with mc.open(file_path, "wb") as f:
        pickle.dump("test wb rb", f)

    with mc.open(file_path, "rb") as f:
        s = pickle.load(f)
    assert s == "test wb rb"


def test_object_exists():
    mc = MinioClient()
    file_path = pathlib.PurePosixPath(test_bucket_name) / "text_test.pkl"
    res1 = mc.object_exists(str(file_path))
    res2 = mc.object_exists("a" + str(file_path))
    assert res1 == True
    assert res2 == False


def test_dump_object_cache():
    mc = MinioClient()
    file_path = pathlib.PurePosixPath(
        test_bucket_name) / "dump/dump_object.pkl"
    mc.dump_object_cache(test_object, file_path)


def test_load_object_cache():
    mc = MinioClient()
    file_path = pathlib.PurePosixPath(
        test_bucket_name) / "dump/dump_object.pkl"
    obj = mc.load_object_cache(file_path)
    assert obj == test_object


def test_list_object():
    mc = MinioClient()
    res = mc.list_objects(test_bucket_name, verbose=False)
    assert len(res) > 0
    
def test_kv_store():
    np.random.seed(0)
    k1 = np.random.randn(3, 4)
    v1 = np.random.randn(1, 2)
    k2 = np.random.randn(2, 1)
    v2 = np.random.randn(1, 1)
    client = MinioClient()
    kv = S3KV(client, path="kv-store/test")
    kv.put(k1, v1)
    kv.put(k2, v2)
    assert np.array_equal(kv.get(k1), v1)
    assert np.array_equal(kv.get(k2), v2)
    # print(kv.get(k1))
    # print(kv.get(k2))

def test_auto_refresh():
    # auto refresh has resolution = 1 seconds
    mc1 = MinioClient(disable_auto_refresh=True, cache_path="D:\\tmp\\test_aux_cache") # do not save mtime
    mc2 = MinioClient()
    file_path = str(pathlib.PurePosixPath(test_bucket_name) / "text_auto_refresh.pkl")
    cnt1 = 0
    for i in range(10):
        time.sleep(1)
        mc1.dump_object_cache("a", file_path)
        s1 = mc2.load_object_cache(file_path, verbose=False)
        mc1.dump_object_cache("b", file_path)
        s2 = mc2.load_object_cache(file_path, verbose=False)
        if s1 == s2 and s1 == "a":
            cnt1 += 1
    assert cnt1 >= 9
    
    cnt2 = 0
    for i in range(10):
        time.sleep(1)
        mc1.dump_object_cache("a", file_path)
        s1 = mc2.load_object_cache(file_path, verbose=False)
        time.sleep(1)
        mc1.dump_object_cache("b", file_path)
        s2 = mc2.load_object_cache(file_path, verbose=False)
        if s1 == "a" and s2 == "b":
            cnt2 += 1
    assert cnt2 == 10
    
def test_upload_file():
    mc = MinioClient()
    r = mc.upload_file(os.path.abspath(__file__), "/tmp/test/test_usable.txt", verbose=True)
    assert r