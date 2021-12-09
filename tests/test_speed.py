import pathlib
import pickle
import time

import numpy as np

from easy_minio import MinioClient

test_bucket_name = "easy-minio-test"
test_object = {"a": 1, "b": 2}

def test_rw_multiple_objects():
    mc = MinioClient()
    num = 1000
    t1 = time.time()
    for i in range(num):
        # msg = "id:{}".format(i)
        msg = np.random.randn(100, 100)
        file_path = pathlib.PurePosixPath(
            test_bucket_name) / "multi/dump_object_{}.pkl".format(i)
        mc.dump_object_cache(msg, file_path)
    t2 = time.time()
    print("put time {}".format(t2 - t1))
    paths = []
    for i in range(num):
        file_path = pathlib.PurePosixPath(
            test_bucket_name) / "multi/dump_object_{}.pkl".format(i)
        paths.append(file_path)
    objs = mc.load_object_cache(paths, refresh=True)
    print("len ", len(objs))
    # print(obj)
    t3 = time.time()
    print("load object time {}".format(t3 - t2))


def test_get_multiple_objects():
    mc = MinioClient()
    num = 1000
    t1 = time.time()
    paths = []
    for i in range(num):
        file_path = pathlib.PurePosixPath(
            test_bucket_name) / "multi/dump_object_{}.pkl".format(i)
        paths.append(file_path)
    cache_paths = mc.get_object_cache(paths, refresh=True)
    print("len ", len(cache_paths))
    t2 = time.time()
    print("get object time {}".format(t2 - t1))


def test_get_multiple_objects_error():
    mc = MinioClient()
    num = 10
    paths = []
    for i in range(num):
        file_path = pathlib.PurePosixPath(
            test_bucket_name) / "multi/dump_object_{}_ne.pkl".format(i)
        paths.append(file_path)
    try:
        _ = mc.get_object_cache(paths, refresh=True)
    except Exception as e:
        print("{}".format(e))

def test_load_multiple_objects_error():
    mc = MinioClient()
    num = 10
    paths = []
    for i in range(num):
        file_path = pathlib.PurePosixPath(
            test_bucket_name) / "multi/dump_object_{}_ne.pkl".format(i)
        paths.append(file_path)
    print(paths)
    try:
        _ = mc.load_object_cache(paths, refresh=True)
    except Exception as e:
        print("{}".format(e))