import pathlib
import pickle
import time

import numpy as np

from easy_minio import MinioClient

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
    print(res)


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