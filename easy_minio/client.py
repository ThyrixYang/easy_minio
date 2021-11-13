import pathlib
import os
import pickle
from typing import Iterable
import warnings
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from copy import deepcopy

from minio import Minio

from .utils import infer_format, get_bucket_and_prefix, create_parent_folder_if_not_exists, is_path


class Open:
    def __init__(self, easy_client, file_path, mode="r", refresh=True, version_id=None):
        assert mode in ["r", "rb", "w", "wb", "a"]
        file_path = str(file_path)
        self.easy_client = easy_client
        self.file_path = file_path
        self.mode = mode
        path = file_path.strip("/")
        self.cache_file_path = pathlib.Path(self.easy_client.cache_path) / path
        create_parent_folder_if_not_exists(self.cache_file_path)
        self.bucket, self.prefix = get_bucket_and_prefix(path)
        if ("r" in mode or "a" in mode) and not refresh:
            warnings.warn(
                "on file '{}', reading or appending with refresh=False, the file may be stale".format(file_path))
        if version_id is not None and "r" not in mode:
            raise ValueError()
        if "r" in mode:
            self.easy_client.get_object_cache(
                file_path, refresh=refresh, version_id=version_id)
        if "a" in mode:
            if self.easy_client.object_exists(file_path):
                self.easy_client.get_object_cache(
                    file_path, refresh=refresh, version_id=version_id)

    def __enter__(self):
        self.file = open(str(self.cache_file_path), self.mode)
        return self.file

    def __exit__(self, exception_type, exception_value, traceback):
        self.file.close()
        self.easy_client._client.fput_object(
            self.bucket, self.prefix, str(self.cache_file_path))

def unwrap_load_object_cache(args):
    mc = MinioClient(endpoint=args["endpoint"],
                     access_key=args["access_key"],
                     secret_key=args["secret_key"],
                     cache_path=args["cache_path"])
    
    return mc._load_object_cache(path=args["file_path"],
                                 refresh=args["refresh"],
                                 file_format=args["file_format"])
    
def unwrap_get_object_cache(args):
    mc = MinioClient(endpoint=args["endpoint"],
                     access_key=args["access_key"],
                     secret_key=args["secret_key"],
                     cache_path=args["cache_path"])
    return mc._get_object_cache(path=args["file_path"],
                                 refresh=args["refresh"])

class MinioClient:

    def __init__(self,
                 endpoint=None,
                 access_key=None,
                 secret_key=None,
                 cache_path=None,
                 secure=False,
                 **kwargs):
        
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.cache_path = cache_path
        if endpoint is None:
            self.endpoint = os.environ.get("MINIO_ENDPOINT")
        if access_key is None:
            self.access_key = os.environ.get("MINIO_ACCESS_KEY")
        if secret_key is None:
            self.secret_key = os.environ.get("MINIO_SECRET_KEY")
        if cache_path is None:
            self.cache_path = os.environ.get("EASY_MINIO_CACHE")
        assert self.cache_path is not None
        pathlib.Path(self.cache_path).mkdir(parents=True, exist_ok=True)
        self._client = Minio(self.endpoint,
                             access_key=self.access_key,
                             secret_key=self.secret_key,
                             secure=secure,
                             **kwargs)
        
    def download_object(self,
                        object_path,
                        local_path,
                        version_id=None,
                        verbose=False):
        bucket, prefix = get_bucket_and_prefix(object_path)
        if verbose:
            print("Downloading object {}".format(object_path))
        try:
            self._client.fget_object(bucket_name=bucket,
                                    object_name=prefix,
                                    file_path=local_path,
                                    version_id=version_id)
        except Exception as e:
            return e
        return local_path

    def get_object_cache(self,
                         path,
                         refresh=False,
                         version_id=None,
                         verbose=False):
        if is_path(path):
            return self._get_object_cache(path, refresh=refresh, version_id=version_id, verbose=verbose)
        elif isinstance(path, Iterable):
            assert version_id is None
            queries = []
            for p in path:
                query = {
                    "endpoint": self.endpoint,
                    "access_key": self.access_key,
                    "secret_key": self.secret_key,
                    "cache_path": self.cache_path,
                    "file_path": p,
                    "refresh": refresh,
                }
                queries.append(query)
            with Pool() as pool:
                cache_paths = pool.map(unwrap_get_object_cache, queries)
            return cache_paths
        else:
            raise ValueError()

    def _get_object_cache(self, path, refresh=False, version_id=None, verbose=False):
        path = str(path).strip("/")
        cache_file_path = pathlib.Path(self.cache_path) / path
        create_parent_folder_if_not_exists(cache_file_path)

        if refresh and cache_file_path.is_file() and version_id is None:
            os.remove(str(cache_file_path))

        if not cache_file_path.is_file() or version_id is not None:
            res = self.download_object(path, 
                str(cache_file_path), 
                version_id=version_id, 
                verbose=verbose)
            # bucket, prefix = get_bucket_and_prefix(path)
            # if verbose:
            #     print("Downloading object {}".format(path))
            # try:
            #     self._client.fget_object(bucket_name=bucket,
            #                             object_name=prefix,
            #                             file_path=str(cache_file_path),
            #                             version_id=version_id)
            # except Exception as e:
            #     return e
            return res
            return str(cache_file_path)
        else:
            return str(cache_file_path)

    def load_object_cache(self,
                          path,
                          refresh=False,
                          version_id=None,
                          verbose=False,
                          file_format=None):
        if is_path(path):
            return self._load_object_cache(path,
                                           refresh=refresh,
                                           version_id=version_id,
                                           verbose=verbose,
                                           file_format=file_format)
        elif isinstance(path, Iterable):
            queries = []
            for p in path:
                query = {
                    "endpoint": self.endpoint,
                    "access_key": self.access_key,
                    "secret_key": self.secret_key,
                    "cache_path": self.cache_path,
                    "file_path": p,
                    "refresh": refresh,
                    "file_format": file_format
                }
                queries.append(query)
                
            with Pool() as pool:
                objs = pool.map(unwrap_load_object_cache, queries)
            assert version_id is None
            return objs
        else:
            raise ValueError()

    def _load_object_cache(self,
                           path,
                           refresh=False,
                           version_id=None,
                           verbose=False,
                           file_format=None):
        path = str(path)
        if file_format is None:
            file_format = infer_format(path)
        object_cache_path = self._get_object_cache(
            path, refresh=refresh, version_id=version_id, verbose=verbose)
        if isinstance(object_cache_path, Exception):
            return object_cache_path
        if file_format == "pickle":
            with open(object_cache_path, "rb") as f:
                return pickle.load(f)
        else:
            raise ValueError(
                "file_format {} not supported".format(file_format))

    def dump_object_cache(self,
                          obj,
                          path,
                          file_format=None,
                          verbose=False):
        path = str(path)
        if file_format is None:
            file_format = infer_format(path)
        path = path.strip("/")
        cache_file_path = pathlib.Path(self.cache_path) / path
        create_parent_folder_if_not_exists(cache_file_path)
        bucket, prefix = get_bucket_and_prefix(path)
        if cache_file_path.is_file():
            os.remove(str(cache_file_path))
        if file_format == "pickle":
            with open(cache_file_path, "wb") as f:
                pickle.dump(obj, f)
        else:
            raise ValueError(
                "file_format {} not supported".format(file_format))

        if verbose:
            print("Putting object {}".format(path))
        self._client.fput_object(bucket, prefix, str(cache_file_path))
        return str(cache_file_path)

    def object_exists(self, path):
        path = str(path)
        bucket, prefix = get_bucket_and_prefix(path)
        try:
            self._client.stat_object(bucket, prefix)
            return True
        except Exception as e:
            if "NoSuchKey" in str(e) or "NoSuchBucket" in str(e):
                return False
            else:
                raise e

    def open(self, file_path, mode="r", refresh=True):
        return Open(self, file_path, mode=mode, refresh=refresh, version_id=None)

    def make_bucket(self, bucket, exist_ok=True):
        if self._client.bucket_exists(bucket):
            if exist_ok:
                return
            else:
                raise ValueError()
        else:
            self._client.make_bucket(bucket)

    def list_objects(self, path, recursive=True, verbose=False):
        bucket, prefix = get_bucket_and_prefix(path)
        objs = []
        if verbose:
            print(
                "----------------- listing objects in {} --------------------".format(path))
        for obj in self._client.list_objects(bucket,
                                             prefix=prefix,
                                             recursive=recursive,
                                             include_version=False,
                                             use_url_encoding_type=False):
            if verbose:
                print("bucket_name: {}, is_dir: {}, object_name: {}".format(
                    obj.bucket_name, obj.is_dir, obj.object_name))
            if not obj.is_dir:
                objs.append("/".join([obj.bucket_name, obj.object_name]))
        return objs
        # print(obj.bucket_name, obj.content_type, obj.etag, obj.is_dir, obj.is_latest, obj.last_modified, obj.metadata, obj.object_name, obj.size, obj.storage_class, obj.version_id)
        # data None None True None None None datasets/ None None None