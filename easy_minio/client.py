import pathlib
import os
import pickle
from typing import Iterable
import warnings
from multiprocessing import Pool
import shutil


from sqlitedict import SqliteDict
import xxhash
from minio import Minio

from .utils import infer_format, get_bucket_and_prefix, create_parent_folder_if_not_exists, is_path, os_name

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
            else:
                self.cache_file_path.unlink(missing_ok=True)

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
                                 file_format=args["file_format"],
                                 version_id=None,
                                 verbose=False)
    
def unwrap_get_object_cache(args):
    mc = MinioClient(endpoint=args["endpoint"],
                     access_key=args["access_key"],
                     secret_key=args["secret_key"],
                     cache_path=args["cache_path"])
    return mc._get_object_cache(path=args["file_path"],
                                 refresh=args["refresh"],
                                 version_id=None,
                                 verbose=False)
    
    
class S3KV:
    
    def __init__(self, client, path, conflict_error=False):
        self.client = client
        self.path = path
        self.conflict_error = conflict_error
        
    def get_hash(self, k):
        h = xxhash.xxh64()
        h.update(k)
        hash_value = h.hexdigest()
        return hash_value
        
    def put(self, k, v):
        hash_value = self.get_hash(k)
        object_name = "k{}.pkl".format(hash_value)
        object_path = str(pathlib.PurePosixPath(self.path) / object_name)
        if self.client.object_exists(object_path):
            if self.conflict_error:
                raise ValueError("dumping to a existing key: {}".format(object_name))
            else:
                warnings.warn("dumping to a existing key: {}".format(object_name))
        self.client.dump_object_cache({"k": k, "v": v}, object_path)
    
    def get(self, k, default=None):
        hash_value = self.get_hash(k)
        object_name = "k{}.pkl".format(hash_value)
        object_path = str(pathlib.PurePosixPath(self.path) / object_name)
        if self.client.object_exists(object_path):
            obj_dict = self.client.load_object_cache(object_path)
            return obj_dict["v"]
        else:
            return default
    

class MinioClient:

    def __init__(self,
                 endpoint=None,
                 access_key=None,
                 secret_key=None,
                 cache_path=None,
                 client_name=None,
                 secure=False,
                 disable_auto_refresh=False,
                 **kwargs):
        
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.cache_path = cache_path
        self.disable_auto_refresh = disable_auto_refresh
        if endpoint is None:
            self.endpoint = os.environ.get("MINIO_ENDPOINT")
        if access_key is None:
            self.access_key = os.environ.get("MINIO_ACCESS_KEY")
        if secret_key is None:
            self.secret_key = os.environ.get("MINIO_SECRET_KEY")
        if cache_path is None:
            self.cache_path = os.environ.get("EASY_MINIO_CACHE")
        if client_name is None:
            self.client_name = os.environ.get("MINIO_NAME")
        assert self.cache_path is not None
        pathlib.Path(self.cache_path).mkdir(parents=True, exist_ok=True)
        self._client = Minio(self.endpoint,
                             access_key=self.access_key,
                             secret_key=self.secret_key,
                             secure=secure,
                             **kwargs)
        self._database_path = str(pathlib.Path(self.cache_path) / "easy_minio.sqlite")
        if not self.disable_auto_refresh:
            self._mtime_dict = SqliteDict(filename=self._database_path, 
                                        tablename="mtime",
                                        autocommit=True)
        
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
                         refresh="auto",
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

    def _get_object_cache(self, path, refresh, version_id, verbose):
        path = str(path).strip("/")
        bucket, prefix = get_bucket_and_prefix(path)
        cache_file_path = pathlib.Path(self.cache_path) / path
        create_parent_folder_if_not_exists(cache_file_path)
        
        if refresh == "auto":
            if self.disable_auto_refresh:
                raise ValueError("auto refresh is disabled but refresh is 'auto'")
            modify_time = self._mtime_dict.get(path, None)
            stat = self._client.stat_object(bucket, prefix)
            remote_mtime = str(stat.last_modified.timestamp())
            if str(remote_mtime) == modify_time:
                refresh = False
            else:
                refresh = True

        if refresh and cache_file_path.is_file() and version_id is None:
            os.remove(str(cache_file_path))

        if not cache_file_path.is_file() or version_id is not None:
            res = self.download_object(path, 
                str(cache_file_path), 
                version_id=version_id, 
                verbose=verbose)
            
            stat = self._client.stat_object(bucket, prefix)
            remote_mtime = str(stat.last_modified.timestamp())
            self._mtime_dict[path] = remote_mtime
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
        else:
            return str(cache_file_path)

    def load_object_cache(self,
                          path,
                          refresh="auto",
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
                           refresh,
                           version_id,
                           verbose,
                           file_format):
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
        if not self.disable_auto_refresh:
            stat = self._client.stat_object(bucket, prefix)
            remote_mtime = str(stat.last_modified.timestamp())
            self._mtime_dict[path] = remote_mtime
        return str(cache_file_path)
    
    def upload_file(self,
                    file_path,
                    remote_path,
                    verbose=False):
        remote_path = str(remote_path)
        file_path = str(file_path)
        remote_path = remote_path.strip("/")
        bucket, prefix = get_bucket_and_prefix(remote_path)

        if verbose:
            print("Putting object {}".format(file_path))
        self._client.fput_object(bucket, prefix, file_path)
        if not self.disable_auto_refresh:
            stat = self._client.stat_object(bucket, prefix)
            remote_mtime = str(stat.last_modified.timestamp())
            self._mtime_dict[remote_path] = remote_mtime
        return file_path

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

    def open(self, file_path, mode="r", refresh="auto"):
        return Open(self, file_path, mode=mode, refresh=refresh, version_id=None)
    
    def download_sync(self, remote_path, local_path, refresh="auto", verbose=False):
        objs = self.list_objects(remote_path, verbose=verbose)
        _ = self.get_object_cache(objs, verbose=verbose, refresh=refresh)
        cache_dir = pathlib.Path(self.cache_path) / remote_path.strip("/")
        if os_name == "Windows":
            warnings.warn("on windows sync is done by simply copy cache, thus not fully supported")
            shutil.copytree(str(cache_dir), str(local_path), dirs_exist_ok=True)
        else:
            os.symlink(cache_dir, local_path, target_is_directory=True)
    
    def upload_folder(self, local_path, remote_path, verbose=False):
        """Note: the same as cp local_path/* remote_path/*, so local_path name is not contained.

        Args:
            local_path ([type]): [description]
            remote_path ([type]): [description]
            verbose (bool, optional): [description]. Defaults to False.
        """
        remote_path = pathlib.PurePosixPath(remote_path)
        for root, dir_names, file_names in os.walk(local_path):
            r = root.replace(local_path, "")
            if os_name == "Windows":
                r = r.strip("\\")
            else:
                r = r.strip("/")
            remote_prefix = remote_path / r
            for fn in file_names:
                local_file_path = os.path.join(root, fn)
                remote_file_path = str(remote_prefix / fn)
                self.upload_file(local_file_path, remote_file_path, verbose=verbose)
                if verbose:
                    print("uploading ", r, dir_names, file_names)

    # def make_bucket(self, bucket, exist_ok=True):
    #     if self._client.bucket_exists(bucket):
    #         if exist_ok:
    #             return
    #         else:
    #             raise ValueError()
    #     else:
    #         self._client.make_bucket(bucket)

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
    
    def __del__(self):
        if not self.disable_auto_refresh:
            self._mtime_dict.close()