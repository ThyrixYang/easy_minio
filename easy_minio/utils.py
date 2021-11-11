import pathlib


def infer_format(path):
    extension = path.split(".")[-1]
    if extension == "pkl":
        return "pickle"
    else:
        raise ValueError("Extension {} not supported".format(extension))


def get_bucket_and_prefix(path):
    path = path.strip("/")
    sp = path.split("/")
    bucket = sp[0]
    obj_prefix = "/".join(sp[1:])
    return bucket, obj_prefix


def create_parent_folder_if_not_exists(path):
    p = pathlib.Path(path)
    pathlib.Path(p.parent).mkdir(parents=True, exist_ok=True)


def create_folder_if_not_exists(path):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def is_path(o):
    return isinstance(o, (str, 
                          pathlib.Path, 
                          pathlib.PurePosixPath, 
                          pathlib.PosixPath))