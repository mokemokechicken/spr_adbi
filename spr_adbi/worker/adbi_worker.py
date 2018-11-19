import json
import os
import shutil
import sys
from io import BytesIO
from pathlib import Path
from traceback import format_exception
from typing import List, Optional, ByteString

from spr_adbi.const import STATUS_SUCCESS, STATUS_ERROR, PATH_STDIN, PATH_ARGS
from spr_adbi.util.s3_util import get_s3_client, upload_fileobj_to_s3, upload_file_to_s3, download_as_data_from_s3, \
    split_bucket_and_key

from spr_adbi.util import s3_util


def create_worker(args: List[str] = None):
    """Usage:
        create_worker()
        create_worker(args_list)

    :param args:
    :return:
    """
    if args is None:
        args = sys.argv[1:]
    assert args and isinstance(args, (list, tuple))
    storage_dir = args[0]
    if storage_dir.startswith("s3://"):
        return S3ADBIWorker(args)
    else:
        return LocalADBIWorker(args)


class ADBIWorker:
    def __init__(self, args: List[str]):
        self.finished = False
        self.storage_dir = args[0]
        self._args = args[1:]
        self._setup()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.finished:
            if exc_type is None:
                self.success()
            else:
                self.error("".join(format_exception(exc_type, exc_val, exc_tb)))

    def args(self) -> List[str]:
        if self._args:
            ret = self._args
        else:
            args_json = self.read(PATH_ARGS)
            if args_json:
                ret = json.loads(args_json)
            else:
                ret = []
        return ret

    def stdin(self) -> Optional[ByteString]:
        if not os.isatty(0):  # 0 means STDIN
            data = sys.stdin.read()
        else:
            data = self.read(PATH_STDIN)

        if isinstance(data, str):
            return data.encode()
        else:
            return data

    def read(self, relative_path: str) -> Optional[ByteString]:
        """

        :param relative_path: relative to storage_dir
        :return:
        """
        assert relative_path
        if relative_path[0] == "/":
            relative_path = relative_path[1:]
        path = f"{self.storage_dir}/{relative_path}"
        return self._read(path)

    def write(self, relative_path: str, data):
        """

        :param relative_path: relative to storage_dir
        :param data:
        :return:
        """
        assert relative_path
        if relative_path[0] == "/":
            relative_path = relative_path[1:]
        path = f"{self.storage_dir}/{relative_path}"
        self._write(path, data)

    def write_file(self, relative_path, local_path):
        assert relative_path
        if relative_path[0] == "/":
            relative_path = relative_path[1:]
        path = f"{self.storage_dir}/{relative_path}"
        self._write_file(path, local_path)

    def set_progress(self, message: str):
        self.write("progress", message)

    def success(self, output_info: dict = None, output_file_info: dict = None):
        """

        :param Optional[dict] output_info:
            key:  path on {storage_dir}/output/*
            value: data(byte or str)
        :param Optional[dict] output_file_info:
            key:  path on {storage_dir}/output/*
            value: local file path
        :return:
        """
        self.output_info(output_info, output_file_info)
        self.write(f"status", STATUS_SUCCESS)
        self.finished = True

    def error(self, message: str, output_info: dict = None, output_file_info: dict = None):
        """

        :param str message: write to {storage_dir}/output/__error__.txt
        :param Optional[dict] output_info:
            key:  path on {storage_dir}/output/*
            value: data(byte or str)
        :param Optional[dict] output_file_info:
            key:  path on {storage_dir}/output/*
            value: local file path
        :return:
        """
        output_info = output_info or {}
        output_info['__error__.txt'] = message
        self.output_info(output_info, output_file_info)
        self.write(f"status", STATUS_ERROR)
        self.finished = True

    def output_info(self, output_info: dict = None, output_file_info: dict = None):
        """

        :param Optional[dict] output_info:
            key:  path on {storage_dir}/output/*
            value: data(byte or str)
        :param Optional[dict] output_file_info:
            key:  path on {storage_dir}/output/*
            value: local file path
        :return:
        """
        if output_info:
            for key, value in output_info.items():
                if value is not None:
                    self.write(f"output/{key}", value)
        if output_file_info:
            for key, local_path in output_file_info.items():
                with open(local_path, "r") as f:
                    self.write(f"output/{key}", f.read())

    def get_input_filenames(self) -> List[str]:
        """

        :return: return List of path relative to storage_dir
        """
        return self._get_input_filenames()

    def _setup(self):
        pass

    def _write(self, path, data):
        pass

    def _write_file(self, path, local_path):
        pass

    def _read(self, path: str) -> Optional[ByteString]:
        pass

    def _get_input_filenames(self):
        return []


class LocalADBIWorker(ADBIWorker):
    def _setup(self):
        os.makedirs(self.storage_dir, exist_ok=True)

    def _write(self, path: str, data):
        mode = "wt" if isinstance(data, str) else "wb"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, mode) as f:
            f.write(data)

    def _write_file(self, path, local_path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        shutil.copy(local_path, path)

    def _read(self, path: str):
        if os.path.exists(path):
            with open(path, "rb") as f:
                return f.read()

    def _get_input_filenames(self):
        input_dir = Path(f"{self.storage_dir}/input")
        return [str(x.relative_to(self.storage_dir)) for x in input_dir.glob("**/*")]


class S3ADBIWorker(ADBIWorker):
    client = None

    def _setup(self):
        self.client = get_s3_client()

    def _write(self, path: str, data):
        if isinstance(data, str):
            data = data.encode()
        with BytesIO(data) as fileobj:
            upload_fileobj_to_s3(self.client, fileobj, path)

    def _write_file(self, path, local_path):
        upload_file_to_s3(self.client, local_path, path)

    def _read(self, path: str):
        return download_as_data_from_s3(self.client, path)

    def _get_input_filenames(self):
        key_list = s3_util.list_paths(self.client, f"{self.storage_dir}/input/")
        ret = []
        _, base_key = split_bucket_and_key(self.storage_dir)
        base_key = "/" + base_key
        for key in key_list:
            relative_key = Path(key).relative_to(base_key)
            ret.append(str(relative_key))
        return ret
