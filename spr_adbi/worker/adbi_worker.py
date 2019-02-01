import json
import os
import sys
from logging import getLogger
from time import time
from traceback import format_exception
from typing import List, Optional, ByteString

from spr_adbi.common.adbi_io import ADBIIO, ADBIS3IO, ADBILocalIO
from spr_adbi.common_types import ProgressLog
from spr_adbi.const import STATUS_SUCCESS, STATUS_ERROR, PATH_STDIN, PATH_ARGS, PATH_PROGRESS, PATH_STATUS, \
    PATH_PROGRESS_LOG

logger = getLogger(__name__)


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
    return ADBIWorker(args)


class ADBIWorker:
    def __init__(self, args: List[str]):
        self.finished = False
        self.storage_dir = args[0]
        self.io_client: ADBIIO = None
        self._args = args[1:]
        self.progress_log: List[dict] = []

        if self.storage_dir.endswith("/"):
            self.storage_dir = self.storage_dir[:-1]
        self._setup()

    def _setup(self):
        if self.storage_dir.startswith("s3://"):
            self.io_client = ADBIS3IO(self.storage_dir)
        else:
            self.io_client = ADBILocalIO(self.storage_dir)

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
        logger.info(f"reading from {relative_path}")
        return self.io_client.read(relative_path)

    def write(self, relative_path: str, data):
        """

        :param relative_path: relative to storage_dir
        :param data:
        :return:
        """
        assert relative_path
        if relative_path[0] == "/":
            relative_path = relative_path[1:]
        logger.info(f"writing to {relative_path}")
        self.io_client.write(relative_path, data)

    def write_file(self, relative_path, local_path):
        assert relative_path
        if relative_path[0] == "/":
            relative_path = relative_path[1:]
        logger.info(f"writing {local_path} file to {relative_path}")
        self.io_client.write_file(relative_path, local_path)

    def set_progress(self, message: str):
        logger.info(f"progress: {message}")
        self.io_client.write(PATH_PROGRESS, message)
        self._append_progress_log(message)

    def _append_progress_log(self, message: str):
        self.progress_log.append(dict(time=time(), message=message))
        self.io_client.write(PATH_PROGRESS_LOG, json.dumps(self.progress_log, ensure_ascii=False))

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
        logger.info(f"success")
        self.output_info(output_info, output_file_info)
        self.io_client.write(PATH_STATUS, STATUS_SUCCESS)
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
        logger.info(f"error")
        output_info = output_info or {}
        output_info['__error__.txt'] = message
        self.output_info(output_info, output_file_info)
        self.io_client.write(PATH_STATUS, STATUS_ERROR)
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
                    self.io_client.write(f"output/{key}", value)
        if output_file_info:
            for key, local_path in output_file_info.items():
                with open(local_path, "rb") as f:
                    self.io_client.write(f"output/{key}", f.read())

    def get_input_filenames(self) -> List[str]:
        """

        :return: return List of path relative to storage_dir
        """
        return self.io_client.get_input_filenames()
