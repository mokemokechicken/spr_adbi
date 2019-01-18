import json
import os
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Union, Iterable, Tuple
from uuid import uuid4

from spr_adbi.const import ENV_KEY_ADBI_BASE_DIR, PATH_ARGS, PATH_STDIN, PATH_INPUT_FILES
from spr_adbi.util import s3_util
from spr_adbi.util.datetime_util import JST
from spr_adbi.util.s3_util import get_s3_client, upload_fileobj_to_s3, upload_file_to_s3, download_as_data_from_s3, \
    split_bucket_and_key


def create_client(env: dict = None):
    """

    :type env: dict
    :rtype: ADBIClient
    """
    env = env or {}
    env_dict = dict(os.environ)
    env_dict.update(env)
    base_dir = env_dict.get(ENV_KEY_ADBI_BASE_DIR) or "tmp"
    return ADBIClient(base_dir, **env_dict)


class ADBIClient:
    def __init__(self, base_dir: str, **kwargs):
        self.base_dir = base_dir
        self.options = kwargs
        self.io_client: ADBIClientIO = None
        self._setup()

    def _setup(self):
        pass

    def prepare_writer(self, process_id):
        target_dir = f"{self.base_dir}/{process_id}"
        if target_dir.startswith("s3://"):
            self.io_client = ADBIClientS3IO(target_dir)
        else:
            self.io_client = ADBIClientLocalIO(target_dir)

    def request(self, func_id, args: Optional[Union[List, Tuple]] = None, stdin: Optional[Union[bytes, str]] = None,
                input_info: dict = None, input_file_info: dict = None, max_retry=None):
        """

        :param func_id:
        :param args:
        :param stdin:
        :param input_info: 'input/files' 以下に書き込む key が相対PATH, value がデータ
        :param input_file_info: 'input/files' 以下に書き込む key が相対PATH, value が Local File Path
        :param max_retry:
        :rtype: ADBIJob
        """
        assert isinstance(func_id, str)
        assert args is None or isinstance(args, (list, tuple))
        assert stdin is None or isinstance(stdin, (bytes, str))
        assert input_info is None or isinstance(input_info, dict)
        assert input_file_info is None or isinstance(input_file_info, dict)

        process_id = self.create_process_id(func_id)
        self.prepare_writer(process_id)
        self.write_input_data(args, stdin, input_info, input_file_info)

    def write_input_data(self, args: Iterable[str], stdin, input_file: dict, input_file_info: dict):
        if args:
            self.io_client.write(PATH_ARGS, json.dumps(args, ensure_ascii=False))
        if stdin:
            self.io_client.write(PATH_STDIN, stdin)

        if input_file:
            for key, data in input_file.items():
                if data is not None:
                    assert isinstance(data, (bytes, str))
                    self.io_client.write(f"{PATH_INPUT_FILES}/{key}", data)

        if input_file_info:
            for key, path in input_file_info.items():
                self.io_client.write_file(f"{PATH_INPUT_FILES}/{key}", path)

    @staticmethod
    def create_process_id(func_id) -> str:
        time_str = datetime.now(tz=JST).strftime('%Y%m%d.%H%M%S.JST')
        random_str = uuid4().hex
        return f"{time_str}-{func_id}-{random_str}"


class ADBIClientIO:
    def __init__(self, base_dir):
        self.base_dir = base_dir
        self._setup()

    def _setup(self):
        pass

    def write(self, path, data: Union[str, bytes]):
        assert isinstance(data, (str, bytes))
        if isinstance(data, str):
            data = data.encode()
        self._write(path, data)

    def write_file(self, path, local_path):
        self._write_file(path, local_path)

    def read(self, path) -> bytes:
        return self._read(path)

    def get_output_filenames(self) -> List[str]:
        return self._get_output_filenames()

    def _write(self, path, data: bytes):
        raise NotImplemented()

    def _write_file(self, path, local_path):
        raise NotImplemented()

    def _read(self, path) -> bytes:
        raise NotImplemented()

    def _get_output_filenames(self) -> List[str]:
        raise NotImplemented()


class ADBIClientLocalIO(ADBIClientIO):
    def _write(self, path, data: bytes):
        path = Path(self.base_dir) / path
        os.makedirs(path.parent, exist_ok=True)
        with path.open("wb") as f:
            f.write(data)

    def _write_file(self, path, local_path):
        with open(local_path, "rb") as f:
            self.write(path, f.read())

    def _read(self, path) -> bytes:
        path = Path(self.base_dir) / path
        with path.open("rb") as f:
            return f.read()

    def _get_output_filenames(self) -> List[str]:
        input_dir = Path(f"{self.base_dir}/output")
        return [str(x.relative_to(self.base_dir)) for x in input_dir.glob("**/*")]


class ADBIClientS3IO(ADBIClientIO):
    client = None

    def _setup(self):
        self.client = get_s3_client()

    def _write(self, path: str, data: bytes):
        path = f'{self.base_dir}/{path}'
        with BytesIO(data) as f:
            upload_fileobj_to_s3(self.client, f, path)

    def _write_file(self, path, local_path):
        upload_file_to_s3(self.client, local_path, path)

    def _read(self, path: str) -> bytes:
        path = f'{self.base_dir}/{path}'
        return download_as_data_from_s3(self.client, path)

    def _get_output_filenames(self) -> List[str]:
        key_list = s3_util.list_paths(self.client, f"{self.base_dir}/output/")
        ret = []
        _, base_key = split_bucket_and_key(self.base_dir)
        base_key = "/" + base_key
        for key in key_list:
            relative_key = Path(key).relative_to(base_key)
            ret.append(str(relative_key))
        return ret


class ADBIJob:
    pass


class ADBIOutput:
    pass
