import os
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Union

from spr_adbi.const import ENV_KEY_ADBI_BASE_DIR
from spr_adbi.util import s3_util
from spr_adbi.util.s3_util import get_s3_client, upload_fileobj_to_s3, upload_file_to_s3, download_as_data_from_s3, \
    split_bucket_and_key


def create_client(env: dict = None):
    """

    :type env: dict
    :rtype: ADBIClient
    """
    env = env or {}
    base_dir = env.get(ENV_KEY_ADBI_BASE_DIR) or os.environ.get(ENV_KEY_ADBI_BASE_DIR) or "tmp"
    return ADBIClient(base_dir, **env)


class ADBIClient:
    def __init__(self, base_dir: str, **kwargs):
        if base_dir.startswith("s3://"):
            self.writer = ADBIClientS3IO(base_dir)
        else:
            self.writer = ADBIClientLocalIO(base_dir)
        self.options = kwargs
        self._setup()

    def _setup(self):
        pass

    def request(self, func_id, args: List[str] = None, stdin: Optional[Union[bytes, str]] = None,
                input_info: dict = None, input_file_info: dict = None, max_retry=None):
        """

        :param func_id:
        :param args:
        :param stdin:
        :param input_info:
        :param input_file_info:
        :param max_retry:
        :rtype: ADBIJob
        """
        pass


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
