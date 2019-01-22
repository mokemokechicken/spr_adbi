from io import BytesIO
from pathlib import Path
from typing import Union, Optional, List

from botocore.exceptions import ClientError

from spr_adbi.util import s3_util
from spr_adbi.util.s3_util import get_s3_client, upload_fileobj_to_s3, upload_file_to_s3, download_as_data_from_s3, \
    split_bucket_and_key, delete_file_on_s3


class ADBIIO:
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

    def read(self, path) -> Optional[bytes]:
        return self._read(path)

    def delete(self, path):
        return self._delete(path)

    def get_filenames(self) -> List[str]:
        return self._get_filenames()

    def get_output_filenames(self) -> List[str]:
        return self._get_output_filenames()

    def _write(self, path, data: bytes):
        raise NotImplemented()

    def _write_file(self, path, local_path):
        raise NotImplemented()

    def _read(self, path) -> bytes:
        raise NotImplemented()

    def _delete(self, path):
        raise NotImplemented()

    def _get_output_filenames(self) -> List[str]:
        raise NotImplemented()

    def _get_filenames(self) -> List[str]:
        raise NotImplemented()


class ADBIS3IO(ADBIIO):
    client = None

    def _setup(self):
        self.client = get_s3_client()

    def _write(self, path: str, data: bytes):
        path = f'{self.base_dir}/{path}'
        with BytesIO(data) as f:
            upload_fileobj_to_s3(self.client, f, path)

    def _write_file(self, path, local_path):
        upload_file_to_s3(self.client, local_path, path)

    def _read(self, path: str) -> Optional[bytes]:
        path = f'{self.base_dir}/{path}'
        try:
            return download_as_data_from_s3(self.client, path)
        except ClientError as e:
            if str(e.response.get('Error', {}).get('Code')) == '404':
                return None
            raise e

    def _delete(self, path: str):
        path = f'{self.base_dir}/{path}'
        delete_file_on_s3(self.client, path)

    def _get_filenames(self):
        key_list = s3_util.list_paths(self.client, f"{self.base_dir}")
        ret = []
        _, base_key = split_bucket_and_key(self.base_dir)
        base_key = "/" + base_key
        for key in key_list:
            relative_key = Path(key).relative_to(base_key)
            ret.append(str(relative_key))
        return ret

    def _get_output_filenames(self) -> List[str]:
        filenames = self._get_filenames()
        return list(filter(lambda x: x.startswith("output/"), filenames))
