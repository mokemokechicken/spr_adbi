import json
import shutil
from pathlib import Path
from time import time

from pytest_mock import MockFixture

import spr_adbi.worker.adbi_worker as t
from spr_adbi.common.adbi_io import ADBIS3IO
from spr_adbi.common_types import ProgressLog
from spr_adbi.const import PATH_PROGRESS, PATH_PROGRESS_LOG

TMP_DIR = str((Path(__file__).parent.parent.parent / "tmp/test").absolute())
TP = Path(TMP_DIR)
WORKING_DIR = "s3://bucket/my/path"


def test_create_worker():
    obj = t.create_worker([WORKING_DIR])
    assert isinstance(obj, t.ADBIWorker)
    assert isinstance(obj.io_client, ADBIS3IO)
    assert obj.storage_dir == WORKING_DIR


class TestS3ADBIWorker:
    obj = None
    in_dir = None
    out_dir = None

    def setup_method(self, method):
        shutil.rmtree(TMP_DIR, ignore_errors=True)
        self.obj = t.create_worker([WORKING_DIR])
        self.in_dir = f"{WORKING_DIR}/input"
        self.out_dir = f"{WORKING_DIR}/output"

    def teardown_method(self, method):
        shutil.rmtree(TMP_DIR, ignore_errors=True)

    def test_get_input_filenames(self, mocker: MockFixture):
        files = ["/my/path/input/file1", "/my/path/input/xyz"]
        mocker.patch('spr_adbi.util.s3_util.list_paths', return_value=files)
        filenames = list(sorted(self.obj.get_input_filenames()))
        assert "input/file1" == filenames[0]
        assert "input/xyz" == filenames[1]

    def test_set_progress(self, mocker: MockFixture):
        self.obj.io_client = mocker.MagicMock()
        mocker.patch('time.time', return_value=888)

        msg = 'hello!'
        self.obj.set_progress(msg)
        self.obj.io_client.write.assert_any_call(PATH_PROGRESS, msg)
        self.obj.io_client.write.assert_any_call(PATH_PROGRESS_LOG, json.dumps([dict(time=888, message=msg)]))
        assert self.obj.progress_log == [dict(time=888, message=msg)]
