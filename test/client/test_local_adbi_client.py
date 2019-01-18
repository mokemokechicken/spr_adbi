import json
import shutil
from pathlib import Path

import spr_adbi.client.adbi_client as t
from spr_adbi.const import ENV_KEY_ADBI_BASE_DIR

TMP_DIR = str((Path(__file__).parent.parent.parent / "tmp/test").absolute())
WORKING_DIR = TMP_DIR + "/working"
TP = Path(TMP_DIR)
WP = Path(WORKING_DIR)


def test_create_local_client():
    obj = t.create_client({ENV_KEY_ADBI_BASE_DIR: '/path/to/base_dir'})
    assert isinstance(obj, t.ADBIClient)
    assert obj.base_dir == '/path/to/base_dir'

    obj.prepare_writer("pid")
    assert isinstance(obj.io_client, t.ADBIClientLocalIO)
    assert obj.io_client.base_dir == '/path/to/base_dir/pid'


class TestADBIClientLocal:
    def setup_method(self, method):
        shutil.rmtree(TMP_DIR, ignore_errors=True)
        self.obj = t.create_client({ENV_KEY_ADBI_BASE_DIR: WORKING_DIR})
        self.in_dir = WP / "input"
        self.out_dir = WP / "output"

    def teardown_method(self, method):
        shutil.rmtree(TMP_DIR, ignore_errors=True)

