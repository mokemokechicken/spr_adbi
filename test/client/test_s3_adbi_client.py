import json
import shutil
from pathlib import Path

import spr_adbi.client.adbi_client as t
from spr_adbi.const import ENV_KEY_ADBI_BASE_DIR

WORKING_DIR = 's3://my_bucket/adbi'


def test_create_s3_client():
    obj = t.create_client({ENV_KEY_ADBI_BASE_DIR: WORKING_DIR})
    assert isinstance(obj, t.ADBIClient)
    assert obj.writer.base_dir == WORKING_DIR
    assert isinstance(obj.writer, t.ADBIClientS3IO)


class TestADBIClientS3:
    def setup_method(self, method):
        self.obj = t.create_client({ENV_KEY_ADBI_BASE_DIR: WORKING_DIR})
        self.in_dir = f"{WORKING_DIR}/input"
        self.out_dir = f"{WORKING_DIR}/output"

    def teardown_method(self, method):
        pass
