import json
import shutil
from pathlib import Path

import spr_adbi.client.adbi_client as t
from spr_adbi.const import ENV_KEY_ADBI_BASE_DIR, ENV_KEY_SQS_NAME

WORKING_DIR = 's3://my_bucket/adbi'
SQS_NAME = 'test-adbi.fifo'


def create_client():
    obj = t.create_client({ENV_KEY_ADBI_BASE_DIR: WORKING_DIR, ENV_KEY_SQS_NAME: SQS_NAME})
    return obj


def test_create_s3_client():
    obj = create_client()
    assert isinstance(obj, t.ADBIClient)
    assert obj.base_dir == WORKING_DIR

    obj._prepare_writer("pid")
    assert isinstance(obj.io_client, t.ADBIClientS3IO)
    assert obj.io_client.base_dir == f"{WORKING_DIR}/pid"


class TestADBIClientS3:
    def setup_method(self, method):
        self.obj = create_client()
        self.in_dir = f"{WORKING_DIR}/input"
        self.out_dir = f"{WORKING_DIR}/output"

    def teardown_method(self, method):
        pass
