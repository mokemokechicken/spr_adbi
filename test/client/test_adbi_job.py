from pytest_mock import MockFixture

from spr_adbi.client.adbi_client import ADBIJob
from spr_adbi.common_types import ProgressLog
from spr_adbi.const import PATH_PROGRESS_LOG


def test_adbi_job_get_progress_log(mocker: MockFixture):
    io_client = mocker.MagicMock()
    io_client.read.return_value = '[{"time": 999, "message": "hello"}]'.encode()
    job = ADBIJob('s3://dummy/io/dir', io_client)
    ret = job.get_progress_log()
    assert ret[0] == ProgressLog(999, "hello")
    io_client.read.assert_called_with(PATH_PROGRESS_LOG)
