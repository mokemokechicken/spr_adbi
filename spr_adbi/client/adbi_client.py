import json
import os
from collections import defaultdict
from datetime import datetime
from logging import getLogger
from time import time, sleep
from typing import List, Optional, Union, Iterable, Tuple, Callable
from uuid import uuid4

from spr_adbi.client.job_event import ADBIJobEventChangeStatus, ADBIJobEventChangeProgress, ADBIJobEvent
from spr_adbi.common.adbi_io import ADBIIO, ADBIS3IO
from spr_adbi.common_types import ProgressLog
from spr_adbi.const import ENV_KEY_ADBI_BASE_DIR, PATH_ARGS, PATH_STDIN, PATH_INPUT_FILES, ENV_KEY_SQS_NAME, \
    PATH_STATUS, PATH_PROGRESS, STATUS_SUCCESS, STATUS_ERROR, PATH_PROGRESS_LOG
from spr_adbi.util.datetime_util import JST
from spr_adbi.util.s3_util import create_boto3_session_of_assume_role_delayed

logger = getLogger(__name__)


def create_client(env: dict = None):
    """

    :type env: dict
    :rtype: ADBIClient
    """
    env = env or {}
    env_dict = dict(os.environ)
    env_dict.update(env)

    errors = []
    if ENV_KEY_SQS_NAME not in env_dict:
        errors.append(f'Please Specify SQS name by {ENV_KEY_SQS_NAME} Env Variable.')
    if ENV_KEY_ADBI_BASE_DIR not in env_dict:
        errors.append(f'Please Specify Base S3 Dir by {ENV_KEY_ADBI_BASE_DIR} Env Variable.')
    if errors:
        raise RuntimeError("\n\t" + "\n\t".join(errors))

    base_dir = env_dict.get(ENV_KEY_ADBI_BASE_DIR)
    return ADBIClient(base_dir, **env_dict)


class ADBIClient:
    def __init__(self, env_base_dir: str, **kwargs):
        assert env_base_dir.startswith("s3://")
        self.env_base_dir = env_base_dir
        self.options = kwargs
        self.io_client: ADBIIO = None
        self._aws_session = None

        if self.env_base_dir.endswith("/"):
            self.env_base_dir = self.env_base_dir[:-1]

        self._setup()

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

        process_id = self._create_process_id(func_id)
        self._prepare_writer(process_id)
        self._write_input_data(args, stdin, input_info, input_file_info)
        message = json.dumps([func_id, self.io_client.base_dir])

        queue = self._prepare_queue_client()
        response = queue.send_message(MessageBody=message, MessageGroupId=process_id,
                                      MessageDeduplicationId=process_id)
        return ADBIJob(base_dir=self.io_client.base_dir,
                       io_client=self.io_client,
                       queue_name=self.queue_name,
                       queue_message_id=response.get('MessageId'))

    def _setup(self):
        pass

    @property
    def aws_session(self):
        if self._aws_session is None:
            self._aws_session = create_boto3_session_of_assume_role_delayed()
        return self._aws_session

    def _prepare_queue_client(self):
        return self.aws_session.resource('sqs').get_queue_by_name(QueueName=self.queue_name)

    @property
    def queue_name(self):
        return self.options[ENV_KEY_SQS_NAME]

    def _prepare_writer(self, process_id):
        target_dir = f"{self.env_base_dir}/{process_id}"
        self.io_client = ADBIS3IO(target_dir)

    def _write_input_data(self, args: Iterable[str], stdin, input_file: dict, input_file_info: dict):
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
    def _create_process_id(func_id) -> str:
        time_str = datetime.now(tz=JST).strftime('%Y%m%d.%H%M%S.JST')
        random_str = uuid4().hex
        return f"{time_str}-{func_id}-{random_str}"


class ADBIJob:
    def __init__(self, base_dir, io_client, queue_name=None, queue_message_id=None):
        self.base_dir: str = base_dir
        self.io_client: ADBIIO = io_client
        self.queue_name: Optional[str] = queue_name
        self.queue_message_id: Optional[str] = queue_message_id
        self._finished = False
        self._final_status = None
        self._last_status = None
        self._event_listeners = defaultdict(lambda: [])

    def get_status(self) -> Optional[str]:
        status = self.io_client.read(PATH_STATUS)
        if status is not None:
            self._last_status = status.decode().strip()
        return self._last_status

    def get_progress(self) -> Optional[str]:
        progress = self.io_client.read(PATH_PROGRESS)
        if progress is not None:
            return progress.decode().strip()

    def get_progress_log(self) -> List[ProgressLog]:
        ret = []
        progress_log = self.io_client.read(PATH_PROGRESS_LOG)

        # noinspection PyBroadException
        try:
            if progress_log:
                for log in json.loads(progress_log.decode(), encoding='utf8'):
                    ret.append(ProgressLog(log.get('time'), log.get('message')))
        except Exception:
            pass

        return ret

    @property
    def s3_uri(self):
        return self.io_client.base_dir

    @property
    def finished(self) -> bool:
        if not self._finished:
            status = self.get_status()
            logger.info(f"check finished: status={status}")

            self._finished = status in (STATUS_SUCCESS, STATUS_ERROR)
            if self._finished:
                self._final_status = status
        return self._finished

    def is_success(self) -> bool:
        return self.finished and self._final_status == STATUS_SUCCESS

    def is_error(self) -> bool:
        return self.finished and self._final_status == STATUS_ERROR

    def wait(self, timeout=3600, raise_if_timeout=True, polling_interval=3) -> Optional[bool]:
        start_time = time()
        last_status = None
        last_progress = None
        while time() - start_time < timeout:
            if self.finished:
                return self.is_success()

            if self._last_status != last_status:
                last_status = self._last_status
                self._emit(ADBIJobEventChangeStatus(self, last_status))

            progress = self.get_progress()
            if progress != last_progress:
                last_progress = progress
                self._emit(ADBIJobEventChangeProgress(self, progress))

            sleep(polling_interval)

        if raise_if_timeout:
            raise ADBITimeout()
        else:
            return None

    def on(self, event_name: str, function: Callable):
        self._event_listeners[event_name].append(function)

    def _emit(self, event: ADBIJobEvent):
        for function in self._event_listeners[event.event_name]:
            try:
                function(event)
            except Exception as e:
                logger.warning(f"exception in ADBIJobEvent handler {e}")

    def get_output(self):
        """

        :rtype: ADBIOutput
        """
        return ADBIOutput(self.io_client)


class ADBITimeout(Exception):
    pass


class ADBIOutput:
    def __init__(self, io_client: ADBIIO):
        self.io_client = io_client

    def get_filenames(self) -> List[str]:
        return self.io_client.get_output_filenames()

    def get_file_content(self, filename) -> Optional[bytes]:
        return self.io_client.read(filename)
