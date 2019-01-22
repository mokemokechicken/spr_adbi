import json
import os
from collections import namedtuple
from logging import getLogger
from time import sleep
from typing import Optional

from spr_adbi.common.resolver import WorkerResolver, WorkerInfo
from spr_adbi.const import ENV_KEY_SQS_NAME
from spr_adbi.util.s3_util import create_boto3_session_of_assume_role_delayed

logger = getLogger(__name__)
QueueMessage = namedtuple('QueueMessage', 'message func_id s3_uri')


def create_dispatcher(resolver: WorkerResolver, env: dict = None):
    env = env or {}
    env_dict = dict(os.environ)
    env_dict.update(env)

    errors = []
    if ENV_KEY_SQS_NAME not in env_dict:
        errors.append(f'Please Specify SQS name by {ENV_KEY_SQS_NAME} Env Variable.')
    if errors:
        raise RuntimeError("\n\t" + "\n\t".join(errors))

    return ADBIDispatcher(resolver, env_dict)


class ADBIDispatcher:
    def __init__(self, resolver: WorkerResolver, env: dict):
        self.env = env
        self.resolver = resolver
        self._aws_session = None
        self._queue = None

    @property
    def aws_session(self):
        if self._aws_session is None:
            self._aws_session = create_boto3_session_of_assume_role_delayed()
        return self._aws_session

    @property
    def queue(self):
        if self._queue is None:
            self._queue = self.aws_session.resource('sqs').get_queue_by_name(QueueName=self.queue_name)
        return self._queue

    @property
    def queue_name(self):
        return self.env[ENV_KEY_SQS_NAME]

    def watch(self):
        while True:
            message = self.fetch_message()
            worker_info = self.resolver.resolve(message.func_id)

            if worker_info:
                self.handle_message(message, worker_info)
            else:
                logger.info(f"can not handle func_id {message.func_id}")
                message.message.change_visibility(VisibilityTimeout=0)
                sleep(5)

    def fetch_message(self):
        while True:
            messages = self.queue.receive_messages()
            if not messages:
                continue
            msg = messages[0]
            message_body = json.loads(msg.body())
            if not isinstance(message_body, list) or len(message_body) != 2:
                logger.warning(f'illegal message: {message_body}')
                msg.delete()
                continue
            return QueueMessage(msg, message_body[0], message_body[1])

    def handle_message(self, message: QueueMessage, worker_info: WorkerInfo):
        pass











