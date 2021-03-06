import os
from encodings.base64_codec import base64_decode
from logging import getLogger

import docker
from docker import DockerClient

from spr_adbi.dispatcher.resolver import WorkerInfo
from spr_adbi.const import ENV_KEY_ECR_ACCOUNT_IDS
from spr_adbi.util.s3_util import create_boto3_session_of_assume_role_delayed

logger = getLogger(__name__)


class ContainerManager:
    def __init__(self, worker_info: WorkerInfo, base_uri: str):
        """

        :param worker_info:
        :param base_uri:
        """
        self.worker_info = worker_info
        self.base_uri = base_uri
        self.setup()

    def setup(self):
        pass

    def login_container_registry(self):
        pass

    def pull_container(self):
        pass

    def run_container(self, runtime_config=None):
        """

        :param dict runtime_config: kwargs of
            https://github.com/docker/docker-py/blob/master/docker/models/containers.py#L506
        :return: (success:bool, stdout, stderr)
        """
        raise NotImplemented()


class AWSContainerManager(ContainerManager):
    session = None
    ecr_client = None
    docker_client: DockerClient = None

    def __init__(self, worker_info: WorkerInfo, base_uri: str, region_name=None):
        self.region_name = region_name or os.environ.get("AWS_REGION")
        super().__init__(worker_info, base_uri)

    def setup(self):
        super().setup()
        self.session = create_boto3_session_of_assume_role_delayed(region_name=self.region_name)
        self.ecr_client = self.session.client("ecr", region_name=self.region_name)

    def login_container_registry(self):
        logger.info("logging in docker registry")
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecr.html#ECR.Client.get_authorization_token
        # registry_ids = os.environ.get(ENV_KEY_ECR_ACCOUNT_IDS, "").split(",")
        image_account_id = self.worker_info.image_id.split(".")[0]
        logger.info(f"account_id: {image_account_id}")
        response = self.ecr_client.get_authorization_token(registryIds=[image_account_id])
        token = response.get('authorizationData')[0].get('authorizationToken')
        id_pass, _ = base64_decode(token.encode())
        user_name, password = id_pass.decode().split(":")

        # docker.login : https://docker-py.readthedocs.io/en/stable/client.html
        self.docker_client = docker.from_env()
        registry_url = 'https://{account}.dkr.ecr.{region_name}.amazonaws.com/'.format(account=image_account_id,
                                                                                       region_name=self.region_name)
        self.docker_client.login(username=user_name, password=password, registry=registry_url)

    def pull_container(self):
        logger.info(f"pulling docker container {self.worker_info.image_id}")
        self.docker_client.images.pull(self.worker_info.image_id)

    def run_container(self, runtime_config=None):
        """

        :param dict runtime_config: kwargs of
            https://github.com/docker/docker-py/blob/master/docker/models/containers.py#L506
        :return: (success:bool, stdout, stderr)
        """
        runtime_config = runtime_config or {}
        commands = self.worker_info.entry_point + [self.base_uri]
        logger.info(f"run container: {self.worker_info.image_id} {commands} {runtime_config}")
        try:
            ret = self.docker_client.containers.run(self.worker_info.image_id, commands, stdout=True, stderr=True,
                                                    remove=True, **runtime_config)
            return True, ret, None
        except Exception as e:
            return False, None, str(e)
