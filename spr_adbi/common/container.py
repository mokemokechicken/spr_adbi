import os
from encodings.base64_codec import base64_decode
from logging import getLogger

import docker
from docker import DockerClient

from spr_adbi.common.resolver import WorkerInfo
from spr_adbi.const import ENV_KEY_ECR_ACCOUNT_IDS
from spr_adbi.util.s3_util import create_boto3_session_of_assume_role_delayed

logger = getLogger(__name__)


class ContainerManager:
    def __init__(self, worker_info: WorkerInfo, base_uri: str):
        self.worker_info = worker_info
        self.base_uri = base_uri
        self.setup()

    def setup(self):
        pass

    def login_container_registry(self):
        pass

    def pull_container(self):
        pass

    def run_container(self):
        """

        :return: (success:bool, stdout, stdin)
        """
        raise NotImplemented()


class AWSContainerManager(ContainerManager):
    session = None
    ecr_client = None
    docker_client: DockerClient = None

    def setup(self):
        super().setup()
        self.session = create_boto3_session_of_assume_role_delayed()
        self.ecr_client = self.session.client("ecr")
        # print(self.ecr_client.meta.config)

    @property
    def region_name(self):
        return self.ecr_client.meta.region_name

    def login_container_registry(self):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecr.html#ECR.Client.get_authorization_token
        registry_ids = os.environ.get(ENV_KEY_ECR_ACCOUNT_IDS, "").split(",")
        response = self.ecr_client.get_authorization_token(registryIds=registry_ids)
        token = response.get('authorizationData')[0].get('authorizationToken')
        id_pass, _ = base64_decode(token.encode())
        user_name, password = id_pass.decode().split(":")

        # docker.login : https://docker-py.readthedocs.io/en/stable/client.html
        self.docker_client = docker.from_env()
        registry_url = 'https://{account}.dkr.ecr.{region_name}.amazonaws.com/'.format(account=registry_ids[0],
                                                                                       region_name=self.region_name)
        self.docker_client.login(username=user_name, password=password, registry=registry_url)
