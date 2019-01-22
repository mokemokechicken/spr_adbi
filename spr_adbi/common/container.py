from spr_adbi.common.resolver import WorkerInfo


class ContainerManager:
    def __init__(self, worker_info: WorkerInfo, base_uri: str):
        self.worker_info = worker_info
        self.base_uri = base_uri

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
    def login_container_registry(self):
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/ecr.html#ECR.Client.get_authorization_token
        # docker.login : https://docker-py.readthedocs.io/en/stable/client.html
        pass
