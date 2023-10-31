from datetime import datetime
from logging import getLogger

from spr_adbi.common.adbi_io import ADBIS3IO
from spr_adbi.dispatcher.container import ContainerManager, AWSContainerManager
from spr_adbi.dispatcher.resolver import WorkerInfo
from spr_adbi.const import PATH_STATUS, STATUS_ERROR, PATH_PROGRESS, STATUS_RUNNING
from spr_adbi.util.datetime_util import JST


logger = getLogger(__name__)


class WorkerManager:
    def __init__(self, worker_info: WorkerInfo, base_uri: str, region_name=None):
        self.worker_info = worker_info
        self.base_uri = base_uri
        self.io_client = self.create_io_client(base_uri, region_name)
        self.container_manager = self.create_container_manager(worker_info, base_uri, region_name)

    def create_io_client(self, base_uri, region_name):
        return ADBIS3IO(base_uri, region_name)

    def create_container_manager(self, worker_info, base_uri, region_name) -> ContainerManager:
        return AWSContainerManager(worker_info, base_uri, region_name=region_name)

    def set_status(self, value):
        logger.info(f"set status to {value}")
        self.io_client.write(PATH_STATUS, str(value))

    def run(self, max_retry=1):
        success = False

        try:
            self.container_manager.login_container_registry()
            self.container_manager.pull_container()
        except Exception as e:
            logger.error(f"fail to fetch container {e}", stack_info=True)
            self.set_status(STATUS_ERROR)
            return False

        for retry_idx in range(1, max_retry+1):
            try:
                if retry_idx > 1:
                    logger.info(f"retry worker(try={retry_idx})")
                self.cleanup_workspace()
                success = self.start_worker(retry_idx)
            except Exception as e:
                logger.warning(f"Error Happen in running worker: {e}", stack_info=True)

            if success:
                logger.info(f"success to process {self.base_uri}")
                return True

            self.set_status(STATUS_ERROR)
        logger.warning(f"fail to process {self.base_uri}")

    def cleanup_workspace(self):
        logger.info("cleanup workspace")
        filenames = self.io_client.get_filenames()
        for filename in filenames:
            if filename == PATH_PROGRESS:
                self.io_client.delete(filename)
            elif filename.startswith("output/"):
                self.io_client.delete(filename)

    def start_worker(self, retry_idx: int) -> bool:
        logger.info("start worker")
        log_dir = f"run-{retry_idx}"
        self.io_client.write(f"{log_dir}/start_time", datetime.now(tz=JST).isoformat())
        self.set_status(STATUS_RUNNING)

        success = stdout = stderr = None
        try:
            success, stdout, stderr = self.container_manager.run_container(self.worker_info.runtime_config)
        except Exception as e:
            logger.warning(f"error in running container: {e}", stack_info=True)

        if stderr:
            logger.warning(stderr)

        self.io_client.write(f"{log_dir}/stdout", stdout)
        self.io_client.write(f"{log_dir}/stderr", stderr)
        self.io_client.write(f"{log_dir}/end_time", datetime.now(tz=JST).isoformat())
        self.io_client.write(f"{log_dir}/status", self.io_client.read(PATH_STATUS))

        return success
