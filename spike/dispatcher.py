import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from logging import basicConfig, INFO

from spr_adbi.client.adbi_client import create_client
from spr_adbi.common.resolver import WorkerResolver, WorkerInfo
from spr_adbi.dispatcher.adbi_dispatcher import create_dispatcher, WorkerManager


def main(image_id, entry_point):
    """
    ## require args
    image_id: str
    entry_point: List

    ## require env vars

    - ADBI_BASE_DIR
    - ADBI_SQS_NAME
    - ADBI_ECR_ACCOUNT_IDS
    """
    basicConfig(level=INFO)
    # executor = ThreadPoolExecutor(max_workers=1)
    # executor.submit(run_dispatcher, image_id, entry_point)

    jobs = [push_request(i) for i in range(20)]
    executor = ThreadPoolExecutor(max_workers=1)
    executor.submit(watch_jobs, jobs)

    run_dispatcher(image_id, entry_point)


def run_dispatcher(image_id, entry_point):
    runtime_config = dict(
        environment=export_env_vars(),
    )
    resolver = ConstantWorkerResolver(image_id, entry_point, runtime_config)
    dispatcher = create_dispatcher(resolver, WorkerManager)
    dispatcher.watch()


def push_request(i):
    client = create_client()
    job = client.request('test.echo', ["hello", str(i), str(datetime.now())])
    return job


def watch_jobs(jobs):
    for job in jobs:
        watch_job(job)


def watch_job(job):
    is_success = job.wait()
    if not is_success:
        print("finish error")
    else:
        print("finish success")
        output = job.get_output()
        filenames = output.get_filenames()
        print(filenames)
        for filename in filenames:
            print(filename)
            print(output.get_file_content(filename))
            print()


class ConstantWorkerResolver(WorkerResolver):
    def __init__(self, image_id, entry_point, default_runtime_config=None):
        self.image_id = image_id
        self.entry_point = entry_point
        self.default_runtime_config = default_runtime_config or {}

    def resolve(self, func_id):
        return WorkerInfo(self.image_id, self.entry_point, self.default_runtime_config)


def export_env_vars():
    environment = {}
    for k, v in os.environ.items():
        if k.startswith("AWS_"):
            environment[k] = v
    return environment


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2:])
