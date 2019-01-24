import os
import sys
from logging import basicConfig, INFO

from spr_adbi.dispatcher.container import AWSContainerManager
from spr_adbi.dispatcher.resolver import WorkerInfo


def main(image_id, base_uri, entry_point):
    basicConfig(level=INFO)

    wi = WorkerInfo(image_id, entry_point)
    manager = AWSContainerManager(wi, base_uri)
    manager.login_container_registry()
    manager.pull_container()

    environment = {}
    for k, v in os.environ.items():
        if k.startswith("AWS_"):
            environment[k] = v
    success, stdout, stderr = manager.run_container(runtime_config=dict(
        environment=environment,
    ))
    print(success)
    if stdout:
        print(stdout.decode())
    if stderr:
        print(stderr.decode())


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3:])
