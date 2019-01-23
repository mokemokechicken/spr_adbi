import os
import sys

from spr_adbi.common.container import AWSContainerManager
from spr_adbi.common.resolver import WorkerInfo


def main(container_id, base_uri, entry_point):
    wi = WorkerInfo(container_id, entry_point)
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
    print(stdout)
    print(stderr)


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2], sys.argv[3:])
