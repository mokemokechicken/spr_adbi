import sys

from spr_adbi.common.container import AWSContainerManager
from spr_adbi.common.resolver import WorkerInfo


def main(container_id, entry_point):
    wi = WorkerInfo(container_id, entry_point)
    manager = AWSContainerManager(wi, "s3://bucket/")
    manager.login_container_registry()


if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2:])
