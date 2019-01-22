from typing import Optional, List


class WorkerInfo:
    container_id: str
    entry_point: List[str]

    def __init__(self, container_id, entry_point):
        self.container_id = container_id
        self.entry_point = entry_point


class WorkerResolver:
    def resolve(self, func_id) -> Optional[WorkerInfo]:
        pass

