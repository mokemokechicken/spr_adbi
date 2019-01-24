from typing import Optional, List


class WorkerInfo:
    image_id: str
    entry_point: List[str]
    runtime_config: Optional[dict]

    def __init__(self, image_id, entry_point, runtime_config=None):
        self.image_id = image_id
        self.entry_point = entry_point
        self.runtime_config = runtime_config


class WorkerResolver:
    def resolve(self, func_id) -> Optional[WorkerInfo]:
        pass

