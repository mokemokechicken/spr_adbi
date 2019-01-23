from typing import Optional, List


class WorkerInfo:
    image_id: str
    entry_point: List[str]

    def __init__(self, image_id, entry_point):
        self.image_id = image_id
        self.entry_point = entry_point


class WorkerResolver:
    def resolve(self, func_id) -> Optional[WorkerInfo]:
        pass

