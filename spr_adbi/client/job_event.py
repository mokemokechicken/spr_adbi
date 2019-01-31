from time import time


class ADBIJobEvent:
    event_name: str = None

    def __init__(self):
        self.event_time = time()


class ADBIJobEventChangeStatus(ADBIJobEvent):
    event_name = 'change_status'

    def __init__(self, status):
        super().__init__()
        self.status = status


class ADBIJobEventChangeProgress(ADBIJobEvent):
    event_name = 'change_progress'

    def __init__(self, progress):
        super().__init__()
        self.progress = progress
