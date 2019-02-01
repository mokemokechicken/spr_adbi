from time import time


class ADBIJobEvent:
    event_name: str = None

    def __init__(self, job):
        """

        :param job: spr_adbi.client.adbi_client.ADBIJob
        """
        self.event_time = time()
        self.job = job


class ADBIJobEventChangeStatus(ADBIJobEvent):
    event_name = 'change_status'

    def __init__(self, job, status):
        super().__init__(job)
        self.status = status


class ADBIJobEventChangeProgress(ADBIJobEvent):
    event_name = 'change_progress'

    def __init__(self, job, progress):
        super().__init__(job)
        self.progress = progress
