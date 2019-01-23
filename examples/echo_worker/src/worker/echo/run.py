import json
from logging import basicConfig, INFO

from spr_adbi.worker.adbi_worker import create_worker, ADBIWorker


def start():
    #getLogger().addHandler(StreamHandler())
    basicConfig(level=INFO)
    with create_worker() as w:
        EchoWorker(w).start()


class EchoWorker:
    def __init__(self, worker: ADBIWorker):
        self.worker = worker

    def start(self):
        output_info = {
            "args": json.dumps(self.worker.args()),
            "stdin": self.worker.stdin(),
        }
        for in_filename in self.worker.get_input_filenames():
            data = self.worker.read(in_filename)
            output_info[in_filename] = data
        self.worker.success(output_info)


if __name__ == '__main__':
    start()
