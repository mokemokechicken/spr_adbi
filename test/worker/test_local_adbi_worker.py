import json
import shutil
from pathlib import Path

import spr_adbi.worker.adbi_worker as t

TMP_DIR = str((Path(__file__).parent.parent.parent / "tmp/test").absolute())
WORKING_DIR = TMP_DIR + "/working"
TP = Path(TMP_DIR)
WP = Path(WORKING_DIR)


def test_create_worker():
    obj = t.create_worker([WORKING_DIR])
    assert isinstance(obj, t.LocalADBIWorker)
    assert obj.storage_dir == WORKING_DIR


class TestLocalADBIWorker:
    def setup_method(self, method):
        shutil.rmtree(TMP_DIR, ignore_errors=True)
        self.obj = t.create_worker([WORKING_DIR])
        self.in_dir = WP / "input"
        self.out_dir = WP / "output"

    def teardown_method(self, method):
        shutil.rmtree(TMP_DIR, ignore_errors=True)

    def test_args(self):
        self.in_dir.mkdir(parents=True)
        with open(self.in_dir / "args", "wt") as f:
            json.dump(["aaa", "bbb", "-c"], f)
        assert self.obj.args() == ["aaa", "bbb", "-c"]

        obj = t.create_worker([WORKING_DIR, "xxx", "yyy"])
        assert obj.args() == ["xxx", "yyy"]

    def test_stdin(self):
        self.in_dir.mkdir(parents=True)
        data = "abcdef\n123456".encode()
        with open(self.in_dir / "stdin", "wb") as f:
            f.write(data)
        assert self.obj.stdin() == data

    def test_read(self):
        self.in_dir.mkdir(parents=True)
        data = "abcdef\n123456".encode()
        with open(self.in_dir / "hogehoge", "wb") as f:
            f.write(data)
        assert self.obj.read("input/hogehoge") == data

    def test_write(self):
        data = b"adbi_worker1"
        self.obj.write("output/my_name", data)
        assert data == read_wp("output/my_name")

    def test_write_file(self):
        data = b"adbi_worker2"
        with open(TP / "tmp", "wb") as f:
            f.write(data)

        self.obj.write_file("output/my_name", str(TP / "tmp"))
        assert data == read_wp("output/my_name")

    def test_set_progress(self):
        progress = "very good"
        self.obj.set_progress(progress)
        assert progress == read_wp("progress", "rt")

    def test_success(self):
        data = b"adbi_worker2"
        lp = str(TP / "tmp")
        with open(lp, "wb") as f:
            f.write(data)
        self.obj.success(dict(url="https://heaven/", password="door"), dict(name=lp))
        assert read_wp("output/url", "rt") == "https://heaven/"
        assert read_wp("output/password", "rt") == "door"
        assert read_wp("output/name", "rb") == data
        assert read_wp("status", "rt") == "SUCCESS"

    def test_error(self):
        data = b"adbi_worker2"
        lp = str(TP / "tmp")
        with open(lp, "wb") as f:
            f.write(data)
        self.obj.error("Oh!", dict(url="https://heaven/", password="door"), dict(name=lp))
        assert read_wp("output/url", "rt") == "https://heaven/"
        assert read_wp("output/password", "rt") == "door"
        assert read_wp("output/name", "rb") == data
        assert read_wp("status", "rt") == "ERROR"
        assert read_wp("output/__error__.txt", "rt") == "Oh!"

    def test_get_input_filenames(self):
        self.in_dir.mkdir(parents=True)
        for name in ["file1", "xyz"]:
            with open(self.in_dir / name, "wt") as f:
                f.write("zz")

        filenames = list(sorted(self.obj.get_input_filenames()))
        assert "input/file1" == filenames[0]
        assert "input/xyz" == filenames[1]


def read_wp(path, mode="rb"):
    with open(WP / path, mode) as f:
        return f.read()
