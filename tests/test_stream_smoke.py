import pytest


# Skip if the compiled extension (or its deps) is not available.
pytest.importorskip("retracesoftware_stream")
stream = pytest.importorskip("retracesoftware.stream")


def _thread_id() -> str:
    return "main-thread"


def _noop_stack_diff(previous, recorded, replayed):
    return None


def test_replace_prefix():
    assert stream.replace_prefix("root/path", "root/", "/tmp/") == "/tmp/path"
    assert stream.replace_prefix("root/path", "nomatch/", "/tmp/") == "root/path"


def test_writer_reader_roundtrip(tmp_path):
    path = tmp_path / "trace.bin"

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as writer:
        writer("hello", 123)
        writer.flush()

    with stream.reader(
        path,
        thread=_thread_id,
        timeout_seconds=1,
        verbose=False,
        on_stack_difference=_noop_stack_diff,
    ) as reader:
        assert reader() == "hello"
        assert reader() == 123

