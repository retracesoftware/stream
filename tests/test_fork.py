"""Fork safety tests for the writer + AsyncFilePersister.

Verifies that os.fork() is handled correctly:
- Parent continues recording after fork
- Child does not corrupt the parent's trace file
- Data written before, during (leaked), and after fork is preserved
"""
import os
import sys
import pytest

pytest.importorskip("retracesoftware.stream")
stream = pytest.importorskip("retracesoftware.stream")

if sys.platform == "win32":
    pytest.skip("fork not available on Windows", allow_module_level=True)


def _thread_id() -> str:
    return "main-thread"


def _read_value(reader):
    while True:
        val = reader()
        if not isinstance(val, (stream.Bind, stream.ThreadSwitch)):
            return val


def test_fork_parent_continues(tmp_path):
    """Parent records before and after fork; child exits without writing."""
    path = tmp_path / "trace.bin"

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as w:
        w("before_fork")
        w.flush()

        pid = os.fork()
        if pid == 0:
            # Child: just exit without writing
            os._exit(0)

        # Parent: wait for child, then write more
        os.waitpid(pid, 0)
        w("after_fork")
        w.flush()

    with stream.reader(path=path, read_timeout=2, verbose=False) as r:
        assert _read_value(r) == "before_fork"
        assert _read_value(r) == "after_fork"


def test_fork_child_disabled(tmp_path):
    """Child's writer is disabled after fork (output=None)."""
    path = tmp_path / "trace.bin"

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as w:
        w("parent_data")
        w.flush()

        pid = os.fork()
        if pid == 0:
            # Child: try writing -- should be a no-op
            w("child_data_should_not_appear")
            w.flush()
            os._exit(0)

        os.waitpid(pid, 0)
        w.flush()

    with stream.reader(path=path, read_timeout=2, verbose=False) as r:
        assert _read_value(r) == "parent_data"


def test_fork_preserves_large_trace(tmp_path):
    """Fork in the middle of a large recording preserves all parent data."""
    path = tmp_path / "trace.bin"
    n_before = 100
    n_after = 100

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as w:
        for i in range(n_before):
            w(f"pre_{i}")
        w.flush()

        pid = os.fork()
        if pid == 0:
            os._exit(0)

        os.waitpid(pid, 0)

        for i in range(n_after):
            w(f"post_{i}")
        w.flush()

    with stream.reader(path=path, read_timeout=2, verbose=False) as r:
        for i in range(n_before):
            assert _read_value(r) == f"pre_{i}"
        for i in range(n_after):
            assert _read_value(r) == f"post_{i}"


def test_fork_multiple_times(tmp_path):
    """Multiple forks in sequence don't corrupt the trace."""
    path = tmp_path / "trace.bin"

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as w:
        for fork_num in range(3):
            w(f"round_{fork_num}")
            w.flush()

            pid = os.fork()
            if pid == 0:
                os._exit(0)
            os.waitpid(pid, 0)

        w("final")
        w.flush()

    with stream.reader(path=path, read_timeout=2, verbose=False) as r:
        for fork_num in range(3):
            assert _read_value(r) == f"round_{fork_num}"
        assert _read_value(r) == "final"
