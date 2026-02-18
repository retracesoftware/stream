"""Fork safety tests for the writer + AsyncFilePersister.

Verifies that os.fork() is handled correctly:
- Parent continues recording after fork
- Child does not corrupt the parent's trace file
- Data written before, during (leaked), and after fork is preserved
- Reader PID filtering, set_pid switching, and frame buffering
"""
import os
import sys
import time
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


def _read_all_values(reader):
    """Read all non-control values until EOF."""
    values = []
    try:
        while True:
            val = reader()
            if not isinstance(val, (stream.Bind, stream.ThreadSwitch, stream.Control)):
                values.append(val)
    except Exception:
        pass
    return values


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


# ---------------------------------------------------------------------------
# PID filtering + set_pid + buffering
# ---------------------------------------------------------------------------

def test_reader_filters_by_first_pid(tmp_path):
    """Reader only returns frames matching the first PID it encounters."""
    path = tmp_path / "trace.bin"

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as w:
        w("parent_before")
        w.flush()

        pid = os.fork()
        if pid == 0:
            w("child_data")
            w.flush()
            time.sleep(0.05)
            os._exit(0)

        os.waitpid(pid, 0)
        w("parent_after")
        w.flush()

    with stream.reader(path=path, read_timeout=2, verbose=False) as r:
        vals = _read_all_values(r)

    assert "parent_before" in vals
    assert "parent_after" in vals
    assert "child_data" not in vals


def test_reader_set_pid_switches_to_child(tmp_path):
    """After set_pid, reader returns the child's frames."""
    path = tmp_path / "trace.bin"
    child_pid_holder = [None]

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as w:
        w("parent_first")
        w.flush()

        pid = os.fork()
        if pid == 0:
            w("child_val_1")
            w("child_val_2")
            w.flush()
            time.sleep(0.05)
            os._exit(0)

        child_pid_holder[0] = pid
        os.waitpid(pid, 0)
        w("parent_second")
        w.flush()

    child_pid = child_pid_holder[0]

    with stream.reader(path=path, read_timeout=2, verbose=False) as r:
        assert _read_value(r) == "parent_first"

        r.set_pid(child_pid)
        vals = _read_all_values(r)

    assert "child_val_1" in vals
    assert "child_val_2" in vals
    assert "parent_second" not in vals


def test_reader_buffers_skipped_frames(tmp_path):
    """Child frames written before parent resumes are buffered and
    available after set_pid, not lost."""
    path = tmp_path / "trace.bin"
    child_pid_holder = [None]

    r_fd, w_fd = os.pipe()

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as w:
        w("parent_pre")
        w.flush()

        pid = os.fork()
        if pid == 0:
            os.close(r_fd)
            w("child_early_1")
            w("child_early_2")
            w.flush()
            os.write(w_fd, b'\x00')
            os.close(w_fd)
            time.sleep(0.1)
            os._exit(0)

        os.close(w_fd)
        os.read(r_fd, 1)
        os.close(r_fd)

        child_pid_holder[0] = pid
        os.waitpid(pid, 0)

        w("parent_post")
        w.flush()

    child_pid = child_pid_holder[0]

    with stream.reader(path=path, read_timeout=2, verbose=False) as r:
        assert _read_value(r) == "parent_pre"
        assert _read_value(r) == "parent_post"

        r.set_pid(child_pid)
        vals = _read_all_values(r)

    assert "child_early_1" in vals
    assert "child_early_2" in vals


def test_list_pids(tmp_path):
    """list_pids returns all unique PIDs present in a trace."""
    path = tmp_path / "trace.bin"

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as w:
        w("from_parent")
        w.flush()

        pid = os.fork()
        if pid == 0:
            w("from_child")
            w.flush()
            time.sleep(0.05)
            os._exit(0)

        os.waitpid(pid, 0)
        w.flush()

    pids = stream.list_pids(path)
    assert len(pids) >= 2
    assert os.getpid() in pids
    assert pid in pids


def test_set_pid_on_empty_buffer(tmp_path):
    """set_pid with no buffered frames for that PID still works;
    reader continues from the file stream."""
    path = tmp_path / "trace.bin"
    child_pid_holder = [None]

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as w:
        w("parent_only_1")
        w.flush()

        pid = os.fork()
        if pid == 0:
            time.sleep(0.05)
            os._exit(0)

        child_pid_holder[0] = pid
        os.waitpid(pid, 0)

        w("parent_only_2")
        w.flush()

    with stream.reader(path=path, read_timeout=2, verbose=False) as r:
        assert _read_value(r) == "parent_only_1"
        r.set_pid(child_pid_holder[0])
        vals = _read_all_values(r)

    assert "parent_only_2" not in vals
