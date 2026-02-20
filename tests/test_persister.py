"""Tests for AsyncFilePersister via the writer pipeline.

The persister is no longer directly callable -- it processes QueueEntry
items from ObjectWriter via the SPSC queue. These tests exercise the
persister's file handling and data integrity through the writer.
"""
import gc
import os
import struct
import threading

import pytest

pytest.importorskip("retracesoftware.stream")
import retracesoftware.stream as stream

_mod = stream._backend_mod
AsyncFilePersister = _mod.AsyncFilePersister

_thread_id = lambda: threading.current_thread().ident


def _unframe(data: bytes) -> bytes:
    """Strip PID frame headers and return concatenated payloads."""
    out = bytearray()
    i = 0
    while i + 6 <= len(data):
        _pid, length = struct.unpack_from('<IH', data, i)
        i += 6
        out.extend(data[i:i + length])
        i += length
    return bytes(out)


# ---------------------------------------------------------------------------
# Construction / teardown
# ---------------------------------------------------------------------------

def test_construct_and_close(tmp_path):
    """Persister opens a file; close joins any threads."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    assert path.exists()
    p.close()


def test_close_is_idempotent(tmp_path):
    """Calling close() multiple times must not crash."""
    p = AsyncFilePersister(str(tmp_path / "out.bin"))
    p.close()
    p.close()
    p.close()


def test_dealloc_without_close(tmp_path):
    """Dropping all references should cleanly shut down."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    del p
    gc.collect()
    assert path.exists()


def test_open_nonexistent_directory():
    """Opening a file in a missing directory raises IOError."""
    with pytest.raises(IOError):
        AsyncFilePersister("/no/such/directory/file.bin")


def test_exclusive_lock(tmp_path):
    """A second persister on the same file must fail with IOError (flock)."""
    path = tmp_path / "locked.bin"
    p1 = AsyncFilePersister(str(path))
    with pytest.raises(IOError, match="exclusive"):
        AsyncFilePersister(str(path))
    p1.close()


# ---------------------------------------------------------------------------
# Writing through the writer pipeline
# ---------------------------------------------------------------------------

def test_write_single_message(tmp_path):
    """Write a single object through the writer and verify file has data."""
    path = tmp_path / "out.bin"
    with stream.writer(path, thread=_thread_id) as w:
        w("hello")
        w.flush()

    raw = path.read_bytes()
    assert len(raw) > 0
    payload = _unframe(raw)
    assert len(payload) > 0


def test_write_multiple_messages(tmp_path):
    """Multiple writes produce valid PID-framed output."""
    path = tmp_path / "out.bin"
    with stream.writer(path, thread=_thread_id) as w:
        for i in range(100):
            w(f"msg_{i}")
        w.flush()

    raw = path.read_bytes()
    payload = _unframe(raw)
    assert len(payload) > 0


def test_close_drains_queue(tmp_path):
    """All queued writes must be flushed to disk before close() returns."""
    path = tmp_path / "out.bin"
    with stream.writer(path, thread=_thread_id) as w:
        for i in range(500):
            w(f"item_{i:04d}")

    raw = path.read_bytes()
    assert len(raw) > 0
    payload = _unframe(raw)
    assert len(payload) > 0


def test_truncates_existing_file(tmp_path):
    """Opening a path that already contains data truncates to zero."""
    path = tmp_path / "out.bin"
    path.write_bytes(b"old content that should disappear")

    p = AsyncFilePersister(str(path))
    p.close()

    assert path.read_bytes() == b""


def test_append_mode(tmp_path):
    """Opening with append=True preserves existing data."""
    path = tmp_path / "out.bin"

    with stream.writer(path, thread=_thread_id) as w:
        w("first")
        w.flush()

    size_after_first = path.stat().st_size
    assert size_after_first > 0

    with stream.writer(path, thread=_thread_id, append=True) as w:
        w("second")
        w.flush()

    assert path.stat().st_size > size_after_first


def test_drain_and_resume(tmp_path):
    """Drain stops the writer thread; resume restarts it."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    p.drain()
    p.resume()
    p.close()


def test_many_writes_stress(tmp_path):
    """Stress test: many rapid writes all get persisted."""
    path = tmp_path / "out.bin"
    with stream.writer(path, thread=_thread_id, flush_interval=999) as w:
        for i in range(5000):
            w(f"stress_{i}")
        w.flush()

    raw = path.read_bytes()
    payload = _unframe(raw)
    assert len(payload) > 0


def test_fd_getter(tmp_path):
    """The fd property returns a valid file descriptor."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    assert p.fd >= 0
    p.close()
    assert p.fd < 0


def test_path_getter(tmp_path):
    """The path property returns the file path."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    assert p.path == str(path)
    p.close()
