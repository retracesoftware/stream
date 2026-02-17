"""Standalone tests for AsyncFilePersister.

These exercise the persister directly with raw bytes/memoryview,
without going through ObjectWriter or the writer wrapper.
"""
import gc
import os

import pytest

pytest.importorskip("retracesoftware.stream")
import retracesoftware.stream as stream

_mod = stream._backend_mod
AsyncFilePersister = _mod.AsyncFilePersister


# ---------------------------------------------------------------------------
# Construction / teardown
# ---------------------------------------------------------------------------

def test_construct_and_close(tmp_path):
    """Persister opens a file and the writer thread starts; close joins it."""
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
    """Dropping all references should cleanly shut down the writer thread."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    p(b"garbage-collected")
    del p
    gc.collect()

    assert path.stat().st_size == len(b"garbage-collected")


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
# Writing bytes
# ---------------------------------------------------------------------------

def test_write_bytes_single(tmp_path):
    """Write a single bytes object and verify file contents."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    p(b"hello")
    p.close()

    assert path.read_bytes() == b"hello"


def test_write_bytes_multiple(tmp_path):
    """Multiple byte writes are concatenated in order."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    p(b"aaa")
    p(b"bbb")
    p(b"ccc")
    p.close()

    assert path.read_bytes() == b"aaabbbccc"


def test_write_empty_bytes(tmp_path):
    """Writing empty bytes should not corrupt the file."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    p(b"before")
    p(b"")
    p(b"after")
    p.close()

    assert path.read_bytes() == b"beforeafter"


# ---------------------------------------------------------------------------
# Writing memoryview
# ---------------------------------------------------------------------------

def test_write_memoryview(tmp_path):
    """Persister accepts a memoryview and writes the underlying bytes."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    data = b"memoryview-data"
    p(memoryview(data))
    p.close()

    assert path.read_bytes() == data


def test_write_memoryview_slice(tmp_path):
    """A sliced memoryview writes only the visible portion."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    data = b"ABCDEFGHIJ"
    mv = memoryview(data)[3:7]  # "DEFG"
    p(mv)
    p.close()

    assert path.read_bytes() == b"DEFG"


def test_write_mixed_bytes_and_memoryview(tmp_path):
    """Interleaving bytes and memoryview writes preserves order."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))
    p(b"bytes-")
    p(memoryview(b"mv-"))
    p(b"bytes")
    p.close()

    assert path.read_bytes() == b"bytes-mv-bytes"


# ---------------------------------------------------------------------------
# Type checking
# ---------------------------------------------------------------------------

def test_rejects_str(tmp_path):
    """Passing a str should raise TypeError."""
    p = AsyncFilePersister(str(tmp_path / "out.bin"))
    with pytest.raises(TypeError, match="expected memoryview or bytes"):
        p("not bytes")
    p.close()


def test_rejects_int(tmp_path):
    """Passing an int should raise TypeError."""
    p = AsyncFilePersister(str(tmp_path / "out.bin"))
    with pytest.raises(TypeError):
        p(42)
    p.close()


def test_rejects_list(tmp_path):
    """Passing a list should raise TypeError."""
    p = AsyncFilePersister(str(tmp_path / "out.bin"))
    with pytest.raises(TypeError):
        p([1, 2, 3])
    p.close()


# ---------------------------------------------------------------------------
# Data integrity under volume
# ---------------------------------------------------------------------------

def test_large_sequential_writes(tmp_path):
    """Many small writes are all persisted in order."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))

    n = 10_000
    for i in range(n):
        p(f"{i}\n".encode())
    p.close()

    lines = path.read_text().splitlines()
    assert len(lines) == n
    for i, line in enumerate(lines):
        assert line == str(i), f"line {i}: expected {i!r}, got {line!r}"


def test_large_chunk_writes(tmp_path):
    """Writing chunks larger than the BufferSlot size (64K)."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))

    chunk = bytes(range(256)) * 512  # 128 KB
    p(chunk)
    p(chunk)
    p.close()

    data = path.read_bytes()
    assert len(data) == len(chunk) * 2
    assert data == chunk + chunk


def test_many_flushes_stress(tmp_path):
    """Simulate rapid flush cadence: many tiny writes followed by close."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))

    expected = bytearray()
    for i in range(5000):
        chunk = bytes([i % 256]) * 13
        p(chunk)
        expected.extend(chunk)
    p.close()

    assert path.read_bytes() == bytes(expected)


# ---------------------------------------------------------------------------
# Close drains the queue
# ---------------------------------------------------------------------------

def test_close_drains_queue(tmp_path):
    """All queued writes must be flushed to disk before close() returns."""
    path = tmp_path / "out.bin"
    p = AsyncFilePersister(str(path))

    total = 0
    for i in range(200):
        chunk = os.urandom(1000)
        p(chunk)
        total += len(chunk)

    p.close()

    assert path.stat().st_size == total


# ---------------------------------------------------------------------------
# File is truncated on open
# ---------------------------------------------------------------------------

def test_truncates_existing_file(tmp_path):
    """Opening a path that already contains data truncates to zero."""
    path = tmp_path / "out.bin"
    path.write_bytes(b"old content that should disappear")

    p = AsyncFilePersister(str(path))
    p(b"new")
    p.close()

    assert path.read_bytes() == b"new"
