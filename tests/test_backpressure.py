"""Tests for backpressure timeout, oversized messages, and message boundary framing."""
import pytest

pytest.importorskip("retracesoftware.stream")
import retracesoftware.stream as stream


def _thread_id() -> str:
    return "main-thread"


def _read_all(reader):
    """Read all values from a reader until EOF."""
    values = []
    try:
        while True:
            values.append(reader())
    except Exception:
        pass
    return values


def _read_values(reader):
    """Read all non-control values from a reader until EOF."""
    return [v for v in _read_all(reader) if not isinstance(v, stream.Control)]


# ---------------------------------------------------------------------------
# backpressure_timeout property
# ---------------------------------------------------------------------------

def test_backpressure_timeout_defaults_to_none(tmp_path):
    path = tmp_path / "trace.bin"
    with stream.writer(path, thread=_thread_id) as w:
        assert w.backpressure_timeout is None


def test_backpressure_timeout_property_writable(tmp_path):
    path = tmp_path / "trace.bin"
    with stream.writer(path, thread=_thread_id) as w:
        assert w.backpressure_timeout is None
        w.backpressure_timeout = 0
        assert w.backpressure_timeout == 0.0
        w.backpressure_timeout = 0.05
        assert abs(w.backpressure_timeout - 0.05) < 1e-6
        w.backpressure_timeout = None
        assert w.backpressure_timeout is None


def test_backpressure_timeout_param(tmp_path):
    path = tmp_path / "trace.bin"
    with stream.writer(path, thread=_thread_id, backpressure_timeout=0) as w:
        assert w.backpressure_timeout == 0.0
    with stream.writer(path, thread=_thread_id, backpressure_timeout=0.1) as w:
        assert abs(w.backpressure_timeout - 0.1) < 1e-6


def test_backpressure_timeout_rejects_negative(tmp_path):
    path = tmp_path / "trace.bin"
    with stream.writer(path, thread=_thread_id) as w:
        with pytest.raises(ValueError):
            w.backpressure_timeout = -1


# ---------------------------------------------------------------------------
# Drop mode round-trip
# ---------------------------------------------------------------------------

def test_drop_mode_dropped_marker_roundtrip(tmp_path):
    """When the persister can't keep up, messages are dropped and a DROPPED
    marker with the count appears in the stream."""
    path = tmp_path / "trace.bin"
    held = []
    all_data = bytearray()

    def holding_callback(data):
        held.append(data)

    w = stream.writer(output=holding_callback, thread=_thread_id,
                      backpressure_timeout=0, flush_interval=999)
    w.__enter__()

    # Write enough small messages to fill BOTH 64KB buffers.  After the first
    # buffer flushes, the callback holds its memoryview keeping slot 0 in_use.
    # When slot 1 fills and tries to swap back to slot 0, backpressure
    # triggers a drop.  Each message is ~10 bytes so we need >13000 to exceed
    # 2 * 64KB.
    for i in range(15000):
        w(f"msg_{i:05d}")

    # At least one flush must have happened
    assert len(held) >= 1

    # Copy all held data (before releasing memoryviews)
    for mv in held:
        all_data.extend(bytes(mv))
    del mv  # release loop variable's reference to the memoryview
    held.clear()

    # Write one more message -- py_vectorcall will see dropped_messages > 0
    # and emit the DROPPED marker before the new data.
    w("after_drop")
    w.flush()

    for mv in held:
        all_data.extend(bytes(mv))
    del mv
    held.clear()

    w.__exit__(None, None, None)
    for mv in held:
        all_data.extend(bytes(mv))
    if held:
        del mv
    held.clear()

    # Write combined data to a file for the reader
    path.write_bytes(bytes(all_data))

    with stream.reader(path=path, read_timeout=1, verbose=False) as r:
        values = _read_all(r)

    dropped_markers = [v for v in values if isinstance(v, stream.Dropped)]
    assert len(dropped_markers) >= 1, (
        f"Expected at least one Dropped marker, got types: "
        f"{[type(v).__name__ for v in values[:30]]}"
    )
    assert dropped_markers[0].value > 0

    non_control = [v for v in values if not isinstance(v, stream.Control)]
    assert "after_drop" in non_control


# ---------------------------------------------------------------------------
# Oversized messages (> 64KB)
# ---------------------------------------------------------------------------

def test_oversized_bytes_roundtrip(tmp_path):
    """A single bytes object larger than the 64KB buffer round-trips."""
    path = tmp_path / "trace.bin"
    big = b"X" * (65536 * 2)

    with stream.writer(path, thread=_thread_id) as w:
        w(big)
        w.flush()

    with stream.reader(path=path, read_timeout=1, verbose=False) as r:
        val = _read_values(r)
        assert val == [big]


def test_oversized_string_roundtrip(tmp_path):
    """A single large string that exceeds the buffer size round-trips."""
    path = tmp_path / "trace.bin"
    big_str = "A" * (65536 * 3)

    with stream.writer(path, thread=_thread_id) as w:
        w(big_str)
        w.flush()

    with stream.reader(path=path, read_timeout=1, verbose=False) as r:
        val = _read_values(r)
        assert val == [big_str]


def test_oversized_list_roundtrip(tmp_path):
    """A list with enough entries to exceed the buffer size round-trips."""
    path = tmp_path / "trace.bin"
    big_list = list(range(20000))

    with stream.writer(path, thread=_thread_id) as w:
        w(big_list)
        w.flush()

    with stream.reader(path=path, read_timeout=1, verbose=False) as r:
        val = _read_values(r)
        assert val == [big_list]


def test_oversized_then_normal(tmp_path):
    """An oversized message followed by normal messages all round-trip."""
    path = tmp_path / "trace.bin"
    big = b"Z" * (65536 * 2)

    with stream.writer(path, thread=_thread_id) as w:
        w(big)
        w("small_1", 42)
        w.flush()

    with stream.reader(path=path, read_timeout=1, verbose=False) as r:
        val = _read_values(r)
        assert val == [big, "small_1", 42]


# ---------------------------------------------------------------------------
# Message boundary framing (messages straddling buffer boundary)
# ---------------------------------------------------------------------------

def test_messages_across_buffer_boundary(tmp_path):
    """Many messages that collectively exceed the 64KB buffer all survive."""
    path = tmp_path / "trace.bin"
    count = 3000

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as w:
        for i in range(count):
            w(f"boundary_{i:05d}")
        w.flush()

    with stream.reader(path=path, read_timeout=1, verbose=False) as r:
        vals = _read_values(r)

    assert len(vals) == count
    for i, v in enumerate(vals):
        assert v == f"boundary_{i:05d}", f"Mismatch at index {i}: {v!r}"


def test_mixed_sizes_across_boundary(tmp_path):
    """Messages of varying sizes that straddle the buffer boundary."""
    path = tmp_path / "trace.bin"
    expected = []

    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as w:
        for i in range(500):
            val = "X" * ((i % 200) + 1)
            expected.append(val)
            w(val)
        w.flush()

    with stream.reader(path=path, read_timeout=1, verbose=False) as r:
        vals = _read_values(r)

    assert vals == expected


# ---------------------------------------------------------------------------
# Default wait mode (no dropping with a normal callback)
# ---------------------------------------------------------------------------

def test_wait_mode_no_data_loss(tmp_path):
    """In the default wait mode, all messages are preserved even with
    a slow callback (no memoryview holding, just verifying correctness)."""
    path = tmp_path / "trace.bin"
    count = 5000

    with stream.writer(path, thread=_thread_id) as w:
        for i in range(count):
            w(i)
        w.flush()

    with stream.reader(path=path, read_timeout=1, verbose=False) as r:
        vals = _read_values(r)

    assert len(vals) == count
    for i, v in enumerate(vals):
        assert v == i


# ---------------------------------------------------------------------------
# Dropped class
# ---------------------------------------------------------------------------

def test_dropped_class_value():
    d = stream.Dropped(5)
    assert isinstance(d, stream.Dropped)
    assert isinstance(d, stream.Control)
    assert d.value == 5


def test_dropped_without_callback_skips(tmp_path):
    """When on_dropped is not provided, the reader silently skips DROPPED."""
    path = tmp_path / "trace.bin"
    held = []
    all_data = bytearray()

    def holding_callback(data):
        held.append(data)

    w = stream.writer(output=holding_callback, thread=_thread_id,
                      backpressure_timeout=0, flush_interval=999)
    w.__enter__()

    for i in range(15000):
        w(f"skip_{i:05d}")

    for mv in held:
        all_data.extend(bytes(mv))
    del mv
    held.clear()

    w("final")
    w.flush()

    for mv in held:
        all_data.extend(bytes(mv))
    del mv
    held.clear()

    w.__exit__(None, None, None)
    for mv in held:
        all_data.extend(bytes(mv))
    if held:
        del mv
    held.clear()

    path.write_bytes(bytes(all_data))

    # Use the raw C reader without on_dropped -- Dropped markers are skipped
    r = stream._backend_mod.ObjectStreamReader(
        path=str(path),
        deserialize=lambda x: x,
        bind_singleton=stream.Bind(lambda b: None),
        create_stack_delta=lambda to_drop, frames: None,
        on_thread_switch=stream.ThreadSwitch,
        read_timeout=1,
        verbose=False,
    )
    values = []
    try:
        while True:
            values.append(r())
    except Exception:
        pass

    # No Dropped markers should appear -- they were silently consumed
    dropped_markers = [v for v in values if isinstance(v, stream.Dropped)]
    assert len(dropped_markers) == 0
    # But "final" should be present
    non_control = [v for v in values if not isinstance(v, stream.Control)]
    assert "final" in non_control
