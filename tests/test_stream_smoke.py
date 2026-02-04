import pytest

# 1. Check if the top-level package even exists
try:
    import retracesoftware.stream
    print(f"DEBUG: Found top-level at {retracesoftware.stream.__file__}")
except ImportError as e:
    print(f"DEBUG: Could not find retracesoftware_stream: {e}")
    
# Skip if the compiled extension (or its deps) is not available.
pytest.importorskip("retracesoftware.stream")
stream = pytest.importorskip("retracesoftware.stream")


def _thread_id() -> str:
    return "main-thread"


def _noop_stack_diff(previous, recorded, replayed):
    return None


def test_replace_prefix():
    assert stream.replace_prefix("root/path", "root/", "/tmp/") == "/tmp/path"
    assert stream.replace_prefix("root/path", "nomatch/", "/tmp/") == "root/path"


def test_writer_reader_roundtrip(tmp_path):
    """Basic roundtrip with strings and integers."""
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


def test_primitive_types(tmp_path):
    """Test various primitive types roundtrip correctly."""
    path = tmp_path / "trace.bin"
    
    test_values = [
        # Integers
        0, 1, -1, 42, -999,
        2**31 - 1,   # max int32
        -(2**31),    # min int32
        2**63 - 1,   # max int64
        # Floats
        0.0, 1.5, -3.14159, 1e-10, 1e100,
        float('inf'), float('-inf'),
        # Strings
        "", "a", "hello world", "unicode: 你好 🎉",
        "a" * 1000,  # longer string
        # None
        None,
        # Booleans
        True, False,
    ]
    
    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as writer:
        for val in test_values:
            writer(val)
        writer.flush()
    
    with stream.reader(
        path,
        thread=_thread_id,
        timeout_seconds=1,
        verbose=False,
        on_stack_difference=_noop_stack_diff,
    ) as reader:
        for expected in test_values:
            actual = reader()
            if expected != expected:  # NaN check
                assert actual != actual
            else:
                assert actual == expected, f"Expected {expected!r}, got {actual!r}"


def test_collections(tmp_path):
    """Test lists, tuples, dicts, and sets roundtrip correctly."""
    path = tmp_path / "trace.bin"
    
    test_values = [
        # Lists
        [], [1], [1, 2, 3], ["a", "b", "c"],
        [1, "mixed", 3.14, None, True],
        [[1, 2], [3, 4]],  # nested
        # Tuples
        (), (1,), (1, 2, 3), ("a", "b"),
        (1, (2, (3,))),  # nested
        # Dicts
        {}, {"a": 1}, {"x": 1, "y": 2, "z": 3},
        {"nested": {"inner": 42}},
        {1: "int key", "str": "str key"},
        # Sets
        set(), {1}, {1, 2, 3}, {"a", "b", "c"},
        frozenset(), frozenset({1, 2, 3}),
    ]
    
    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as writer:
        for val in test_values:
            writer(val)
        writer.flush()
    
    with stream.reader(
        path,
        thread=_thread_id,
        timeout_seconds=1,
        verbose=False,
        on_stack_difference=_noop_stack_diff,
    ) as reader:
        for expected in test_values:
            actual = reader()
            assert actual == expected, f"Expected {expected!r}, got {actual!r}"


def test_bytes(tmp_path):
    """Test bytes and bytearray roundtrip correctly."""
    path = tmp_path / "trace.bin"
    
    test_values = [
        b"",
        b"hello",
        b"\x00\x01\x02\xff",
        bytes(range(256)),
        b"x" * 10000,
    ]
    
    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as writer:
        for val in test_values:
            writer(val)
        writer.flush()
    
    with stream.reader(
        path,
        thread=_thread_id,
        timeout_seconds=1,
        verbose=False,
        on_stack_difference=_noop_stack_diff,
    ) as reader:
        for expected in test_values:
            actual = reader()
            assert actual == expected, f"Expected {expected!r}, got {actual!r}"


def test_multiple_writes_single_call(tmp_path):
    """Test writing multiple values in a single call."""
    path = tmp_path / "trace.bin"
    
    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as writer:
        writer("a", "b", "c", 1, 2, 3)
        writer.flush()
    
    with stream.reader(
        path,
        thread=_thread_id,
        timeout_seconds=1,
        verbose=False,
        on_stack_difference=_noop_stack_diff,
    ) as reader:
        assert reader() == "a"
        assert reader() == "b"
        assert reader() == "c"
        assert reader() == 1
        assert reader() == 2
        assert reader() == 3


def test_reader1_roundtrip(tmp_path):
    """Test the reader1 (ObjectStreamReader) API."""
    path = tmp_path / "trace.bin"
    
    test_values = [
        "hello", 123, [1, 2, 3], {"key": "value"}, None, True
    ]
    
    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as writer:
        for val in test_values:
            writer(val)
        writer.flush()
    
    with stream.reader1(path, read_timeout=1, verbose=False) as reader:
        for expected in test_values:
            actual = reader()
            # reader1 may return control objects, skip those
            while isinstance(actual, (stream.Bind, stream.StackDelta, stream.ThreadSwitch)):
                actual = reader()
            assert actual == expected, f"Expected {expected!r}, got {actual!r}"


def test_large_data(tmp_path):
    """Test with larger amounts of data."""
    path = tmp_path / "trace.bin"
    
    # Write many values
    num_values = 1000
    with stream.writer(path, thread=_thread_id, flush_interval=0.01) as writer:
        for i in range(num_values):
            writer(i, f"string_{i}", [i, i+1, i+2])
        writer.flush()
    
    with stream.reader(
        path,
        thread=_thread_id,
        timeout_seconds=5,
        verbose=False,
        on_stack_difference=_noop_stack_diff,
    ) as reader:
        for i in range(num_values):
            assert reader() == i
            assert reader() == f"string_{i}"
            assert reader() == [i, i+1, i+2]

