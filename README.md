# retracesoftware-stream

Fast binary serialization of Python objects — optimized for retrace
record/replay workloads. The core serialization logic is implemented in C++
for speed; the Python layer provides a thin wrapper with customizable
type serializers and a pluggable persistence backend.

## Install

```bash
pip install retracesoftware-stream
```

Requires Python >= 3.11.

## Quick start

```python
from retracesoftware.stream import writer, reader

# ---- Write ----
with writer("trace.bin", thread=lambda: "main") as w:
    w.write_root(42)
    w.write_root("hello")
    w.write_root([1, 2, 3])

# ---- Read ----
with reader("trace.bin", read_timeout=5, verbose=False) as r:
    assert r.read() == 42
    assert r.read() == "hello"
    assert r.read() == [1, 2, 3]
```

## Architecture

Serialization and persistence are **decoupled**. The C++ serialization layer
produces byte buffers; a pluggable output callback decides where those bytes
go (file, memory, network, etc.).

```
                         write_root(obj)
┌─────────────┐  ─────────────────────────▶ ┌──────────────────┐
│   Python    │                              │  C++ writer.h    │
│   writer    │   type_serializer[type(obj)] │  MessageStream   │
│  serialize()│ ◀───── fallback ──────────── │  ::write(obj)    │
└─────────────┘                              └────────┬─────────┘
                                                      │ flush()
                                             ┌────────▼─────────┐
                                             │  PrimitiveStream  │
                                             │  double-buffered  │
                                             │  64 KB BufferSlots│
                                             └────────┬─────────┘
                                                      │ memoryview
                                             ┌────────▼─────────┐
                                             │  output callback  │
                                             │  (any callable)   │
                                             └────────┬─────────┘
                                                      │
                          ┌───────────────────────────┼───────────────────────┐
                          │                           │                       │
                 ┌────────▼────────┐       ┌─────────▼────────┐   ┌──────────▼─────────┐
                 │AsyncFilePersister│       │ Python callable  │   │  FileOutput        │
                 │ (C++ bg thread) │       │ (in-memory, etc.)│   │  (sync fallback)   │
                 │  raw ::write()  │       │                  │   │                    │
                 └─────────────────┘       └──────────────────┘   └────────────────────┘

                                                      │
┌─────────────┐       read()              ┌───────────▼────────┐
│   Python    │ ◀──────────────────────── │  C++ objectstream   │
│   reader    │   type_deserializer[type] │  ObjectStreamReader │
│ deserialize()│ ◀───── PICKLED ───────── │  ::read()           │
└─────────────┘                           └────────────────────┘
```

### Double buffering

`PrimitiveStream` maintains two 64 KB `BufferSlot` objects. Serialization
fills one slot while the previous slot's `memoryview` is being consumed by
the output callback. The `BufferSlot` type implements the Python buffer
protocol and uses `std::atomic<bool> in_use` to coordinate ownership — the
serializer can detect when the callback has finished with a slot without any
locks on the hot path.

### AsyncFilePersister (default)

When a `path` is provided, the writer creates a C++ `AsyncFilePersister` as
the default output callback. It runs a dedicated background thread that drains
a queue of `memoryview` references using raw POSIX `::write()` syscalls.
This keeps file I/O off the main thread entirely. The persister acquires an
exclusive `flock` on the trace file and supports both truncate and append
modes.

### Custom output callbacks

Any Python callable accepting a single `memoryview` (or bytes-like) argument
can be used as an output callback, enabling in-memory record/replay for
testing or streaming to alternative sinks:

```python
chunks = []
with writer(output=lambda data: chunks.append(bytes(data)),
            thread=lambda: "main") as w:
    w("hello")
```

## Wire format

Each value is encoded as a control byte followed by payload. The control
byte's lower 4 bits select the **sized type**; the upper 4 bits encode either
the **size class** (for sized types) or a **fixed-size type** tag.

### Built-in types (handled directly in C++)

| Wire type | Python type | Encoding |
|-----------|-------------|----------|
| `NONE` | `None` | 1 byte (fixed) |
| `TRUE` / `FALSE` | `bool` | 1 byte (fixed) |
| `UINT` | `int` (non-negative) | varint |
| `NEG1` | `int` (-1) | 1 byte (fixed) |
| `INT64` | `int` (signed, fits i64) | 1 + 8 bytes (fixed) |
| `BIGINT` | `int` (arbitrary) | length-prefixed bytes |
| `FLOAT` | `float` | 1 + 8 bytes (fixed) |
| `STR` | `str` | length-prefixed UTF-8 |
| `STR_REF` | `str` (interned) | varint reference to earlier string |
| `BYTES` | `bytes` | length-prefixed raw |
| `LIST` | `list` | length + recursive write of elements |
| `TUPLE` | `tuple` | length + recursive write of elements |
| `DICT` | `dict` | length + recursive write of key/value pairs |
| `SET` | `set` | length + recursive write of elements |
| `FROZENSET` | `frozenset` | length + recursive write of elements |

### Identity types (binding system)

| Wire type | Purpose |
|-----------|---------|
| `BIND` | Assign the next binding ID to an object allocated inside the sandbox |
| `EXT_BIND` | Assign the next binding ID to an object returned from external code (followed by a type reference) |
| `BINDING` | Reference a previously-bound object by its integer ID |
| `BINDING_DELETE` | Release a binding (object was garbage collected) |
| `DELETE` | Release a handle |

### Stream-level markers

| Wire type | Purpose |
|-----------|---------|
| `HANDLE` | Reference a permanent handle (e.g. a `StubRef`) |
| `NEW_HANDLE` | Assign the next handle ID |
| `THREAD_SWITCH` | Mark a switch to a different thread |
| `STACK` | Stack frame delta |
| `ADD_FILENAME` | Register a source filename |
| `CHECKSUM` | Integrity checksum |

### Fallback serialization

| Wire type | Purpose |
|-----------|---------|
| `PICKLED` | Length-prefixed pickle bytes (any type not handled above) |

## Serialization dispatch

The C++ `MessageStream::write(obj)` function dispatches by exact type:

```cpp
if (obj == Py_None)                          → NONE
else if (type == PyUnicode_Type)             → STR
else if (type == PyLong_Type)                → UINT / NEG1 / INT64 / BIGINT
else if (type == PyBytes_Type)               → BYTES
else if (type == PyBool_Type)                → TRUE / FALSE
else if (type == PyTuple_Type)               → TUPLE (recursive)
else if (type == PyList_Type)                → LIST (recursive)
else if (type == PyDict_Type)                → DICT (recursive)
else if (bindings.contains(obj))             → BINDING (reference by ID)
else if (type == PyFloat_Type)               → FLOAT
else if (type == PyMemoryView_Type)          → MEMORY_VIEW
else                                         → write_serialized (Python callback)
```

The **binding check** (line 569 in `writer.h`) is critical: if an object has
been previously bound (via `bind()` or `ext_bind()`), it is written as a
single varint `BINDING` reference instead of being re-serialized. This is how
object identity is preserved across the stream.

The **fallback** `write_serialized` calls the Python `serializer` callback.
If the callback returns `bytes`, they are written as `PICKLED`. If it returns
a Python object (e.g. a handle reference), that object is recursively written
via `write()`.

## The binding system

The binding system tracks object identity across the stream. It maps live
Python objects to integer IDs so the same object is never serialized twice —
subsequent references write a compact `BINDING` varint.

### Internal bind (`BIND`)

Called when a new object is allocated **inside** the sandbox (e.g. a patched
type instance created by user code). Writes a `BIND` marker. The reader
assigns the next object it encounters the corresponding binding ID.

### External bind (`EXT_BIND`)

Called when a new object is returned **from** external code into the sandbox.
Writes an `EXT_BIND` marker followed by a `BINDING` reference to the object's
**type** (which must already be bound). This tells the reader: "a new object
of this type just entered the sandbox — assign it a binding ID."

The type's binding is established when `patch_type` or the proxy factory
registers the type on the writer.

### Garbage collection

When a bound object is garbage collected, the writer's `tp_free` hook fires
and writes a `BINDING_DELETE` marker, freeing the binding ID for reuse.

## Custom type serializers

Both `writer` and `reader` support per-type customization:

### Writer

```python
w = writer("trace.bin", thread=lambda: "main")

# Register a custom serializer for SomeType
w.type_serializer[SomeType] = my_serialize_fn

# When write(obj) encounters an unknown type, it calls:
#   type_serializer.get(type(obj), pickle.dumps)(obj)
```

The serializer function receives the object and must return either:
- `bytes` — written as `PICKLED` on the stream
- A Python object — recursively written via `write()` (e.g. a handle reference)

### Reader

```python
r = reader("trace.bin", read_timeout=5, verbose=False)

# Register a custom deserializer for StubRef
r.type_deserializer[StubRef] = my_deserialize_fn

# When a PICKLED value is read, the reader calls:
#   pickle.loads(bytes)
# Then checks type_deserializer for a post-processing hook.
```

## How retrace uses the serializers

The proxy system registers three custom `type_serializer` entries to handle
the different kinds of objects that cross the sandbox boundary:

### 1. Patched type instances

```python
# When patch_type(cls) is called:
writer.type_serializer[patched_cls] = functional.side_effect(writer.ext_bind)
```

Instances of patched types are not serialized as data. Instead, `ext_bind`
is called as a side effect, which writes an `EXT_BIND` marker referencing the
type. The reader allocates a binding ID for the object. From that point on,
any reference to the same object writes a compact `BINDING` varint.

This works because patched types have their methods replaced globally — any
instance, whether freshly allocated or deserialized, has its methods routed
through the gates. The stream only needs to track identity, not state.

### 2. Proxied (DynamicProxy) type instances

```python
# When a DynamicProxy type is created for a non-patched return type:
ref = writer.handle(StubRef(proxytype))
writer.type_serializer[proxytype] = functional.constantly(ref)
```

A `StubRef` is a serializable descriptor of the type's interface (method
names, module, qualified name). It is assigned a permanent **handle** on the
stream. Whenever an instance of the DynamicProxy type is encountered, the
serializer writes the handle reference — not the object's data.

On the reader side, the `StubRef` is deserialized and `StubFactory`
synthesizes a stub type where every method reads the next result from the
stream. The stub behaves identically to the original object during replay.

### 3. Module objects

```python
writer.type_serializer[types.ModuleType] = GlobalRef
```

Modules are serialized as `GlobalRef(module)` — a `(module_name, attr_name)`
pair that can be resolved by `importlib` during replay.

## Fork safety

On Unix, `os.fork()` duplicates file descriptors. If a child process flushes
or writes to the parent's trace file the recording is corrupted. The writer
registers `os.register_at_fork` handlers to prevent this:

| Phase | Action |
|---|---|
| **before fork** | Flush the `ObjectWriter`, close the `AsyncFilePersister` (drains its queue and joins the writer thread), swap `output` to a synchronous Python append-writer, disable buffering (`buffer_writes = False`). |
| **after fork (parent)** | Create a new `AsyncFilePersister` in append mode on the same file, restore it as `output`, re-enable buffering. |
| **after fork (child)** | Set `output = None` — the child cannot write to the parent's trace. |

During the fork window the writer degrades to a synchronous
open-append-close cycle per flush. This is safe but slower, so the window
is kept as short as possible.

### Preventing retrace recursion

When retrace instruments `open()` and `write()`, the synchronous fallback
writer would trigger infinite recursion. Pass a `disable_retrace` context
manager to suppress tracing during internal I/O:

```python
w = writer("trace.bin", thread=get_thread, disable_retrace=my_ctx_mgr)
```

The `AsyncFilePersister` uses raw POSIX `::write()` in C++ so it is never
traced — only the Python fallback during the fork window needs the guard.

## Multi-threading

The writer is thread-safe. Events from different threads are interleaved on
a single stream with `THREAD_SWITCH` markers. The reader emits `ThreadSwitch`
control objects that the `per_thread` demuxer uses to split the stream into
per-thread sequences.

```python
from retracesoftware.stream import per_thread

# Demultiplex a single stream into per-thread sources
source = per_thread(source=r.read, thread=get_thread_id, timeout=5)
```

## Writer properties

| Property | Type | Description |
|---|---|---|
| `output` | callable or `None` | Output callback; writable. Set to `None` to disable writing. |
| `buffer_writes` | `bool` | When `False`, every `write()` call flushes immediately. Default `True`. |
| `path` | path-like | Trace file path (set when constructed with `path=`). |

## Source layout

```
src/retracesoftware/stream/
└── __init__.py          Python wrapper (writer, reader, per_thread, fork safety)

cpp/
├── wireformat.h         Wire format constants (SizedTypes, FixedSizeTypes, Control)
├── writer.h             Core serialization (PrimitiveStream, MessageStream)
├── bufferslot.h         BufferSlot — 64 KB double-buffer with atomic in_use flag
├── persister.cpp        AsyncFilePersister — background-thread file writer
├── stream.h             Shared declarations and type externs
├── base.h               Base types and helpers
├── objectwriter.cpp     ObjectWriter Python type (write_root, handle, output property)
├── objectstream.cpp     ObjectStreamReader (read, read_pickled, bindings)
├── module.cpp           Python C extension module definition
├── search.cpp           Stream search utilities
└── unordered_dense.h    Vendored hash map

tests/
├── test_stream_smoke.py Round-trip serialization, in-memory callbacks, large data
├── test_persister.py    AsyncFilePersister unit tests (19 tests)
└── test_fork.py         os.fork() interaction tests (4 tests)
```

## Dependencies

| Package | Purpose |
|---|---|
| `retracesoftware-functional` | Functional combinators (`isinstanceof`, `sequence`) |
| `retracesoftware-utils` | C++ utilities (`demux` for multi-thread) |
