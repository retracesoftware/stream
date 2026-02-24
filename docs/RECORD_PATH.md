# Record path

Detailed walkthrough of the record (write) path — from `write_root(obj)` on
the main thread through serialization and I/O on background threads to bytes
on disk.  Also covers wire format and serialization dispatch.

## Three-thread pipeline

When a value is recorded, it flows through three threads connected by two
lock-free SPSC queues. The main thread never serializes or does I/O — it
only pushes tagged pointer-sized words into a queue.

```
  Main thread                    Writer thread                 Drain thread
 ┌─────────────────┐            ┌─────────────────┐           ┌──────────────┐
 │  ObjectWriter    │            │ AsyncFile-      │           │ drain_loop   │
 │                  │  QEntry    │ Persister       │ PyObject* │              │
 │  write_root(obj) ├───────────▶ writer_loop     ├───────────▶ Py_DECREF    │
 │  bind(obj)       │  SPSC     │                 │  SPSC     │ total_removed│
 │  ext_bind(obj)   │  queue    │ MessageStream   │  return   │  += est_size │
 │  handle(obj)     │ (64K)     │  ::write(obj)   │  queue    │              │
 │  object_freed()  │           │  ::bind()       │ (128K)    │              │
 └────────┬────────┘            │  ::flush()      │           └──────────────┘
          │                     └────────┬────────┘
          │                              │
          │                     ┌────────▼────────┐
          │ backpressure:       │ PrimitiveStream  │
          │ spin until          │ (vector<uint8_t>)│
          │ inflight < limit    └────────┬────────┘
          │                              │ flush
          │                     ┌────────▼────────┐
          │                     │ PidFramedOutput  │
          │                     │ [pid:4][len:2]   │
          │                     │ [payload]        │
          │                     └────────┬────────┘
          │                              │ ::write(fd)
          │                     ┌────────▼────────┐
          │                     │  file / fifo /   │
          │                     │  unix socket     │
          │                     └─────────────────┘
```

## 1. Main thread — ObjectWriter (`objectwriter.cpp`)

`ObjectWriter` never serializes. It _flattens_ each value into a sequence of
tagged `QEntry` words (one `uintptr_t` each) and pushes them into the SPSC
forward queue.  The low 3 bits of each word encode the tag:

| Tag | Meaning |
|-----|---------|
| `TAG_OBJECT` (0) | `PyObject*` — persister serializes via `MessageStream::write` |
| `TAG_DELETE` (1) | `PyObject*` identity — binding deletion notification (no incref) |
| `TAG_THREAD` (2) | `PyThreadState*` — thread-switch stamp (no incref) |
| `TAG_PICKLED` (3) | `PyObject*` bytes — already pickled on main thread |
| `TAG_NEW_HANDLE` (4) | `PyObject*` — register a new permanent handle |
| `TAG_BIND` (5) | `PyObject*` — bind an internal object |
| `TAG_EXT_BIND` (6) | `PyObject*` — bind an external object |
| `TAG_COMMAND` (7) | Non-pointer: `[len:32][cmd:29][tag:3]` — flush, shutdown, container headers, etc. |

### Container flattening

Container types (list, tuple, dict) are pre-flattened on the main thread:
a `CMD_LIST` / `CMD_TUPLE` / `CMD_DICT` command entry encoding the length is
pushed, followed by each element recursively via `push_value`.  This means
the writer thread never needs to hold a reference to the container itself —
only to the leaf objects.

### Pickle on main thread

Types that can't be natively serialized are pickle-dumped on the main thread
and pushed as `TAG_PICKLED` bytes.  This keeps the writer thread free of
arbitrary Python callbacks for unknown types.

### Immortal objects

Immortal objects (None, True, False, small ints) are pushed without an
incref since their refcount is never modified.  All other objects get a
`Py_INCREF` before entering the queue to keep them alive until the writer
thread finishes with them.

### 32-bit fallback

On 32-bit platforms only 2 tag bits are available, so `TAG_PICKLED`,
`TAG_NEW_HANDLE`, `TAG_BIND`, and `TAG_EXT_BIND` are encoded as a
`TAG_COMMAND` entry followed by a separate `TAG_OBJECT` pointer entry.

## 2. Writer thread — `writer_loop` (`persister.cpp`)

A dedicated `std::thread` spins on the SPSC queue.  When entries appear it
acquires the GIL, drains the queue in a batch, and releases the GIL before
spinning again.  Each entry is dispatched by tag into `MessageStream` which
performs the actual wire-format serialization into a `PrimitiveStream` byte
buffer.

After serializing an object, the writer thread pushes the `PyObject*` into
the _return queue_ instead of calling `Py_DECREF` directly.  This avoids
running Python destructors or GC on the writer thread, which could cascade
into arbitrary Python code.

On `CMD_FLUSH` or when the batch is fully drained, `PrimitiveStream::flush`
is called which writes the buffered bytes through `PidFramedOutput` to the
file descriptor.

### Serialization stack

```
QEntry
  → MessageStream::write(obj)      # type dispatch, binding check, string interning
    → PrimitiveStream::write_*()   # encodes control bytes, varints, raw bytes
      → buffer (vector<uint8_t>)
        → flush → OutputFn(PidFramedOutput::write)
          → [pid:4][len:2][payload] frames → ::write(fd)
```

### Thread identity

`TAG_THREAD` entries carry a raw `PyThreadState*`.  The writer thread
maintains a `thread_cache` mapping `PyThreadState*` → thread handle
(looked up from `PyThreadState.dict` using the ObjectWriter as key).
A `THREAD_SWITCH` marker is written to the stream only when the thread
changes.

## 3. Drain thread — `drain_loop` (`persister.cpp`)

A second `std::thread` consumes the return queue.  It acquires the GIL in
batches, calls `Py_DECREF` on each returned object, and updates the
`total_removed` atomic counter with each object's estimated size.  When many
objects have refcount 1 (triggering deallocation), the drain thread yields
the GIL every 100 µs to avoid starving other threads.

## 4. Backpressure

The `ObjectWriter` tracks an `inflight` estimate: `total_added - total_removed`.
Before pushing each value, if in-flight bytes exceed `inflight_limit`
(default 128 MB), the main thread spins with `std::this_thread::yield()`
until the persister catches up or a timeout expires.  If the timeout fires
the writer disables itself to avoid blocking the application.

Similarly, if the SPSC queue itself is full (capacity exhausted), the main
thread spins on `try_push` with the same timeout behavior.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `inflight_limit` | 128 MB | Max estimated bytes between producer and consumer |
| `stall_timeout` | 5 s | How long to spin before disabling the writer |
| `queue_capacity` | 65536 | Forward SPSC queue capacity (entries) |
| `return_queue_capacity` | 131072 | Return SPSC queue capacity (entries) |

## 5. PID-framed output

`PidFramedOutput` wraps each flush into `[pid:4][len:2][payload]` frames
clamped to ≤ 64 KB so the 2-byte length field always suffices.  After
`fork()`, parent and child each write PID-stamped frames to the same fd
(switched to `O_APPEND`).  The reader demultiplexes by PID.

```
┌──────┬───────┬─────────────────────────────┐
│ PID  │ LEN   │ payload (wire-format bytes)  │
│ 4 B  │ 2 B   │ ≤ 64 KB                     │
└──────┴───────┴─────────────────────────────┘
```

For FIFO outputs the frame size is clamped to `PIPE_BUF` (typically 512 B)
to guarantee atomic writes.  For Unix domain sockets the frame size is
derived from `SO_SNDBUF`.

## Output targets

`AsyncFilePersister` detects the path type at init and adapts:

| Path type | Behavior |
|-----------|----------|
| Regular file | `O_WRONLY \| O_CREAT`, exclusive `flock`, truncate or append mode |
| Named pipe (FIFO) | `O_WRONLY`, no `O_CREAT`, frame size = `PIPE_BUF` |
| Unix socket | `AF_UNIX SOCK_DGRAM`, `connect()`, frame size = `SO_SNDBUF` |

## Lifecycle

```
writer.__init__
  → AsyncFilePersister(path)      # opens fd, acquires flock
  → ObjectWriter.__init__(output) # calls AsyncFilePersister_setup:
      → creates SPSC queues
      → creates PidFramedOutput + MessageStream
      → spawns writer_thread (writer_loop)
      → spawns return_thread (drain_loop)

writer(obj)  /  writer.write_root(obj)
  → ObjectWriter::py_vectorcall
    → send_thread()               # push TAG_THREAD if thread callable set
    → write_root(obj)             # push_value into forward queue

writer.flush()
  → push CMD_FLUSH               # writer thread flushes PrimitiveStream

writer.__exit__  /  writer.disable()
  → push CMD_SHUTDOWN            # writer thread flushes and exits
  → AsyncFilePersister.close()   # joins threads, drains queues, closes fd
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

The **binding check** is critical: if an object has been previously bound
(via `bind()` or `ext_bind()`), it is written as a single varint `BINDING`
reference instead of being re-serialized. This is how object identity is
preserved across the stream.

The **fallback** `write_serialized` calls the Python `serializer` callback.
If the callback returns `bytes`, they are written as `PICKLED`. If it returns
a Python object (e.g. a handle reference), that object is recursively written
via `write()`.
