# Container Serialization: Issues and Tradeoffs

This document captures the design considerations around serializing Python
container objects (dicts, lists) in the retrace recording stream.

## The Problem

Python containers can be arbitrarily nested and self-referential:

```python
d = {"config": {"db": {"host": "localhost", "port": 5432}}}
d["self"] = d  # cycle
```

The stream serializer recursively writes container elements. Without
protection, a self-referential container blows the C stack.

## Current Design

### Depth guard (stream layer)

`write_dict` and `write_list` in `MessageStream` track a `write_depth`
counter. When depth reaches `MAX_WRITE_DEPTH` (64), the container is
handed off to `pickle.dumps` instead of continuing recursive
serialization. Pickle handles cycles natively via its memo table.

- The guard only applies to dict and list -- the only mutable containers
  that can hold self-references. Scalars, tuples, bytes, and bound objects
  are never depth-checked (zero overhead on the hot path).
- If pickle itself fails (e.g. unpicklable object), `None` is written as
  a last-resort fallback.
- The reader side already handles the `PICKLED` wire format via
  `pickle.loads`.

### Why depth 64?

Real-world nesting in retrace streams is almost always shallow (< 5
levels). Function arguments are flat primitives, short lists, and shallow
dicts. Complex objects go through the binding/proxy system as handle
references, not recursive serialization. A limit of 64 is generous enough
to never interfere with legitimate data while stopping pathological cases.

## Future Direction: One-Level Serialization at the Proxy Layer

A potentially cleaner approach is to limit container serialization to a
single level at the **proxy layer** rather than relying on depth guards in
the stream serializer.

### The idea

When the proxy layer encounters a dict or list, serialize its immediate
keys/values (or elements), but if any value is itself a dict or list,
**bind it as a proxy handle** instead of inlining it.

```
# What gets written for {"a": 1, "nested": {"x": 2}}:
#   DICT(2)
#     "a" -> 1            (primitive, serialized inline)
#     "nested" -> HANDLE   (dict, bound as proxy)
```

### Advantages

- **Cycle problem vanishes**: a self-referential dict writes a handle for
  the back-reference, not a recursive serialization.
- **Bounded serialization cost**: one level of container is always cheap.
  A dict with 1000 keys of primitives serializes in bounded space.
- **Smaller traces**: nested containers that are never accessed on replay
  are never serialized. Only pay for what replay actually touches.
- **Lazy replay**: nested containers materialize on demand during replay.
- **Predictable performance**: serialization cost is proportional to the
  number of top-level entries, never to total tree depth.

### Concerns

#### C extensions expecting real dicts

If a C extension calls `PyDict_GetItem` directly on a value, it expects a
real `PyDict`, not a proxy. However:

- Most well-written C extensions use the abstract mapping protocol
  (`PyObject_GetItem`, `PyMapping_GetItemString`) which respects
  `__getitem__` and works with dict-like objects.
- `PyDict_Check` passes for dict subclasses, so a proxy that subclasses
  `dict` would satisfy the check.
- The `PyDict_*` fast-path APIs are mainly used by CPython internals
  (module dicts, frame locals, `**kwargs`), not for user-provided
  arguments.

#### C extensions mutating passed-in dicts

If a C extension adds/removes keys from a dict argument, a proxy would
need to capture those mutations. However:

- Mutating a dict passed as an argument is an anti-pattern in Python.
- No well-known C extension libraries (database drivers, serializers,
  HTTP clients, numpy, etc.) follow this pattern for user-provided dicts.
- `**kwargs` dicts are always real dicts created by the interpreter.

#### Nested dicts in C extension I/O

How common are nested dicts as C extension inputs or outputs?

- **Database drivers** (psycopg2, sqlite3): flat parameter dicts, tuple
  results. Never nested.
- **Serialization** (json, msgpack): `json.dumps` receives nested dicts
  but uses the abstract protocol. Retrace typically traces the caller, not
  json itself.
- **HTTP/network** (requests, urllib3): headers and params are flat dicts.
- **numpy/scipy/pandas**: dtype specs and options are flat.
- **gRPC/protobuf**: metadata is flat key-value pairs. Messages are
  protobuf classes (proxied as objects).

Nested dicts at C extension boundaries are extremely rare. The one-level
rule would cover essentially all real-world C extension interactions.

### Tradeoff

Small nested containers (`{"a": {"x": 1}}`) add marginal proxy overhead
where inlining would be slightly cheaper. But this overhead is tiny
compared to the wins on large or cyclic structures, and deep nesting is
rare in recording streams.

## Summary

| Approach | Handles cycles | Trace size | Complexity | Status |
|---|---|---|---|---|
| Depth guard + pickle fallback | Yes (via pickle memo) | Unbounded for deep trees | Low (stream layer only) | **Implemented** |
| One-level proxy serialization | Yes (handles are natural) | Bounded | Medium (proxy layer change) | **Future consideration** |

The depth guard is the current safety net. The one-level proxy approach is
a natural evolution that would make the depth guard effectively
unreachable while providing better trace size and replay performance.
