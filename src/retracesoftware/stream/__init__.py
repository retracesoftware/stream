"""
retracesoftware.stream - Runtime selectable release/debug builds

Set RETRACE_DEBUG=1 to use the debug build with symbols and assertions.
"""
import os
import pickle
import inspect
import threading
import time
import weakref
import sys
import re
from types import ModuleType
from typing import Any

import retracesoftware.functional as functional


def _is_truthy_env(v):
    if v is None:
        return False
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}

_DEBUG_MODE = _is_truthy_env(os.getenv("RETRACE_DEBUG"))

_backend_mod: ModuleType
__backend__: str

try:
    if _DEBUG_MODE:
        import _retracesoftware_stream_debug as _backend_mod  # type: ignore
        __backend__ = "native-debug"
    else:
        import _retracesoftware_stream_release as _backend_mod  # type: ignore
        __backend__ = "native-release"
except Exception:
    raise ImportError("Failed to load retracesoftware_stream native extension")

# Expose debug mode flag
DEBUG_MODE = _DEBUG_MODE and __backend__.startswith("native")

def __getattr__(name: str) -> Any:
    return getattr(_backend_mod, name)

def _export_public(mod: ModuleType) -> None:
    g = globals()
    for k, v in mod.__dict__.items():
        if k.startswith("_"):
            continue
        g[k] = v

_export_public(_backend_mod)

# ---------------------------------------------------------------------------
# High-level API (convenience wrappers around C++ extension)
# ---------------------------------------------------------------------------

def replace_prefix(s, old_prefix, new_prefix):
    return new_prefix + s[len(old_prefix):] if s.startswith(old_prefix) else s

def call_periodically(interval, func):
    ref = weakref.ref(func)
    sleep = time.sleep

    def run():
        while (obj := ref()):
            obj()
            sleep(interval)

    threading.Thread(target=run, args=(), name="Retrace flush tracefile", daemon=True).start()

def extract_frozen_name(filename):
    match = re.match(r"<frozen (.+)>", filename)
    return match.group(1) if match else None

_cwd = os.getcwd()

def normalize_path(path):
    frozen_name = extract_frozen_name(path)
    if frozen_name:
        return sys.modules[frozen_name].__file__
    else:
        return replace_prefix(path, _cwd + '/', '')


class writer(_backend_mod.ObjectWriter):

    def __init__(self, path, thread, 
                 flush_interval=0.1,
                 verbose=False, 
                 stacktraces=False, 
                 magic_markers=False):
        
        super().__init__(path, thread=thread, serializer=self.serialize, 
                        verbose=verbose,
                        stacktraces=stacktraces,
                        normalize_path=normalize_path,
                        magic_markers=magic_markers)
        
        self.exclude_from_stacktrace(writer.serialize)
        self.type_serializer = {}
                
        call_periodically(interval=flush_interval, func=self.flush)
    
    def __enter__(self): return self

    def __exit__(self, *args):
        self.keep_open = False

    def serialize(self, obj):
        return self.type_serializer.get(type(obj), pickle.dumps)(obj)


class reader(_backend_mod.ObjectReader):

    def __init__(self, path, thread, timeout_seconds=5, verbose=False, on_stack_difference=None, magic_markers=False):
        self.timeout_seconds = timeout_seconds

        super().__init__(path,
                         thread=thread, 
                         on_stack_difference=on_stack_difference,
                         deserializer=self.deserialize,
                         verbose=verbose,
                         normalize_path=normalize_path,
                         magic_markers=magic_markers)
        
        self.exclude_from_stacktrace(reader.__call__)
        self.type_deserializer = {}
    
    def __enter__(self): return self

    def __exit__(self, *args):
        self.close()

    def __call__(self):
        return super().__call__(timeout_seconds=self.timeout_seconds,
                       stacktrace=inspect.stack)
    
    def deserialize(self, bytes):
        obj = pickle.loads(bytes)
        if type(obj) in self.type_deserializer:
            return self.type_deserializer[type(obj)](obj)
        else:
            return obj


bind = object()


class StickyPred:
    def __init__(self, pred, extract, initial):
        self.pred = pred
        self.extract = extract
        self.value = initial

    def __call__(self, obj):
        if self.pred(obj):
            self.value = self.extract(obj)
        return self.value


def drop(pred, source):
    def f():
        obj = source()
        return f() if pred(obj) else obj
    return f


class Control:
    def __init__(self, value):
        self.value = value


class Bind(Control):
    pass


class StackDelta:
    __slots__ = ['to_drop', 'new_frames']

    def __init__(self, to_drop, new_frames):
        self.to_drop = to_drop
        self.new_frames = new_frames


class Stack(Control):
    pass


class ThreadSwitch(Control):
    pass


def per_thread(source, thread, timeout):
    stacks = {}
    is_thread_switch = functional.isinstanceof(ThreadSwitch)
    
    key_fn = StickyPred(
        pred=is_thread_switch,
        extract=lambda ts: ts.value,
        initial=thread())

    demux = _backend_mod.Demux(key_fn=key_fn, source=source, timeout=timeout)
    is_stack_delta = functional.isinstanceof(StackDelta)

    def create_stack(stack_delta):
        prev = stacks.get(thread(), [])
        common = prev[:-stack_delta.to_drop] if stack_delta.to_drop > 0 else prev
        stack = common + stack_delta.new_frames
        stacks[thread()] = stack
        return Stack(stack)

    expand_stack = functional.if_then_else(is_stack_delta, create_stack, functional.identity)
    return drop(is_thread_switch, functional.sequence(thread, demux, expand_stack))


class reader1(_backend_mod.ObjectStreamReader):

    def __init__(self, path, read_timeout, verbose, magic_markers=False):
        super().__init__(
            path=str(path),
            deserialize=self.deserialize,
            bind_singleton=Bind(self.bind),
            on_thread_switch=ThreadSwitch,
            create_stack_delta=StackDelta,
            read_timeout=read_timeout,
            verbose=verbose,
            magic_markers=magic_markers)

        self.type_deserializer = {}

    def __enter__(self): return self

    def __exit__(self, *args):
        self.close()
    
    def deserialize(self, bytes):
        obj = pickle.loads(bytes)
        if type(obj) in self.type_deserializer:
            return self.type_deserializer[type(obj)](obj)
        else:
            return obj


def stack(exclude):
    return [(normalize_path(filename), lineno) for filename, lineno in _backend_mod.stack(exclude)]


__all__ = sorted([k for k in globals().keys() if not k.startswith("_")])
