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
except Exception as e:
    raise ImportError(f"Failed to load retracesoftware_stream native extension: {e}") from e

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


def normalize_path(path):
    """
    Normalize a path to an absolute path for recording.
    
    Recording stores absolute paths. Replay uses path_info from settings.json
    to map paths appropriately.
    """
    # Handle frozen modules - get their actual file path
    frozen_name = extract_frozen_name(path)
    if frozen_name:
        mod = sys.modules.get(frozen_name)
        if mod and hasattr(mod, '__file__') and mod.__file__:
            return os.path.realpath(mod.__file__)
        return path  # fallback to original if module not found
    
    # Return canonical absolute path
    try:
        return os.path.realpath(path)
    except (OSError, TypeError):
        return path


def get_path_info():
    """Get current path info for recording in settings.json."""
    try:
        cwd = os.path.realpath(os.getcwd())
    except OSError:
        cwd = None
    
    real_sys_path = []
    for p in sys.path:
        if p:
            try:
                real_p = os.path.realpath(p)
                if real_p:
                    real_sys_path.append(real_p)
            except OSError:
                pass
    
    return {
        'cwd': cwd,
        'sys_path': real_sys_path,
    }


class writer(_backend_mod.ObjectWriter):

    def __init__(self, path, thread, 
                 flush_interval=0.1,
                 verbose=False, 
                 magic_markers=False):
        
        super().__init__(path, thread=thread, serializer=self.serialize, 
                        verbose=verbose,
                        normalize_path=normalize_path,
                        magic_markers=magic_markers)
        
        self.type_serializer = {}
                
        call_periodically(interval=flush_interval, func=self.flush)
    
    def __enter__(self): return self

    def __exit__(self, *args):
        self.keep_open = False

    def serialize(self, obj):
        return self.type_serializer.get(type(obj), pickle.dumps)(obj)


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


class ThreadSwitch(Control):
    pass


def per_thread(source, thread, timeout):
    import retracesoftware.utils as utils

    is_thread_switch = functional.isinstanceof(ThreadSwitch)
    
    key_fn = StickyPred(
        pred=is_thread_switch,
        extract=lambda ts: ts.value,
        initial=thread())

    demux = utils.demux(source=source, key_function=key_fn, timeout_seconds=timeout)
    return drop(is_thread_switch, functional.sequence(thread, demux))


class reader1(_backend_mod.ObjectStreamReader):

    def __init__(self, path, read_timeout, verbose, magic_markers=False):
        super().__init__(
            path=str(path),
            deserialize=self.deserialize,
            bind_singleton=Bind(self.bind),
            on_thread_switch=ThreadSwitch,
            create_stack_delta=lambda to_drop, frames: None,
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


__all__ = sorted([k for k in globals().keys() if not k.startswith("_")])
