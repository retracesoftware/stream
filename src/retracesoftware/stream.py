import retracesoftware_stream as _stream
from retracesoftware_stream import *
# import retracesoftware.utils as utils
# import retracesoftware.functional as functional
import retracesoftware.functional as functional

import pickle
import inspect
import os
import threading
import time
import weakref
import sys
import re



def replace_prefix(s, old_prefix, new_prefix):
    return new_prefix + s[len(old_prefix):] if s.startswith(old_prefix) else s

def call_periodically(interval, func):
    ref = weakref.ref(func)
    sleep = time.sleep

    def run():
        while (obj := ref()):
            obj()
            sleep(interval)

    threading.Thread(target = run, args = (), name="Retrace flush tracefile", daemon = True).start()

def extract_frozen_name(filename):
    # This pattern looks for exactly <frozen ...>
    match = re.match(r"<frozen (.+)>", filename)
    
    # If a match is found, return group 1; otherwise return None
    return match.group(1) if match else None

cwd = os.getcwd()

def normalize_path(path):
    frozen_name = extract_frozen_name(path)

    if frozen_name:
        return sys.modules[frozen_name].__file__
    else:
        return replace_prefix(path, cwd + '/', '')

class writer(_stream.ObjectWriter):

    def __init__(self, path, thread, 
                 flush_interval = 0.1,
                 verbose = False, 
                 stacktraces = False, 
                 magic_markers = False):
        
        super().__init__(path, thread = thread, serializer = self.serialize, 
                        verbose = verbose,
                        stacktraces = stacktraces,
                        normalize_path = normalize_path,
                        magic_markers = magic_markers)
        
        self.exclude_from_stacktrace(writer.serialize)
        self.type_serializer = {}
                
        call_periodically(interval = flush_interval, func = self.flush)
    
    def __enter__(self): return self

    def __exit__(self, *args):
        self.keep_open = False

    def serialize(self, obj):
        # TODO, could add memoize one arg for performance
        # s = self.type_serializer.get(type(obj), pickle.dumps)(obj)
        # print(f'Serialized: {obj} to {s}')
        # return s
        return self.type_serializer.get(type(obj), pickle.dumps)(obj)

class reader(_stream.ObjectReader):

    def __init__(self, path, thread, timeout_seconds = 5, verbose = False, on_stack_difference = None, magic_markers = False):

        self.timeout_seconds = timeout_seconds

        super().__init__(path,
                         thread = thread, 
                         on_stack_difference = on_stack_difference,
                         deserializer = self.deserialize,
                         verbose = verbose,
                         normalize_path = normalize_path,
                         magic_markers = magic_markers)
        
        self.exclude_from_stacktrace(reader.__call__)
        self.type_deserializer = {}
    
    def __enter__(self): return self

    def __exit__(self, *args):
        self.close()

    def __call__(self):
        return super().__call__(timeout_seconds = self.timeout_seconds,
                       stacktrace = inspect.stack)
    
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
        pred = is_thread_switch,
        extract = lambda ts: ts.value,
        initial = thread())

    demux = _stream.Demux(key_fn = key_fn, source = source, timeout = timeout)

    is_stack_delta = functional.isinstanceof(StackDelta)

    def create_stack(stack_delta):
        prev = stacks.get(thread(), [])

        common = prev[:-stack_delta.to_drop] if stack_delta.to_drop > 0 else prev
        stack = common + stack_delta.new_frames
        stacks[thread()] = stack
        return Stack(stack)

    expand_stack = functional.if_then_else(is_stack_delta, create_stack, functional.identity)

    return drop(is_thread_switch, functional.sequence(thread, demux, expand_stack))

class reader1(_stream.ObjectStreamReader):

    def __init__(self, path, read_timeout, verbose, magic_markers = False):
        super().__init__(
            path = str(path),
            deserialize = self.deserialize,
            bind_singleton = Bind(self.bind),
            on_thread_switch = ThreadSwitch,
            create_stack_delta = StackDelta,
            read_timeout = read_timeout,
            verbose = verbose,
            magic_markers = magic_markers)

        self.type_deserializer = {}

    def __enter__(self): return self

    def __exit__(self, *args):
        self.close()

    # def __call__(self):
    #     return super().__call__(timeout_seconds = self.timeout_seconds,
    #                    stacktrace = inspect.stack)
    
    def deserialize(self, bytes):
        obj = pickle.loads(bytes)

        if type(obj) in self.type_deserializer:
            return self.type_deserializer[type(obj)](obj)
        else:
            return obj

cwd = os.getcwd()

def stack(exclude):
    return [(normalize_path(filename),lineno) for filename,lineno in _stream.stack(exclude)]

