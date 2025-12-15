import retracesoftware_stream as _stream
from retracesoftware_stream import *
# import retracesoftware.utils as utils
# import retracesoftware.functional as functional

import pickle
import inspect
import os
import threading
import time
import weakref

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

class writer(_stream.ObjectWriter):

    def __init__(self, path, thread, 
                 flush_interval = 0.1,
                 verbose = False, 
                 stacktraces = False, 
                 magic_markers = False):
        
        cwd = os.getcwd()

        def normalize_path(path):
            return replace_prefix(path, cwd + '/', '')

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

        cwd = os.getcwd()

        def normalize_path(path):
            return replace_prefix(path, cwd + '/', '')

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
