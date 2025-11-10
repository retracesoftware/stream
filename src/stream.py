import retracesoftware_stream as _stream
from retracesoftware_stream import *
# import retracesoftware.utils as utils
# import retracesoftware.functional as functional

import pickle
import inspect

class writer(_stream.ObjectWriter):

    def __init__(self, path, thread, verbose = False):
        super().__init__(path, thread = thread, serializer = self.serialize, verbose = verbose)
        self.type_serializer = {}
    
    def serialize(self, obj):
        # TODO, could add memoize one arg for performance
        # s = self.type_serializer.get(type(obj), pickle.dumps)(obj)
        # print(f'Serialized: {obj} to {s}')
        # return s
        return self.type_serializer.get(type(obj), pickle.dumps)(obj)
    
class reader(_stream.ObjectReader):

    def __init__(self, path, thread, timeout_seconds = 5, verbose = False):

        self.timeout_seconds = timeout_seconds

        super().__init__(path, 
                         thread = thread, 
                         deserializer = self.deserialize,
                         verbose = verbose)
        
        self.type_deserializer = {}
    
    def __call__(self):
        return super().__call__(timeout_seconds = self.timeout_seconds,
                       stacktrace = inspect.stack)
    
    def deserialize(self, bytes):
        obj = pickle.loads(bytes)

        if type(obj) in self.type_deserializer:
            return self.type_deserializer[type(obj)](obj)
        else:
            return obj
