import retracesoftware_stream as _stream
from retracesoftware_stream import *
# import retracesoftware_utils as utils
# import retracesoftware.functional as functional

import pickle

class writer(_stream.ObjectWriter):

    def __init__(self, path):
        super().__init__(path, serializer = self.serialize)
        self.type_serializer = {}
    
    def serialize(self, obj):
        # TODO, could add memoize one arg for performance
        return self.type_serializer.get(type(obj), pickle.dumps)(obj)
    
class reader(_stream.ObjectReader):

    def __init__(self, path):
        super().__init__(path, deserializer = self.deserialize)
        self.type_deserializer = {}
    
        # def read():
        #     current = None
        #     obj = self.impl()
        #     while isinstance(obj, ThreadSwitch):
        #         current = obj.id
        #         obj = self.impl()
        #     return (current, obj)
        
        # demux = utils.demux(source = read, key_function = lambda obj: obj[0])

        # self.read = functional.repeatedly(lambda: demux(utils.thread_id())[1])

    def deserialize(self, bytes):
        obj = pickle.loads(bytes)

        if type(obj) in self.type_deserializer:
            return self.type_deserializer[type(obj)](obj)
        else:
            return obj
