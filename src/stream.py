import retracesoftware_stream as _stream
from retracesoftware_stream import *
import pickle

class writer(_stream.ObjectWriter):

    def __init__(self, path):
        super().__init__(path = path, serializer = self.serialize)
        self.type_serializer = {}

    def serialize(self, obj):
        # TODO, could add memoize one arg for performance
        return self.type_serializer.get(type(obj), pickle.dumps)(obj)

class reader(_stream.ObjectReader):
    def __init__(self, path):
        super().__init__(path = path, deserializer = self.deserialize)

        self.type_deserializer = {}

    def deserialize(self, bytes):
        obj = pickle.loads(bytes)

        if type(obj) in self.type_deserializer:
            return self.type_deserializer[type(obj)](obj)
        else:
            return obj

    def __call__(self):
        obj = super().__call__()
        print(f'stream.reader: {obj}')
        return obj
