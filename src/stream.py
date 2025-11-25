import retracesoftware_stream as _stream
from retracesoftware_stream import *
# import retracesoftware.utils as utils
# import retracesoftware.functional as functional

import pickle
import inspect
import itertools
import ast
import sys
import importlib
from typing import Optional

def frozen_to_module(name: str):
    """
    Given a frozen filename like "<frozen importlib._bootstrap>",
    return the actual module object (or None if not frozen).
    """
    if not name.startswith("<frozen ") or not name.endswith(">"):
        return None

    # Extract the part between <frozen ...>
    module_name = name[len("<frozen "):-1]

    # Special case: the stdlib ships some as _frozen_importlib and _frozen_importlib_external
    if module_name == "importlib._bootstrap":
        return sys.modules.get("_frozen_importlib") or importlib._bootstrap
    if module_name == "importlib._bootstrap_external":
        return sys.modules.get("_frozen_importlib_external") or importlib._bootstrap_external

    # Everything else is directly importable
    return importlib.import_module(module_name)
    
def get_source(filename):
    try:
        with open(filename, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return inspect.getsource(frozen_to_module(filename))

def get_function_at_line(filename: str, source: str, lineno: int) -> Optional[str]:
    """
    Return the name of the function (or class/method) that contains the given line number.
    
    Returns None if:
      - file not found
      - syntax error
      - line is outside any function (module level)
    """
    # except FileNotFoundError:
    #     return None
    # except UnicodeDecodeError:
    #     return None

    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError:
        return None

    class FunctionFinder(ast.NodeVisitor):
        def __init__(self, target_lineno: int):
            self.target_lineno = target_lineno
            self.current: Optional[str] = None
            self.stack: list[str] = []

        def visit_FunctionDef(self, node):
            if node.lineno <= self.target_lineno <= node.end_lineno:
                # breakpoint()
                self.stack.append(node.name)
                self.current = node.name
                self.generic_visit(node)
                self.stack.pop()
            elif self.stack:
                # We're inside a nested function/class, keep traversing
                self.generic_visit(node)

        def visit_AsyncFunctionDef(self, node):
            self.visit_FunctionDef(node)  # same logic

        def visit_ClassDef(self, node):
            if node.lineno <= self.target_lineno <= node.end_lineno:
                self.stack.append(node.name)
                # Don't set current here — we prefer method names
                self.generic_visit(node)
                self.stack.pop()

        # def get_result(self) -> Optional[str]:
        #     if not self.stack:
        #         return None
        #     # Return the deepest (most specific) function/method name
        #     return self.current
        #     # return self.stack[-1]

    finder = FunctionFinder(lineno)
    finder.visit(tree)
    # print(f'foind: {finder.current}')
    return finder.current

class writer(_stream.ObjectWriter):

    def __init__(self, path, thread, verbose = False, stacktraces = False):

        super().__init__(path, thread = thread, serializer = self.serialize, 
                        verbose = verbose,
                        stacktraces = stacktraces)
        
        self.exclude_from_stacktrace(writer.serialize)
        self.type_serializer = {}
    
    def serialize(self, obj):
        # TODO, could add memoize one arg for performance
        # s = self.type_serializer.get(type(obj), pickle.dumps)(obj)
        # print(f'Serialized: {obj} to {s}')
        # return s
        return self.type_serializer.get(type(obj), pickle.dumps)(obj)

def all_elements_same(t):
    return len(set(t)) <= 1

def first(coll): return coll[0]

def common_prefix(*colls):
    return list(map(first, itertools.takewhile(all_elements_same, zip(*colls))))

def render_stack(frames):
    for filename,lineno in frames:
        try:
            source = get_source(filename)
            print(f'File "{filename}", line {lineno}, in {get_function_at_line(filename, source, lineno)}')
            print(f'  {source.splitlines()[lineno - 1].lstrip()}')
        except Exception:
            print(f'File not found: "{filename}", line {lineno}')

class reader(_stream.ObjectReader):

    def __init__(self, path, thread, timeout_seconds = 5, verbose = False):

        self.timeout_seconds = timeout_seconds

        excludes = set([('/install/retracesoftware/proxy/replay.py', 210),
                        ('/install/retracesoftware/proxy/replay.py', 175),
                        ('/install/retracesoftware/stream.py', 176),
                        ('/install/retracesoftware/stream.py', 179)])

        def on_stack_difference(previous, record, replay):

            previous = [x for x in previous if x not in excludes]
            record = [x for x in record if x not in excludes]
            replay = [x for x in replay if x not in excludes]

            if record != replay:
                common = common_prefix(previous, record, replay) if previous else common_prefix(record, replay)
                
                if common:
                    print('Common root:')
                    render_stack(common)

                if previous:
                    print('\nlast matching:')
                    render_stack(previous[len(common):])

                print('\nrecord:')
                render_stack(record[len(common):])

                print('\nreplay:')
                render_stack(replay[len(common):])

                breakpoint()
                print(f'on_stack_difference!!!!!')

        super().__init__(path,
                         thread = thread, 
                         on_stack_difference = on_stack_difference,
                         deserializer = self.deserialize,
                         verbose = verbose)
        
        self.exclude_from_stacktrace(reader.__call__)
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
