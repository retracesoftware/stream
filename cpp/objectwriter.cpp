#include "stream.h"
#include "writer.h"

#include <cstddef>
#include <cstdint>
#include <structmember.h>
#include "wireformat.h"
#include <algorithm>
#include <vector>
#include "unordered_dense.h"
#include "base.h"

using namespace ankerl::unordered_dense;

#ifdef _WIN32
    #include <process.h>
    #include <windows.h>
    #define getpid _getpid
#else
    #include <unistd.h>
#endif

int pid() {
#ifdef _WIN32
    return static_cast<int>(GetCurrentProcessId());
#else
    return static_cast<int>(getpid());
#endif
}

namespace retracesoftware_stream {

    struct ObjectWriter;

    static std::vector<ObjectWriter *> writers;

    static void on_free(void * obj);

    struct StreamHandle : public PyObject {
        int index;
        PyObject * writer;
        PyObject * object;
        vectorcallfunc vectorcall;
        
        static int traverse(StreamHandle* self, visitproc visit, void* arg) {
            Py_VISIT(self->writer);
            Py_VISIT(self->object);
            return 0;
        }

        static int clear(StreamHandle* self) {
            Py_CLEAR(self->writer);
            Py_CLEAR(self->object);
            return 0;
        }
    };
    
    struct WeakRefCallback : public PyObject {
        PyObject * handle;
        PyObject * writer;
        vectorcallfunc vectorcall;
        
        static int traverse(StreamHandle* self, visitproc visit, void* arg) {
            Py_VISIT(self->writer);
            return 0;
        }

        static int clear(StreamHandle* self) {
            Py_CLEAR(self->writer);
            return 0;
        }
    };

    static thread_local bool writing = false;

    class Writing {
    private:
        bool previous;

    public:
        Writing() : previous(writing) {
            writing = true;
        }

        ~Writing() {
            writing = previous;
        }

        inline Writing(const Writing&) = delete;
        inline Writing& operator=(const Writing&) = delete;
        inline Writing(Writing&&) = delete;
        inline Writing& operator=(Writing&&) = delete;
    };

    struct ObjectWriter : public ReaderWriterBase {
        
        MessageStream stream;

        size_t messages_written = 0;
        int next_handle;
        PyThreadState * last_thread_state = nullptr;
        int pid;
        bool verbose;
        bool buffer_writes = true;
        PyObject * enable_when;
        retracesoftware::FastCall thread;
        vectorcallfunc vectorcall;
        PyObject *weakreflist;
        bool magic_markers;

        // Output callback is None or null when disabled (no-op mode)
        inline bool is_disabled() const { return stream.is_closed(); }

        void write_magic() { 
            if (magic_markers) {
                stream.write_magic();
            }
        }

        void debug_prefix(size_t bytes_written = 0) {
            if (bytes_written == 0) {
                bytes_written = stream.get_bytes_written();
                printf("Retrace(%i) - ObjectWriter[%lu, %lu] -- ", ::pid(), messages_written, bytes_written);
            } else {
                printf("Retrace(%i) - ObjectWriter[%lu, %lu, %lu] -- ", ::pid(), messages_written, bytes_written, stream.get_bytes_written());
            }
        }

        static PyObject * StreamHandle_vectorcall(StreamHandle * self, PyObject *const * args, size_t nargsf, PyObject* kwnames) {
            
            ObjectWriter * writer = reinterpret_cast<ObjectWriter *>(self->writer);

            if (writer->is_disabled()) Py_RETURN_NONE;

            try {
                writer->write_all(self, args, PyVectorcall_NARGS(nargsf));
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }
        
        void bind(PyObject * obj, bool ext) {
            if (is_disabled()) return;

            check_thread();

            Writing w;

            if (verbose) {
                debug_prefix();
                const char * type = ext ? "EXT_BIND" : "BIND";
                printf("%s(%s)\n", type, Py_TYPE(obj)->tp_name);
            }

            stream.bind(obj, ext);
            messages_written++;
            write_magic();
        }

        void write_delete(int id) {
            if (is_disabled()) return;

            if (verbose) {
                debug_prefix();
                printf("DELETE(%i)\n", id);
            }
            int delta = next_handle - id;

            assert (delta > 0);

            stream.write_handle_delete(delta - 1);
            messages_written++;
        }

        static void StreamHandle_dealloc(StreamHandle* self) {
            
            ObjectWriter * writer = reinterpret_cast<ObjectWriter *>(self->writer);

            // Guard against unsafe dealloc during interpreter shutdown:
            //  - writer may be NULL if GC called tp_clear before tp_dealloc
            //  - writer may be disabled if its output was cleared
            //  - during finalization, write_delete calls back into Python
            //    (PyObject_Str) which is unsafe as objects are
            //    being torn down in unpredictable order
            if (writer && !writer->is_disabled() && !_Py_IsFinalizing()) {
                writer->write_delete(self->index);
            }

            PyObject_GC_UnTrack(self);
            StreamHandle::clear(self);
            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
        }

        PyObject * stream_handle(int index, PyObject * obj) {

            StreamHandle * self = (StreamHandle *)StreamHandle_Type.tp_alloc(&StreamHandle_Type, 0);
            if (!self) return nullptr;

            self->writer = Py_NewRef(this);
            self->index = index;
            self->vectorcall = (vectorcallfunc)StreamHandle_vectorcall;
            self->object = Py_XNewRef(obj);

            return (PyObject *)self;
        }

        PyObject * handle(PyObject * obj) {
            // Return a handle even when disabled (for API compatibility)
            // The handle's vectorcall will no-op
            if (is_disabled()) {
                return stream_handle(next_handle++, nullptr);
            }

            if (verbose) {
                debug_prefix();
                PyObject * str = PyObject_Str(obj);
                printf("NEW_HANDLE(%s)\n", PyUnicode_AsUTF8(str));
                Py_DECREF(str);
            }
            stream.write_new_handle(obj);
            messages_written++;
            return stream_handle(next_handle++, verbose ? obj : nullptr);
        }

        inline size_t get_bytes_written() const { return stream.get_bytes_written(); }

        void write_root(StreamHandle * obj) {
            if (verbose) {
                debug_prefix();
                PyObject * str = PyObject_Str(obj->object);
                printf("%s\n", PyUnicode_AsUTF8(str));
                Py_DECREF(str);
            }
            stream.write_stream_handle(obj);
            write_magic();
            messages_written++;
            stream.mark_message_boundary();
        }

        void write_root(PyObject * obj) {
            size_t before = stream.get_bytes_written();
            stream.write(obj);
            if (verbose) {
                debug_prefix(before);
                PyObject * str = PyObject_Str(obj);
                printf("%s\n", PyUnicode_AsUTF8(str));
                Py_DECREF(str);
            }
            write_magic();
            messages_written++;
            stream.mark_message_boundary();
        }

        void object_freed(PyObject * obj) {
            if (is_disabled()) return;

            if (stream.object_freed(obj)) {
                messages_written++;
            }
        }

        void write_all(StreamHandle * self, PyObject *const * args, size_t nargs) {

            check_thread();

            Writing w;

            write_root(self);
            for (size_t i = 0; i < nargs; i++) {
                write_root(args[i]);
            }
        }

        void write_all(PyObject*const * args, size_t nargs) {

            check_thread();

            Writing w;

            for (size_t i = 0; i < nargs; i++) {
                write_root(args[i]);
            }
        }

        bool enabled() {
            if (enable_when) {
                PyObject * result = PyObject_CallNoArgs(enable_when);
                if (!result) throw nullptr;
                int is_true = PyObject_IsTrue(result);
                Py_DECREF(result);

                switch(is_true) {
                    case 1: 
                        Py_DECREF(enable_when);
                        enable_when = nullptr;
                        return true;
                    case 0:
                        return false;
                    default:
                        throw nullptr;
                }
            }
            return true;
        }

        static PyObject* py_vectorcall(ObjectWriter* self, PyObject*const * args, size_t nargsf, PyObject* kwnames) {

            if (kwnames) {
                PyErr_SetString(PyExc_TypeError, "ObjectWriter does not accept keyword arguments");
                return nullptr;
            }
         
            try {
                uint64_t dropped = self->stream.get_dropped_messages();
                if (dropped > 0) {
                    self->stream.reset_dropped_messages();
                    self->stream.write_dropped_marker(dropped);
                    self->stream.mark_message_boundary();
                }
                self->write_all(args, PyVectorcall_NARGS(nargsf));
                if (!self->buffer_writes) self->stream.flush();
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_flush(ObjectWriter * self, PyObject* unused) {
            try {
                self->stream.flush();
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_handle(ObjectWriter * self, PyObject* obj) {
            try {
                return self->handle(obj);
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_bind(ObjectWriter * self, PyObject* obj) {
            try {
                self->bind(obj, false);
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_ext_bind(ObjectWriter * self, PyObject* obj) {
            try {
                self->bind(obj, true);
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        static int init(ObjectWriter * self, PyObject* args, PyObject* kwds) {

            PyObject * output;
            
            PyObject * thread = nullptr;
            PyObject * normalize_path = nullptr;
            PyObject * serializer = nullptr;

            int verbose = 0;
            int magic_markers = 0;
            
            static const char* kwlist[] = {
                "output", 
                "serializer", 
                "thread", 
                "verbose", 
                "normalize_path",
                "magic_markers",
                nullptr};

            if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO|OpOp", (char **)kwlist, 
                &output, &serializer, &thread, &verbose, &normalize_path, &magic_markers)) {
                return -1;  
            }

            self->verbose = verbose;
            self->magic_markers = magic_markers;
            self->buffer_writes = true;

            self->path = Py_NewRef(Py_None);
            self->thread = thread ? retracesoftware::FastCall(thread) : retracesoftware::FastCall();
            Py_XINCREF(thread);

            self->messages_written = 0;
            self->next_handle = 0;
            
            self->vectorcall = reinterpret_cast<vectorcallfunc>(ObjectWriter::py_vectorcall);

            self->normalize_path = Py_XNewRef(normalize_path);
            self->enable_when = nullptr;

            if (output != Py_None) {
                try {
                    new (&self->stream) MessageStream(output, serializer);
                } catch (...) {
                    return -1;
                }
            }

            writers.push_back(self);

            return 0;
        }

        void check_thread() {
            if (is_disabled()) return;

            PyThreadState * tstate = PyThreadState_Get();

            if (thread.callable && last_thread_state != tstate) {
                last_thread_state = tstate;

                PyObject * thread_handle = PyDict_GetItem(PyThreadState_GetDict(), this);

                if (!thread_handle) {
                    PyObject * id = thread();
                    if (!id) throw nullptr;
                    
                    thread_handle = handle(id);
                    Py_DECREF(id);

                    PyDict_SetItem(PyThreadState_GetDict(), this, thread_handle);
                    Py_DECREF(thread_handle);
                }
                if (verbose) {
                    PyObject * str = PyObject_Str(reinterpret_cast<StreamHandle *>(thread_handle)->object);
                    printf("Retrace - ObjectWriter[%lu] -- THREAD_SWITCH(%s)\n", messages_written, PyUnicode_AsUTF8(str));
                    Py_DECREF(str);
                }
                stream.write_thread_switch(thread_handle);
                messages_written++;
            }
        }

        static int traverse(ObjectWriter* self, visitproc visit, void* arg) {
            self->stream.traverse(visit, arg);
            Py_VISIT(self->thread.callable);
            Py_VISIT(self->path);
            Py_VISIT(self->normalize_path);
            return 0;
        }

        static int clear(ObjectWriter* self) {
            self->stream.gc_clear();
            Py_CLEAR(self->thread.callable);
            Py_CLEAR(self->path);
            Py_CLEAR(self->normalize_path);
            return 0;
        }

        static void dealloc(ObjectWriter* self) {
            self->stream.close();
            
            if (self->weakreflist != NULL) {
                PyObject_ClearWeakRefs((PyObject *)self);
            }

            PyObject_GC_UnTrack(self);
            clear(self);

            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));

            auto it = std::find(writers.begin(), writers.end(), self);
            if (it != writers.end()) {
                writers.erase(it);
            }
        }

        static PyObject * bytes_written_getter(ObjectWriter *self, void *closure) {
            return PyLong_FromLong(self->stream.get_bytes_written());
        }

        static PyObject * path_getter(ObjectWriter *self, void *closure) {
            return Py_NewRef(self->path);
        }

        static int path_setter(ObjectWriter *self, PyObject *value, void *closure) {
            if (value == nullptr) {
                PyErr_SetString(PyExc_AttributeError, "deletion of 'path' is not allowed");
                return -1;
            }

            Py_DECREF(self->path);
            self->path = Py_NewRef(value);
            return 0;
        }

        static PyObject * output_getter(ObjectWriter *self, void *closure) {
            PyObject* cb = self->stream.get_output_callback();
            return Py_NewRef(cb ? cb : Py_None);
        }

        static int output_setter(ObjectWriter *self, PyObject *value, void *closure) {
            if (value == nullptr) {
                PyErr_SetString(PyExc_AttributeError, "deletion of 'output' is not allowed");
                return -1;
            }
            self->stream.set_output_callback(value == Py_None ? nullptr : value);
            return 0;
        }

        static PyObject * drop_mode_getter(ObjectWriter *self, void *closure) {
            return PyBool_FromLong(self->stream.get_drop_mode());
        }

        static int drop_mode_setter(ObjectWriter *self, PyObject *value, void *closure) {
            if (value == nullptr) {
                PyErr_SetString(PyExc_AttributeError, "deletion of 'drop_mode' is not allowed");
                return -1;
            }
            int truthy = PyObject_IsTrue(value);
            if (truthy < 0) return -1;
            self->stream.set_drop_mode(truthy);
            return 0;
        }
    };

    static void on_free(void * obj) {
        for (ObjectWriter * writer : writers) {
            writer->object_freed((PyObject *)obj);
        }
    }

    PyMemberDef StreamHandle_members[] = {
        {"index", T_ULONGLONG, OFFSET_OF_MEMBER(StreamHandle, index), READONLY, "TODO"},
        {NULL}
    };

    int StreamHandle_index(PyObject * streamhandle) {
        return reinterpret_cast<StreamHandle *>(streamhandle)->index;
    }

    // --- BufferSlot type ---

    PyTypeObject BufferSlot_Type = {
        .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = MODULE "BufferSlot",
        .tp_basicsize = sizeof(BufferSlot),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)BufferSlot::dealloc,
        .tp_as_buffer = &BufferSlot_as_buffer,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Fixed-size buffer slot for serialization output",
    };

    // --- StreamHandle type ---

    PyTypeObject StreamHandle_Type = {
        .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = MODULE "StreamHandle",
        .tp_basicsize = sizeof(StreamHandle),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)ObjectWriter::StreamHandle_dealloc,
        .tp_vectorcall_offset = OFFSET_OF_MEMBER(StreamHandle, vectorcall),
        .tp_call = PyVectorcall_Call,
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_HAVE_VECTORCALL,
        .tp_doc = "TODO",
        .tp_traverse = (traverseproc)StreamHandle::traverse,
        .tp_clear = (inquiry)StreamHandle::clear,
        .tp_members = StreamHandle_members,
    };

    // --- ObjectWriter type ---

    static PyMethodDef methods[] = {
        {"handle", (PyCFunction)ObjectWriter::py_handle, METH_O, "Creates handle"},
        {"flush", (PyCFunction)ObjectWriter::py_flush, METH_NOARGS, "Flush buffered data to the output callback"},
        {"bind", (PyCFunction)ObjectWriter::py_bind, METH_O, "TODO"},
        {"ext_bind", (PyCFunction)ObjectWriter::py_ext_bind, METH_O, "TODO"},
        {NULL}  // Sentinel
    };

    static PyMemberDef members[] = {
        {"messages_written", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectWriter, messages_written), READONLY, "TODO"},
        {"verbose", T_BOOL, OFFSET_OF_MEMBER(ObjectWriter, verbose), 0, "TODO"},
        {"buffer_writes", T_BOOL, OFFSET_OF_MEMBER(ObjectWriter, buffer_writes), 0, "When false, flush after every write"},
        {"normalize_path", T_OBJECT, OFFSET_OF_MEMBER(ObjectWriter, normalize_path), 0, "TODO"},
        {"enable_when", T_OBJECT, OFFSET_OF_MEMBER(ObjectWriter, enable_when), 0, "TODO"},
        {NULL}  /* Sentinel */
    };

    static PyGetSetDef getset[] = {
        {"bytes_written", (getter)ObjectWriter::bytes_written_getter, nullptr, "TODO", NULL},
        {"path", (getter)ObjectWriter::path_getter, (setter)ObjectWriter::path_setter, "TODO", NULL},
        {"output", (getter)ObjectWriter::output_getter, (setter)ObjectWriter::output_setter, "Output callback", NULL},
        {"drop_mode", (getter)ObjectWriter::drop_mode_getter, (setter)ObjectWriter::drop_mode_setter, "When true, drop messages under backpressure instead of blocking", NULL},
        {NULL}  // Sentinel
    };

    PyTypeObject ObjectWriter_Type = {
        .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = MODULE "ObjectWriter",
        .tp_basicsize = sizeof(ObjectWriter),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)ObjectWriter::dealloc,
        .tp_vectorcall_offset = OFFSET_OF_MEMBER(ObjectWriter, vectorcall),
        .tp_hash = (hashfunc)_Py_HashPointer,
        .tp_call = PyVectorcall_Call,
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_VECTORCALL,
        .tp_doc = "TODO",
        .tp_traverse = (traverseproc)ObjectWriter::traverse,
        .tp_clear = (inquiry)ObjectWriter::clear,
        .tp_weaklistoffset = OFFSET_OF_MEMBER(ObjectWriter, weakreflist),
        .tp_methods = methods,
        .tp_members = members,
        .tp_getset = getset,
        .tp_init = (initproc)ObjectWriter::init,
        .tp_new = PyType_GenericNew,
    };
}
