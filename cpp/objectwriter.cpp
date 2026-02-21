#include "stream.h"
#include "writer.h"
#include "queueentry.h"
#include "vendor/SPSCQueue.h"

#include <cstddef>
#include <cstdint>
#include <chrono>
#include <thread>
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
    struct AsyncFilePersister;

    static std::vector<ObjectWriter *> writers;

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

    static PyObject* pickle_dumps_fn() {
        static PyObject* dumps = nullptr;
        if (!dumps) {
            PyObject* mod = PyImport_ImportModule("pickle");
            if (!mod) { PyErr_Clear(); return nullptr; }
            dumps = PyObject_GetAttrString(mod, "dumps");
            Py_DECREF(mod);
            if (!dumps) { PyErr_Clear(); return nullptr; }
        }
        return dumps;
    }

    static inline int64_t native_estimate(PyObject* obj) {
        if (obj == Py_None || obj == Py_True || obj == Py_False) return 0;
        PyTypeObject* tp = Py_TYPE(obj);
        if (tp == &PyLong_Type)   return 28;
        if (tp == &PyFloat_Type)  return 24;
        if (tp == &PyUnicode_Type)
            return (int64_t)(sizeof(PyObject) + PyUnicode_GET_LENGTH(obj));
        if (tp == &PyBytes_Type)
            return (int64_t)(sizeof(PyObject) + PyBytes_GET_SIZE(obj));
        if (tp == &PyBool_Type)   return 0;
        if (tp == &PyMemoryView_Type) {
            Py_buffer* view = PyMemoryView_GET_BUFFER(obj);
            return (int64_t)(sizeof(PyObject) + view->len);
        }
        if (tp == &StreamHandle_Type) return 64;
        if (is_patched(tp->tp_free)) return 64;
        return -1;
    }

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
        
        rigtorp::SPSCQueue<uint64_t>* queue = nullptr;
        rigtorp::SPSCQueue<PyObject*>* return_queue = nullptr;
        PyObject* persister = nullptr;

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

        int64_t inflight_bytes = 0;
        int64_t inflight_limit = 128LL * 1024 * 1024;

        inline bool is_disabled() const { return queue == nullptr; }

        void wait_for_inflight() {
            while (inflight_bytes > inflight_limit) {
                Py_BEGIN_ALLOW_THREADS
                std::this_thread::yield();
                Py_END_ALLOW_THREADS
                if (is_disabled()) return;
            }
        }

        void push(uint64_t entry) {
            if (queue->try_push(entry)) return;
            bool ok = false;
            Py_BEGIN_ALLOW_THREADS
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            while (true) {
                if (queue->try_push(entry)) { ok = true; break; }
                if (std::chrono::steady_clock::now() >= deadline) break;
                std::this_thread::yield();
            }
            Py_END_ALLOW_THREADS
            if (!ok) {
                fprintf(stderr, "retrace: writer queue full, disabling recording\n");
                queue = nullptr;
            }
        }

        void debug_prefix(size_t bytes_written = 0) {
            printf("Retrace(%i) - ObjectWriter[%lu] -- ", ::pid(), messages_written);
        }

        static PyObject * StreamHandle_vectorcall(StreamHandle * self, PyObject *const * args, size_t nargsf, PyObject* kwnames) {
            
            ObjectWriter * writer = reinterpret_cast<ObjectWriter *>(self->writer);

            if (writer->is_disabled()) {
                Py_RETURN_NONE;
            }

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

            if (!is_patched(Py_TYPE(obj)->tp_free)) {
                patch_free(Py_TYPE(obj));
            }

            if (ext) {
                push(cmd_entry(CMD_EXT_BIND));
                Py_INCREF(obj);  inflight_bytes += estimate_size(obj); push(obj_entry(obj));
                PyObject* type = (PyObject*)Py_TYPE(obj);
                Py_INCREF(type); inflight_bytes += estimate_size(type); push(obj_entry(type));
            } else {
                push(cmd_entry(CMD_BIND));
                Py_INCREF(obj);  inflight_bytes += estimate_size(obj); push(obj_entry(obj));
            }
            messages_written++;
        }

        void write_delete(int id) {
            if (is_disabled()) return;

            if (verbose) {
                debug_prefix();
                printf("DELETE(%i)\n", id);
            }
            int delta = next_handle - id;
            assert(delta > 0);

            push(cmd_entry(CMD_HANDLE_DELETE, delta - 1));
            messages_written++;
        }

        static void StreamHandle_dealloc(StreamHandle* self) {
            
            ObjectWriter * writer = reinterpret_cast<ObjectWriter *>(self->writer);

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
            if (is_disabled()) {
                return stream_handle(next_handle++, nullptr);
            }

            if (verbose) {
                debug_prefix();
                PyObject * str = PyObject_Str(obj);
                printf("NEW_HANDLE(%s)\n", PyUnicode_AsUTF8(str));
                Py_DECREF(str);
            }

            push(cmd_entry(CMD_NEW_HANDLE));
            Py_INCREF(obj); inflight_bytes += estimate_size(obj); push(obj_entry(obj));
            messages_written++;
            return stream_handle(next_handle++, verbose ? obj : nullptr);
        }

        void write_root(StreamHandle * obj) {
            if (verbose) {
                debug_prefix();
                PyObject * str = PyObject_Str(obj->object);
                printf("HANDLE_REF(%s)\n", PyUnicode_AsUTF8(str));
                Py_DECREF(str);
            }

            push(cmd_entry(CMD_HANDLE_REF, obj->index));
            messages_written++;
        }

        static constexpr int MAX_FLATTEN_DEPTH = 32;

        void push_value(PyObject* obj, int depth = 0) {
            int64_t est = native_estimate(obj);
            if (est >= 0) {
                Py_INCREF(obj);
                inflight_bytes += est;
                push(obj_entry(obj));
                return;
            }

            PyTypeObject* tp = Py_TYPE(obj);

            if (depth < MAX_FLATTEN_DEPTH) {
                if (tp == &PyList_Type) {
                    Py_ssize_t n = PyList_GET_SIZE(obj);
                    push(cmd_entry(CMD_LIST, (uint32_t)n));
                    for (Py_ssize_t i = 0; i < n; i++)
                        push_value(PyList_GET_ITEM(obj, i), depth + 1);
                    return;
                }
                if (tp == &PyTuple_Type) {
                    Py_ssize_t n = PyTuple_GET_SIZE(obj);
                    push(cmd_entry(CMD_TUPLE, (uint32_t)n));
                    for (Py_ssize_t i = 0; i < n; i++)
                        push_value(PyTuple_GET_ITEM(obj, i), depth + 1);
                    return;
                }
                if (tp == &PyDict_Type) {
                    Py_ssize_t n = PyDict_Size(obj);
                    push(cmd_entry(CMD_DICT, (uint32_t)n));
                    Py_ssize_t pos = 0;
                    PyObject *key, *value;
                    while (PyDict_Next(obj, &pos, &key, &value)) {
                        push_value(key, depth + 1);
                        push_value(value, depth + 1);
                    }
                    return;
                }
            } else if (tp == &PyList_Type || tp == &PyTuple_Type || tp == &PyDict_Type) {
                Py_INCREF(obj);
                inflight_bytes += estimate_size(obj);
                push(obj_entry(obj));
                return;
            }

            PyObject* dumps = pickle_dumps_fn();
            PyObject* pickled = dumps ? PyObject_CallOneArg(dumps, obj) : nullptr;
            if (pickled) {
                push(cmd_entry(CMD_PICKLED));
                inflight_bytes += (int64_t)(sizeof(PyObject) + PyBytes_GET_SIZE(pickled));
                push(obj_entry(pickled));
            } else {
                PyErr_Clear();
                Py_INCREF(obj);
                inflight_bytes += estimate_size(obj);
                push(obj_entry(obj));
            }
        }

        void write_root(PyObject * obj) {
            if (verbose) {
                debug_prefix();
                PyObject * str = PyObject_Str(obj);
                printf("%s\n", PyUnicode_AsUTF8(str));
                Py_DECREF(str);
            }

            push_value(obj);
            messages_written++;
        }

        void object_freed(PyObject * obj) {
            if (is_disabled()) return;

            push(cmd_entry(CMD_BINDING_DELETE));
            push(obj_entry(obj));
        }

        void write_all(StreamHandle * self, PyObject *const * args, size_t nargs) {

            wait_for_inflight();
            if (is_disabled()) return;

            check_thread();

            Writing w;

            write_root(self);
            for (size_t i = 0; i < nargs; i++) {
                write_root(args[i]);
            }
        }

        void write_all(PyObject*const * args, size_t nargs) {

            wait_for_inflight();
            if (is_disabled()) return;

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

            if (self->is_disabled()) Py_RETURN_NONE;

            if (kwnames) {
                PyErr_SetString(PyExc_TypeError, "ObjectWriter does not accept keyword arguments");
                return nullptr;
            }
         
            try {
                self->write_all(args, PyVectorcall_NARGS(nargsf));
                if (!self->buffer_writes) {
                    self->push(cmd_entry(CMD_FLUSH));
                }
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_flush(ObjectWriter * self, PyObject* unused) {
            if (self->is_disabled()) Py_RETURN_NONE;
            try {
                self->push(cmd_entry(CMD_FLUSH));
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
            PyObject * preamble = nullptr;

            int verbose = 0;
            long long inflight_limit_arg = 128LL * 1024 * 1024;
            Py_ssize_t queue_capacity_arg = 65536;
            Py_ssize_t return_queue_capacity_arg = 131072;
            
            static const char* kwlist[] = {
                "output", 
                "serializer", 
                "thread", 
                "verbose", 
                "normalize_path",
                "preamble",
                "inflight_limit",
                "queue_capacity",
                "return_queue_capacity",
                nullptr};

            if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO|OpOOLnn", (char **)kwlist, 
                &output, &serializer, &thread, &verbose, &normalize_path, &preamble,
                &inflight_limit_arg, &queue_capacity_arg, &return_queue_capacity_arg)) {
                return -1;  
            }

            self->verbose = verbose;
            self->buffer_writes = true;

            self->path = Py_NewRef(Py_None);
            self->thread = thread ? retracesoftware::FastCall(thread) : retracesoftware::FastCall();
            Py_XINCREF(thread);

            self->messages_written = 0;
            self->next_handle = 0;
            
            self->vectorcall = reinterpret_cast<vectorcallfunc>(ObjectWriter::py_vectorcall);

            self->normalize_path = Py_XNewRef(normalize_path);
            self->enable_when = nullptr;
            self->queue = nullptr;
            self->return_queue = nullptr;
            self->persister = nullptr;
            self->inflight_bytes = 0;
            self->inflight_limit = inflight_limit_arg;

            if (output != Py_None && Py_TYPE(output) == &AsyncFilePersister_Type) {
                SetupResult r = AsyncFilePersister_setup(output, serializer,
                                                         (size_t)queue_capacity_arg,
                                                         (size_t)return_queue_capacity_arg,
                                                         &self->inflight_bytes);
                if (!r.forward_queue) return -1;
                self->queue = (rigtorp::SPSCQueue<uint64_t>*)r.forward_queue;
                self->return_queue = (rigtorp::SPSCQueue<PyObject*>*)r.return_queue;
                self->persister = Py_NewRef(output);
            }

            if (preamble && preamble != Py_None && !self->is_disabled()) {
                try {
                    self->write_root(preamble);
                    self->push(cmd_entry(CMD_FLUSH));
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
                push(cmd_entry(CMD_THREAD_SWITCH));
                Py_INCREF(thread_handle);
                inflight_bytes += estimate_size(thread_handle);
                push(obj_entry(thread_handle));
                messages_written++;
            }
        }

        static int traverse(ObjectWriter* self, visitproc visit, void* arg) {
            Py_VISIT(self->persister);
            Py_VISIT(self->thread.callable);
            Py_VISIT(self->path);
            Py_VISIT(self->normalize_path);
            return 0;
        }

        static int clear(ObjectWriter* self) {
            Py_CLEAR(self->persister);
            Py_CLEAR(self->thread.callable);
            Py_CLEAR(self->path);
            Py_CLEAR(self->normalize_path);
            return 0;
        }

        static void dealloc(ObjectWriter* self) {
            if (self->queue) {
                self->push(cmd_entry(CMD_SHUTDOWN));
                self->queue = nullptr;
            }
            
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
            PyObject* p = self->persister;
            return Py_NewRef(p ? p : Py_None);
        }

        static int output_setter(ObjectWriter *self, PyObject *value, void *closure) {
            if (value == nullptr) {
                PyErr_SetString(PyExc_AttributeError, "deletion of 'output' is not allowed");
                return -1;
            }
            if (value == Py_None) {
                self->queue = nullptr;
                Py_CLEAR(self->persister);
            }
            return 0;
        }

        static PyObject * bytes_written_getter(ObjectWriter *self, void *closure) {
            return PyLong_FromLong(0);
        }

        static PyObject * inflight_limit_getter(ObjectWriter *self, void *closure) {
            return PyLong_FromLongLong(self->inflight_limit);
        }

        static int inflight_limit_setter(ObjectWriter *self, PyObject *value, void *closure) {
            if (value == nullptr) {
                PyErr_SetString(PyExc_AttributeError, "deletion of 'inflight_limit' is not allowed");
                return -1;
            }
            long long v = PyLong_AsLongLong(value);
            if (v == -1 && PyErr_Occurred()) return -1;
            self->inflight_limit = (int64_t)v;
            return 0;
        }

        static PyObject * inflight_bytes_getter(ObjectWriter *self, void *closure) {
            return PyLong_FromLongLong(self->inflight_bytes);
        }
    };

    void on_free(void * obj) {
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
        {NULL}
    };

    static PyMemberDef members[] = {
        {"messages_written", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectWriter, messages_written), READONLY, "TODO"},
        {"verbose", T_BOOL, OFFSET_OF_MEMBER(ObjectWriter, verbose), 0, "TODO"},
        {"buffer_writes", T_BOOL, OFFSET_OF_MEMBER(ObjectWriter, buffer_writes), 0, "When false, flush after every write"},
        {"normalize_path", T_OBJECT, OFFSET_OF_MEMBER(ObjectWriter, normalize_path), 0, "TODO"},
        {"enable_when", T_OBJECT, OFFSET_OF_MEMBER(ObjectWriter, enable_when), 0, "TODO"},
        {NULL}
    };

    static PyGetSetDef getset[] = {
        {"bytes_written", (getter)ObjectWriter::bytes_written_getter, nullptr, "TODO", NULL},
        {"path", (getter)ObjectWriter::path_getter, (setter)ObjectWriter::path_setter, "TODO", NULL},
        {"output", (getter)ObjectWriter::output_getter, (setter)ObjectWriter::output_setter, "Output callback", NULL},
        {"inflight_limit", (getter)ObjectWriter::inflight_limit_getter, (setter)ObjectWriter::inflight_limit_setter,
         "Maximum bytes in-flight between writer and persister", NULL},
        {"inflight_bytes", (getter)ObjectWriter::inflight_bytes_getter, nullptr,
         "Current estimated bytes in-flight", NULL},
        {NULL}
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
