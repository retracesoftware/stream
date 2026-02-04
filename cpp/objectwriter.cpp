#include "stream.h"
#include "writer.h"

#include <cstdint>
#include <mutex>
#include <structmember.h>
#include "wireformat.h"
#include <algorithm>
#include <sstream>
#include <sys/file.h>

#include <vector>
#include <algorithm> 
#include "unordered_dense.h"
#include "base.h"
#include <thread>
#include <condition_variable>

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

    // Forward declaration - defined after ObjectWriter struct
    static void register_atfork_handlers();

    // static void patch_dealloc(PyTypeObject * cls) {
    //     assert(!is_patched(cls->tp_dealloc));
    //     if (cls->tp_free == PyObject_Free) {
    //         cls->tp_free = PyObject_Free_Wrapper;
    //     } else if (cls->tp_free == PyObject_GC_Del) {
    //         cls->tp_free = PyObject_GC_Del_Wrapper;
    //     } else {
    //         freefuncs[cls] = cls->tp_free;
    //         cls->tp_free = generic_free;
    //     }
    // }

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

    // static bool result_to_bool(PyObject * result) {
    //     if (!result) throw nullptr;
    //     int status = PyObject_IsTrue(result);
    //     Py_DECREF(result);
    //     switch (status) {
    //         case 0: return false;
    //         case 1: return true;
    //         default:
    //             throw nullptr;
    //     }
    // }
    
    struct ObjectWriter;

    class EnsureOutput {
    private:
        // Stores the state returned by PyGILState_Ensure()
        ObjectWriter * writer;

    public:
        // Constructor: Acquires the GIL
        EnsureOutput(ObjectWriter * writer);

        // Destructor: Releases the GIL
        ~EnsureOutput();

        // Deleted Copy/Move Constructors and Assignment Operators
        // Prevents accidental copying or moving, as the GIL state should be unique
        // to the scope and thread that created the guard.
        inline EnsureOutput(const EnsureOutput&) = delete;
        inline EnsureOutput& operator=(const EnsureOutput&) = delete;
        inline EnsureOutput(EnsureOutput&&) = delete;
        inline EnsureOutput& operator=(EnsureOutput&&) = delete;
    };

    static thread_local bool writing = false;

    class Writing {
    private:
        // Stores the state returned by PyGILState_Ensure()
        bool previous;

    public:
        // Constructor: Acquires the GIL
        Writing() : previous(writing) {
            writing = true;
        }

        // Destructor: Releases the GIL
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

        // size_t bytes_written;
        size_t messages_written = 0;
        int next_handle;
        PyThreadState * last_thread_state = nullptr;
        bool stacktraces;
        int pid;
        bool verbose;
        PyObject * enable_when;
        retracesoftware::FastCall thread;
        vectorcallfunc vectorcall;
        PyObject *weakreflist;
        // std::thread flusher;
        // std::condition_variable cv;
        // std::recursive_mutex write_lock;
        bool magic_markers;

        // std::thread t1(thread_function, 1);

        inline bool is_disabled() const { return path == Py_None; }

        void write_magic() { 
            if (magic_markers) {
                stream.write_magic();
            }
        }

        void debug_prefix(size_t bytes_written = 0) {
            if (bytes_written == 0) {
                bytes_written = stream.get_bytes_written();
            }
            printf("Retrace(%i) - ObjectWriter[%lu, %lu] -- ", ::pid(), messages_written, stream.get_bytes_written());
        }

        static PyObject * StreamHandle_vectorcall(StreamHandle * self, PyObject *const * args, size_t nargsf, PyObject* kwnames) {
            
            ObjectWriter * writer = reinterpret_cast<ObjectWriter *>(self->writer);

            // No-op when disabled (path=None, performance testing mode)
            if (writer->is_disabled()) Py_RETURN_NONE;

            try {
                writer->write_all(self, args, PyVectorcall_NARGS(nargsf));
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }
        
        void write_filename(PyObject * filename) {
            size_t bytes_written = stream.get_bytes_written();

            if (stream.write_filename(filename, normalize_path)) {
                if (verbose) {
                    debug_prefix(bytes_written);
                    printf("ADD_FILENAME(%s)\n", PyUnicode_AsUTF8(filename));
                }
                messages_written++;
            }
        }

        void write_stacktrace() {
            if (!writing && stacktraces) {

                static thread_local std::vector<Frame> stack;
        
                size_t old_size = stack.size();
                size_t skip = update_stack(exclude_stacktrace, stack);
                
                assert(skip <= old_size);
    
                auto new_frame_elements = std::span(stack).subspan(skip);
    
                for (auto frame : new_frame_elements) {
                    PyObject * filename = frame.code_object->co_filename;
                    assert(PyUnicode_Check(filename));    
                    write_filename(filename);
                }

                if (verbose) {
                    debug_prefix();
                    printf("STACKTRACE\n");
                }                
                stream.write_stacktrace(old_size - skip, new_frame_elements);
                messages_written++;
            }
        }

        void bind(PyObject * obj, bool ext) {
            if (is_disabled()) return;

            EnsureOutput output(this);

            check_thread();
            write_stacktrace();

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

            EnsureOutput output(this);

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
            writer->write_delete(self->index);

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
        }

        void write_root(PyObject * obj) {
            if (verbose) {
                debug_prefix();
                PyObject * str = PyObject_Str(obj);
                printf("%s\n", PyUnicode_AsUTF8(str));
                Py_DECREF(str);
            }
            stream.write(obj);
            write_magic();
            messages_written++;
        }

        void object_freed(PyObject * obj) {
            if (is_disabled()) return;

            EnsureOutput output(this);
            if (stream.object_freed(obj)) {
                messages_written++;
            }
        }

        void write_all(StreamHandle * self, PyObject *const * args, size_t nargs) {

            EnsureOutput output(this);

            check_thread();
            write_stacktrace();

            Writing w;

            write_root(self);
            for (size_t i = 0; i < nargs; i++) {
                write_root(args[i]);
            }
        }
    
        void change_output() {
            PyObject * s = PyObject_Str(path);
            assert(s);
            stream.change_output(PyUnicode_AsUTF8(s));
            Py_DECREF(s);
        }

        void write_all(PyObject*const * args, size_t nargs) {
            EnsureOutput output(this);

            check_thread();
            write_stacktrace();

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
            // std::lock_guard<std::recursive_mutex> lock(self->write_lock);

            if (kwnames) {
                PyErr_SetString(PyExc_TypeError, "ObjectWriter does not accept keyword arguments");
                return nullptr;
            }
         
            try {
                self->write_all(args, PyVectorcall_NARGS(nargsf));
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
            // if (self->file) {
            //     if (self->enabled()) {            
            //         self->write_all(PyVectorcall_NARGS(nargsf), nargs);
            //     }
            //     Py_RETURN_NONE;

            // } else {
            //     self->file = open(self->path, true);

            //     try {
            //         PyObject * result =  py_vectorcall(self, args, nargsf, kwnames);
            //         fclose(self->file);
            //         self->file = nullptr;
            //         return result;
            //     } catch(...) {
            //         fclose(self->file);
            //         self->file = nullptr;                    
            //         throw;
            //     }
            // }
        }

        static PyObject * py_flush(ObjectWriter * self, PyObject* unused) {
            // std::lock_guard<std::recursive_mutex> lock(self->write_lock);
            try {
                self->stream.flush();
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        // static PyObject * py_close(ObjectWriter * self, PyObject* unused) {
        //     // std::lock_guard<std::recursive_mutex> lock(self->write_lock);

        //     if (stream.is_closed()) {
        //         PyErr_Format(PyExc_RuntimeError, "File %S is already closed", self->file);
        //         return nullptr;
        //     }

        //     if (self->file) {
        //         fclose(self->file);
        //         self->file = nullptr;
        //     }
        //     Py_RETURN_NONE;
        // }

        // static PyObject * py_reopen(ObjectWriter * self, PyObject* unused) {
        //     std::lock_guard<std::recursive_mutex> lock(self->write_lock);
        //     if (self->file) {
        //         PyErr_Format(PyExc_RuntimeError, "File %S is already opened", self->file);
        //         return nullptr;
        //     }
            
        //     try {
        //         self->file = open(self->path, true);
        //         Py_RETURN_NONE;
        //     } catch (...) {
        //         return nullptr;
        //     }
        // }

        // static PyObject * py_store_hash_secret(ObjectWriter * self, PyObject* unused) {                
        //     try {
        //         void *secret = &_Py_HashSecret;
        //         self->write((const uint8_t *)secret, sizeof(_Py_HashSecret_t));
        //         Py_RETURN_NONE;
        //     } catch (...) {
        //         return nullptr;
        //     }
        // }

        static PyObject * py_handle(ObjectWriter * self, PyObject* obj) {
            // std::lock_guard<std::recursive_mutex> lock(self->write_lock);
            try {
                return self->handle(obj);
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_bind(ObjectWriter * self, PyObject* obj) {
            // std::lock_guard<std::recursive_mutex> lock(self->write_lock);
            try {
                self->bind(obj, false);
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_ext_bind(ObjectWriter * self, PyObject* obj) {
            // std::lock_guard<std::recursive_mutex> lock(self->write_lock);
            try {
                self->bind(obj, true);
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        // void flush_loop() {
        //     std::lock_guard<std::mutex> lock(write_lock);

        //     while (true)
        //     {
        //         cv.wait_for(write_lock, std::chrono::seconds(5), [this] { 
        //             // The predicate: If this returns true, the wait immediately ends.
        //             return should_exit_;
        //         });

        //         // If should_exit_ is true, break the loop
        //         if (should_exit_) {
        //             break; 
        //         }
        //     } // Lock is automatically released here
        //     // If we woke up due to timeout (not exit signal), run the task
        //     flush_file();
        // }

        static int init(ObjectWriter * self, PyObject* args, PyObject* kwds) {

            PyObject * path;
            
            PyObject * thread = nullptr;
            PyObject * normalize_path = nullptr;
            PyObject * serializer = nullptr;

            int verbose = 0;
            int stacktraces = 0;
            int magic_markers = 0;
            
            static const char* kwlist[] = {
                "path", 
                "serializer", 
                "thread", 
                "verbose", 
                "stacktraces", 
                "normalize_path",
                "magic_markers",
                nullptr};  // Keywords allowed

            if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO|OppOp", (char **)kwlist, 
                &path, &serializer, &thread, &verbose, &stacktraces, &normalize_path, &magic_markers)) {
                return -1;  
                // Return NULL to propagate the parsing error
            }

            self->verbose = verbose;
            self->stacktraces = stacktraces;
            self->magic_markers = magic_markers;

            self->path = Py_NewRef(path);
            // self->serializer = Py_NewRef(serializer);
            self->thread = thread ? retracesoftware::FastCall(thread) : retracesoftware::FastCall();
            Py_XINCREF(thread);

            self->messages_written = 0;
            // enumtype(enumtype);
            self->next_handle = 0;
            
            // new (&self->placeholders) map<PyObject *, uint64_t>();
            // new (&self->type_serializers) map<PyTypeObject *, retracesoftware::FastCall>();

            new (&self->exclude_stacktrace) set<PyObject *>();
            self->vectorcall = reinterpret_cast<vectorcallfunc>(ObjectWriter::py_vectorcall);
            // self->last_thread_state = PyThreadState_Get();

            self->normalize_path = Py_XNewRef(normalize_path);
            self->enable_when = nullptr;
            // self->stack_stop_at = 0;

            if (path != Py_None) {
                PyObject * s = PyObject_Str(path);
                new (&self->stream) MessageStream(PyUnicode_AsUTF8(s), 0, serializer);
                Py_DECREF(s);
            }

            register_atfork_handlers();  // Ensure fork safety is set up
            writers.push_back(self);

            // new (&self->cv) std::condition_variable();
            // new (&self->flusher) std::thread(&ObjectWriter::flush_loop, self);

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
            // Py_VISIT(self->m_global_lookup);
            self->stream.traverse(visit, arg);
            Py_VISIT(self->thread.callable);
            Py_VISIT(self->path);
            return 0;
        }

        static int clear(ObjectWriter* self) {
            // Py_CLEAR(self->name_cache);
            Py_CLEAR(self->thread.callable);
            Py_CLEAR(self->path);
            Py_CLEAR(self->normalize_path);
            return 0;
        }

        static void dealloc(ObjectWriter* self) {
            // lock 
            self->stream.close();
            
            if (self->weakreflist != NULL) {
                PyObject_ClearWeakRefs((PyObject *)self);
            }

            // cv.notify_one();
            // thread.join();

            PyObject_GC_UnTrack(self);
            clear(self);

            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));

            auto it = std::find(writers.begin(), writers.end(), self);
            if (it != writers.end()) {
                writers.erase(it);
            }
        }


        static PyObject * keep_open_getter(ObjectWriter *self, void *closure) {
            return PyBool_FromLong(self->stream.is_closed() ? 0 : 1);
        }

        static int keep_open_setter(ObjectWriter *self, PyObject *value, void *closure) {
            if (value == nullptr) {
                PyErr_SetString(PyExc_AttributeError, "deletion of 'keep_open' is not allowed");
                return -1;
            }
            if (!PyBool_Check(value)) {
                PyErr_SetString(PyExc_AttributeError, "'keep_open' must be set with a boolean flag");
                return -1;
            }
            bool flag = value == Py_True ? true : false;

            if (flag && self->stream.is_closed()) {
                self->change_output();

            } else if (!flag && !self->stream.is_closed()) {
                self->stream.close();
            }            
            return 0;
        }

        static PyObject * bytes_written_getter(ObjectWriter *self, void *closure) {
            return PyLong_FromLong(self->stream.get_bytes_written());
        }

        static PyObject * path_getter(ObjectWriter *self, void *closure) {
            // if (self->on_pid_change == NULL) {
            //     PyErr_SetString(PyExc_AttributeError, "attribute 'on_pid_change' is not set");
            //     return NULL;
            // }
            return Py_NewRef(self->path);
        }

        static int path_setter(ObjectWriter *self, PyObject *value, void *closure) {
 
            if (value == nullptr) {
                PyErr_SetString(PyExc_AttributeError, "deletion of 'path' is not allowed");
                return -1;
            }

            Py_DECREF(self->path);
            self->path = Py_NewRef(value);

            if (!self->stream.is_closed()) {
                self->change_output();
            }
            return 0;
        }
    };

    EnsureOutput::EnsureOutput(ObjectWriter * writer) {
        if (writer->stream.is_closed()) {
            writer->change_output();
            this->writer = writer;
        } else {
            this->writer = nullptr;
        }
    }

    // Destructor: Releases the GIL
    EnsureOutput::~EnsureOutput() {
        if (writer) {
            writer->stream.close();
        }
    }

#ifndef _WIN32
    // Fork safety: prevent child processes from corrupting parent's trace file
    // 
    // On fork(), child inherits parent's FILE* buffer. If child flushes (explicitly
    // or on exit), it writes to parent's file, corrupting the trace.
    //
    // Solution:
    // - prepare: flush all writers so parent's data is safely written
    // - child: close fd without flush, abandon FILE* to prevent corruption

    static void atfork_prepare() {
        for (auto* w : writers) {
            w->stream.flush();
        }
    }

    static void atfork_child() {
        for (auto* w : writers) {
            w->stream.abandon_for_fork();
        }
        writers.clear();  // Child starts fresh - these writers are now invalid
    }

    static bool atfork_registered = false;

    static void register_atfork_handlers() {
        if (!atfork_registered) {
            pthread_atfork(atfork_prepare, nullptr, atfork_child);
            atfork_registered = true;
        }
    }
#else
    static void register_atfork_handlers() {
        // Windows doesn't have fork() - CreateProcess doesn't inherit handles by default
    }
#endif

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

    static PyMethodDef methods[] = {
        // {"add_type_serializer", (PyCFunction)ObjectWriter::py_add_type_serializer, METH_VARARGS | METH_KEYWORDS, "Creates handle"},
        // {"placeholder", (PyCFunction)ObjectWriter::py_placeholder, METH_O, "Creates handle"},
        {"handle", (PyCFunction)ObjectWriter::py_handle, METH_O, "Creates handle"},
        // {"write", (PyCFunction)ObjectWriter::py_write, METH_O, "Write's object returning a handle for future writes"},
        {"flush", (PyCFunction)ObjectWriter::py_flush, METH_NOARGS, "TODO"},
        // {"close", (PyCFunction)ObjectWriter::py_close, METH_NOARGS, "TODO"},
        // {"reopen", (PyCFunction)ObjectWriter::py_reopen, METH_NOARGS, "TODO"},
        {"bind", (PyCFunction)ObjectWriter::py_bind, METH_O, "TODO"},
        {"ext_bind", (PyCFunction)ObjectWriter::py_ext_bind, METH_O, "TODO"},
        // {"store_hash_secret", (PyCFunction)ObjectWriter::py_store_hash_secret, METH_NOARGS, "TODO"},
        {"exclude_from_stacktrace", (PyCFunction)ReaderWriterBase::py_exclude_from_stacktrace, METH_O, "TODO"},

        // {"unique", (PyCFunction)ObjectWriter::py_unique, METH_O, "TODO"},
        // {"delete", (PyCFunction)ObjectWriter::py_delete, METH_O, "TODO"},

        // {"tuple", (PyCFunction)ObjectWriter::py_write_tuple, METH_FASTCALL, "TODO"},
        // {"dict", (PyCFunction)ObjectWriter::, METH_FASTCALL | METH_KEYWORDS, "TODO"},
        {NULL}  // Sentinel
    };

    static PyMemberDef members[] = {
        {"messages_written", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectWriter, messages_written), READONLY, "TODO"},
        // {"stack_stop_at", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectWriter, stack_stop_at), 0, "TODO"},
        {"stacktraces", T_BOOL, OFFSET_OF_MEMBER(ObjectWriter, stacktraces), 0, "TODO"},
        {"verbose", T_BOOL, OFFSET_OF_MEMBER(ObjectWriter, verbose), 0, "TODO"},
        {"normalize_path", T_OBJECT, OFFSET_OF_MEMBER(ObjectWriter, normalize_path), 0, "TODO"},
        {"enable_when", T_OBJECT, OFFSET_OF_MEMBER(ObjectWriter, enable_when), 0, "TODO"},

        // {"path", T_OBJECT, OFFSET_OF_MEMBER(Writer, path), READONLY, "TODO"},
        // {"on_pid_change", T_OBJECT_EX, OFFSET_OF_MEMBER(Writer, on_pid_change), 0, "TODO"},
        {NULL}  /* Sentinel */
    };

    static PyGetSetDef getset[] = {
        {"bytes_written", (getter)ObjectWriter::bytes_written_getter, nullptr, "TODO", NULL},
        {"path", (getter)ObjectWriter::path_getter, (setter)ObjectWriter::path_setter, "TODO", NULL},
        {"keep_open", (getter)ObjectWriter::keep_open_getter, (setter)ObjectWriter::keep_open_setter, "TODO", NULL},

        // {"thread_number", (getter)Writer::thread_getter, (setter)Writer::thread_setter, "TODO", NULL},
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