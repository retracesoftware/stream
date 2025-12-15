#include "stream.h"
#include <stdexcept>
#include <structmember.h>
#include "wireformat.h"
#include <algorithm>
#include <fcntl.h>
#include <sstream>
#include <sys/file.h>
#include <condition_variable>
#include <mutex>
#include <ranges>

#include "reader.h"
#include "base.h"

// #include <frameobject.h>

// void print_current_stack(void)
// {
//     // Get the current frame (top of the stack)
//     PyFrameObject *frame = PyEval_GetFrame();  // borrowed reference!

//     if (frame == NULL) {
//         PySys_WriteStderr("<no Python frame>\n");
//         return;
//     }

//     PySys_WriteStderr("Traceback (most recent call last):\n");

//     // Walk up the frame chain and print each one
//     for (PyFrameObject *f = frame; f != NULL; f = f->f_back) {
//         PyCodeObject *code = PyFrame_GetCode(f);  // new reference
//         if (!code) continue;

//         const char *filename = PyUnicode_AsUTF8(code->co_filename);
//         const char *name     = PyUnicode_AsUTF8(code->co_name);
//         int lineno           = PyFrame_GetLineNumber(f);

//         if (!filename) filename = "<unknown file>";
//         if (!name)     name     = "<unknown function>";

//         PySys_WriteStderr("  File \"%s\", line %d, in %s\n",
//                           filename, lineno, name);

//         Py_DECREF(code);
//     }
// }

namespace retracesoftware_stream {

    class PyGCGuard {
    public:
        PyGCGuard() {
            // Acquisition: Disable GC and save state
            was_enabled_ = PyGC_Disable();
        }

        ~PyGCGuard() {
            // Release: Restore GC state in the destructor
            if (was_enabled_) {
                PyGC_Enable();
            }
        }

    private:
        int was_enabled_;
    };

    static FILE * open(PyObject * path) {
        PyObject * path_str = PyObject_Str(path);
        if (!path_str) throw nullptr;

        // int fd = open(PyUnicode_AsUTF8(path_str), O_WRONLY | O_CREAT | O_EXCL, 0644);
        
        FILE * file = fopen(PyUnicode_AsUTF8(path_str), "rb");

        Py_DECREF(path_str);

        if (!file) {
            PyErr_Format(PyExc_IOError, "Could not open file: %S, mode: %s for reader, error: %s", path, "rb", strerror(errno));
            throw nullptr;
        }

        // int fd = fileno(file);
        
        // if (flock(fd, LOCK_EX | LOCK_NB) == -1) {
        //     fprintf(stderr, "TRIED TO LOCK AN ALREADY LOCKED FILE!!!!\n");

        //     PyErr_Format(PyExc_IOError, "Could not lock file: %S for exclusive access, error: %s", path, strerror(errno));
        //     // perror("flock");
        //     // // Handle locking failure: another process holds the lock
        //     fclose(file);
        //     throw nullptr;
        // }
        return file;
    }
    
    // static bool equal(PyObject * a, PyObject * b) {
    //     retracesoftware::GILGuard guard;

    //     switch (PyObject_RichCompareBool(a, b, Py_EQ)) {
    //         case 0: return false;
    //         case 1: return true;
    //         default:
    //             assert(PyErr_Occurred());
    //             throw nullptr;
    //     };
    // }

    struct ObjectReader : public ReaderWriterBase {
        MessageStream stream;

        size_t messages_read;
        PyObject * on_stack_difference;
        // int pid;

        map<PyObject *, PyObject *> normalized_paths;

        // map<PyTypeObject *, PyObject *> type_deserializers;

        PyObject * deserializer;
        PyObject * transform;
        PyObject * thread;
        // PyObject * active_thread;
        // vectorcallfunc vectorcall;

        // PyObject * pending_reads;
        // PyObject * stacktraces;

        // PyObject * next;
        bool verbose;
        vectorcallfunc vectorcall;

        // map<PyObject *, PyObject *> pending_reads;
        map<PyObject *, Stacktrace> previous_stacks;

        void on_difference(Stacktrace previous, Stacktrace record, Stacktrace replay) {
            auto pythonize = [](Stacktrace frames) { 
                return to_pylist(frames | std::views::transform(&CodeLocation::as_tuple));
            };

            PyUniquePtr py_previous(pythonize(previous));
            PyUniquePtr py_record(pythonize(record));
            PyUniquePtr py_replay(pythonize(replay));

            PyObject * res = PyObject_CallFunctionObjArgs(on_stack_difference, py_previous.get(), py_record.get(), py_replay.get(), nullptr);
            Py_XDECREF(res);
            if (!res) throw nullptr;
        }

        Stacktrace stacktrace() {
            Stacktrace trace;

            for (Frame frame : stack(exclude_stacktrace)) {   
                CodeLocation location = frame.location();
                if (normalize_path) {
                    if (!normalized_paths.contains(location.filename)) {
                        PyObject * normalized = PyObject_CallOneArg(normalize_path, location.filename);
                        if (!normalized) throw nullptr;
                        normalized_paths[Py_NewRef(location.filename)] = normalized;
                    }
                    location.filename = normalized_paths[location.filename];
                }
                trace.push_back(location);
            }
            return trace;
        }

        // Stacktrace read_replay_stack(std::vector<Frame>& previous) {
        //     static thread_local std::vector<CodeLocation> locations;

        //     size_t common = update_stack(exclude_stacktrace, previous);

        //     while (locations.size() > common) locations.pop_back();

        //     for (Frame frame : previous | std::views::drop(common)) {
        //         CodeLocation location = frame.location();
        //         if (normalize_path) {
        //             if (!normalized_paths.contains(location.filename)) {
        //                 PyObject * normalized = PyObject_CallOneArg(normalize_path, location.filename);
        //                 if (!normalized) throw nullptr;
        //                 normalized_paths[Py_NewRef(location.filename)] = normalized;
        //             }
        //             location.filename = normalized_paths[location.filename];
        //         }
        //         locations.push_back(location);
        //     }
        //     return locations;
        // }

        void on_stack(PyObject * thread, const Stacktrace &record) {
            Stacktrace replay = stacktrace();
            if (record != replay) {
                on_difference(previous_stacks[thread], record, replay);
            }
            previous_stacks[thread] = replay;
        }

        // PyObject * create_stacktrace(PyObject * current_thread) {
        //     PyObject * func = PyDict_GetItem(pending_reads, current_thread);

        //     return PyCallable_Check(func) ? PyObject_CallNoArgs(func) : Py_NewRef(Py_None);
        // 

        // void set_stacktrace(PyObject * current_thread) {
        //     assert (PyGILState_Check());

        //     PyObject * trace = create_stacktrace(current_thread);
        //     if (!trace) throw nullptr;
        //     PyDict_SetItem(stacktraces, current_thread, trace);
        //     Py_DECREF(trace);
        // }

        static PyObject * py_bind(ObjectReader *self, PyObject* obj) {

            PyGCGuard guard;

            PyObject * thread = PyObject_CallNoArgs(self->thread);

            try {
                // {
                //     PyObject * s = PyObject_Str(obj);
                //     printf("Binding: %s\n", PyUnicode_AsUTF8(s));
                //     Py_DECREF(s);
                // }

                self->stream.bind(thread, obj);
                Py_DECREF(thread);
            } catch (...) {
                Py_DECREF(thread);
                if (PyErr_Occurred()) {
                    return nullptr;
                } else {
                    raise(SIGTRAP);
                }
            }
            Py_RETURN_NONE;
        }
        
        static PyObject* py_vectorcall(ObjectReader *self, PyObject *const *args, size_t nargsf, PyObject *kwnames) {

            PyGCGuard guard;

            PyObject * thread = PyObject_CallNoArgs(self->thread);

            try {
                return self->stream.next(thread);
            } catch (const std::runtime_error &error) {
                PyErr_SetString(PyExc_RuntimeError, error.what());
                return nullptr;
            } 
            catch (...) {
                if (!PyErr_Occurred()) {
                    raise(SIGTRAP);
                }
                return nullptr;
            }
        }

        static PyObject* create(PyTypeObject* type, PyObject*, PyObject*) {
            auto* self = reinterpret_cast<ObjectReader *>(type->tp_alloc(type, 0));
            if (!self) return nullptr;
            return reinterpret_cast<PyObject*>(self);
        }

        void on_timeout(ThreadMap &threads) {
            raise(SIGTRAP);
        }

        static int init(ObjectReader * self, PyObject* args, PyObject* kwds) {

            try {
                PyObject * path;
                PyObject * deserializer;
                PyObject * transform = nullptr;
                PyObject * thread = nullptr;
                PyObject * on_stack_difference;
                PyObject * normalize_path = nullptr;;
                int magic_markers = 0;

                int verbose = 0;
                static const char* kwlist[] = {
                    "path", 
                    "deserializer", 
                    "on_stack_difference",
                    "transform", 
                    "thread",
                    "verbose",
                    "normalize_path",
                    "magic_markers",
                    nullptr};  // Keywords allowed

                if (!PyArg_ParseTupleAndKeywords(args, kwds, "OOO|OOpOp", (char **)kwlist, 
                    &path, &deserializer, &on_stack_difference,
                    &transform, &thread, &verbose, &normalize_path, &magic_markers)) {
                    return -1;
                    // Return NULL to propagate the parsing error
                }

                self->transform = transform != Py_None ? Py_XNewRef(transform) : nullptr;
                self->deserializer = Py_NewRef(deserializer);

                int read_timeout = 1000;

                new (&self->stream) MessageStream(
                    open(path), 
                    read_timeout,
                    [self](PyObject * thread, const Stacktrace &record) { return self->on_stack(thread, record); }, 
                    [self](PyObject * obj) { return self->transform ? PyObject_CallOneArg(self->transform, obj) : Py_NewRef(obj); }, 
                    [self](PyObject * obj) { return PyObject_CallOneArg(self->deserializer, obj); },
                    [self](ThreadMap& threads) { self->on_timeout(threads); },
                    magic_markers);

                self->stream.set_timeout(10000);

                self->thread = Py_XNewRef(thread);
                self->path = Py_NewRef(path);
                self->messages_read = 0;
                // enumtype(enumtype);
                self->verbose = verbose;
                self->normalize_path = Py_XNewRef(normalize_path);
                self->on_stack_difference = Py_NewRef(on_stack_difference);
                self->vectorcall = reinterpret_cast<vectorcallfunc>(ObjectReader::py_vectorcall);
                // self->pending_reads = PyDict_New();
                // self->stacktraces = nullptr;
                new (&self->exclude_stacktrace) set<PyFunctionObject *>();
                new (&self->normalized_paths) map<PyObject *, PyObject *>();
                new (&self->previous_stacks) map<PyObject *, Stacktrace>();

            } catch (...) {
                return -1;
            }
            return 0;
        }

        static int traverse(ObjectReader* self, visitproc visit, void* arg) {
            // Py_VISIT(self->m_global_lookup);
            Py_VISIT(self->thread);
            Py_VISIT(self->path);
            Py_VISIT(self->deserializer);
            // Py_VISIT(self->bind_singleton);
            Py_VISIT(self->on_stack_difference);
            Py_VISIT(self->normalize_path);

            // for (const auto& [key, value] : self->lookup) {
            //     Py_VISIT(value);
            // }
            // Py_VISIT(self->name_cache);
            return 0;
        }

        static int clear(ObjectReader* self) {
            // Py_CLEAR(self->name_cache);
            Py_CLEAR(self->thread);
            Py_CLEAR(self->path);
            Py_CLEAR(self->deserializer);
            // Py_CLEAR(self->bind_singleton);
            Py_CLEAR(self->on_stack_difference);
            Py_CLEAR(self->normalize_path);

            return 0;
        }

        static void dealloc(ObjectReader* self) {
            // if (self->file) {
            //     fclose(self->file);
            //     self->file = nullptr;
            // }
            
            PyObject_GC_UnTrack(self);
            clear(self);
            self->stream.~MessageStream();

            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
        }

        // static PyObject * next_control_getter(ObjectReader *self, void *closure) {
        //     if (self->next_control == -1) {
        //         Py_RETURN_NONE;
        //     }

        //     return PyLong_FromLong(self->next_control);
        // }
        
        static PyObject * path_getter(ObjectReader *self, void *closure) {
            return Py_NewRef(self->path);
        }

        // PyObject * stacks() {
        //     PyObject * stack = PyObject_CallNoArgs(stacktrace);
        //     if (!stack) return nullptr;
        //         if (timed_out) {
        //             PyDict_SetItem(timed_out, current_thread, stack);
        //             Py_DECREF(stack);
        //             wakeup.notify_all();
        //             while(true) wakeup.wait(lock);
                    
        //         } else {
        //             timed_out = PyDict_New();
        //             PyDict_SetItem(timed_out, current_thread, stack);

        //             wakeup.notify_all();

        //             int total = pending_reads.size();

        //             wakeup.wait(lock, [this, total]() {
        //                 return PyDict_Size(timed_out) == total;
        //             });

        //             if (on_timeout_callback) {
        //                 PyObject_CallFunctionObjArgs(on_timeout_callback, this, timed_out, nullptr);
        //             }
        //             raise(SIGTRAP);
        //         }
        // }

        static PyObject * py_close(ObjectReader *self, PyObject * unused) {
            self->stream.close();
            Py_RETURN_NONE;
        }

        static int path_setter(ObjectReader *self, PyObject *value, void *closure) {
 
            if (value == nullptr) {
                PyErr_SetString(PyExc_AttributeError, "deletion of 'path' is not allowed");
                return -1;
            }
            else {
                switch (PyObject_RichCompareBool(self->path, value, Py_EQ)) {
                case 1: return 0;
                case 0: {
                    
                    self->stream.close();

                    Py_DECREF(self->path);
                    self->path = Py_NewRef(value);
                    
                    if (!PyCallable_Check(value)) {
                        try {
                            self->stream.set_file(open(self->path));
                            // printf("pid: %i creating object writer", getpid());
                            // self->object_writer = new ObjectWriter(self->path, self->enum_type, true, self->check_pid);
                        } catch (...) {
                            return -1;
                        }
                    }
                    return 0;
                }
                default:
                    return -1;
                }
            }
        }

        // static PyObject * py_wake_pending(ObjectReader * self, PyObject* unused) {
        //     self->wakeup.notify_all();
        //     Py_RETURN_NONE;
        // }

        // static PyObject * py_load_hash_secret(ObjectReader * self, PyObject* unused) {                
        //     try {
        //         self->read((uint8_t *)&_Py_HashSecret, sizeof(_Py_HashSecret_t));
        //         Py_RETURN_NONE;
        //     } catch (...) {
        //         return nullptr;
        //     }

        // }
    };

    static PyMethodDef methods[] = {
        // {"load_hash_secret", (PyCFunction)ObjectReader::py_load_hash_secret, METH_NOARGS, "TODO"},
        // {"wake_pending", (PyCFunction)ObjectReader::py_wake_pending, METH_NOARGS, "TODO"},
        {"bind", (PyCFunction)ObjectReader::py_bind, METH_O, "TODO"},
        {"close", (PyCFunction)ObjectReader::py_close, METH_NOARGS, "TODO"},
        {"exclude_from_stacktrace", (PyCFunction)ReaderWriterBase::py_exclude_from_stacktrace, METH_O, "TODO"},

        // {"dump_pending", (PyCFunction)ObjectReader::py_dump_pending, METH_O, "TODO"},
        // {"supply", (PyCFunction)ObjectReader::py_supply, METH_O, "supply the placeholder"},
        // {"intern", (PyCFunction)ObjectWriter::py_intern, METH_FASTCALL, "TODO"},
        // {"replace", (PyCFunction)ObjectWriter::py_replace, METH_VARARGS | METH_KEYWORDS, "TODO"},
        // {"unique", (PyCFunction)ObjectWriter::py_unique, METH_O, "TODO"},
        // {"delete", (PyCFunction)ObjectWriter::py_delete, METH_O, "TODO"},

        // {"tuple", (PyCFunction)ObjectWriter::py_write_tuple, METH_FASTCALL, "TODO"},
        // // {"dict", (PyCFunction)ObjectWriter::, METH_FASTCALL | METH_KEYWORDS, "TODO"},


        {NULL}  // Sentinel
    };

    static PyMemberDef members[] = {
        // {"bytes_read", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectReader, bytes_read), READONLY, "TODO"},
        // {"messages_read", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectReader, messages_read), READONLY, "TODO"},
        // {"stacktraces", T_OBJECT, OFFSET_OF_MEMBER(ObjectReader, stacktraces), READONLY, "TODO"},
        // {"active_thread", T_OBJECT, OFFSET_OF_MEMBER(ObjectReader, active_thread), READONLY, "TODO"},
        {"verbose", T_BOOL, OFFSET_OF_MEMBER(ObjectReader, verbose), 0, "TODO"},
        // {"stack_stop_at", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectReader, stack_stop_at), 0, "TODO"},
        // {"pending_reads", T_OBJECT, OFFSET_OF_MEMBER(ObjectReader, pending_reads), READONLY, "TODO"},
        // {"path", T_OBJECT, OFFSET_OF_MEMBER(Writer, path), READONLY, "TODO"},
        // {"on_pid_change", T_OBJECT_EX, OFFSET_OF_MEMBER(Writer, on_pid_change), 0, "TODO"},
        {NULL}  /* Sentinel */
    };

    static PyGetSetDef getset[] = {
        {"path", (getter)ObjectReader::path_getter, (setter)ObjectReader::path_setter, "TODO", NULL},
        // {"next_control", (getter)ObjectReader::next_control_getter, nullptr, "TODO", NULL},
        // {"next_control", (getter)ObjectReader::next_control_getter, nullptr, "TODO", NULL},
        // {"pending", (getter)ObjectReader::pending_getter, nullptr, "TODO", NULL},
        // {"thread_number", (getter)Writer::thread_getter, (setter)Writer::thread_setter, "TODO", NULL},
        {NULL}  // Sentinel
    };

    PyTypeObject ObjectReader_Type = {
        .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = MODULE "ObjectReader",
        .tp_basicsize = sizeof(ObjectReader),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)ObjectReader::dealloc,
        .tp_vectorcall_offset = OFFSET_OF_MEMBER(ObjectReader, vectorcall),
        .tp_call = PyVectorcall_Call,
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE,
        .tp_doc = "TODO",
        .tp_traverse = (traverseproc)ObjectReader::traverse,
        .tp_clear = (inquiry)ObjectReader::clear,
        .tp_methods = methods,
        .tp_members = members,
        .tp_getset = getset,
        .tp_init = (initproc)ObjectReader::init,
        .tp_new = ObjectReader::create,
    };
}