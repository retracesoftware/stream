#include "stream.h"
#include <cstdint>
#include <structmember.h>
#include "wireformat.h"
#include <algorithm>
#include <fcntl.h>
#include <sstream>
#include <sys/file.h>

#include <vector>
#include <algorithm> 
#include <span>
#include "unordered_dense.h"
#include "base.h"
#include <thread>
#include <condition_variable>

using namespace ankerl::unordered_dense;

namespace retracesoftware_stream {

    struct ObjectWriter;

    static std::vector<ObjectWriter *> writers;
    static map<PyTypeObject *, freefunc> freefuncs;

    static void on_free(void * obj);

    static void generic_free(void * obj) {
        auto it = freefuncs.find(Py_TYPE(obj));
        if (it != freefuncs.end()) {
            on_free(obj);
            it->second(obj);
        } else {
            // bad situation, a memory leak! Maybe print a bad warning
        }
    }

    static void PyObject_GC_Del_Wrapper(void * obj) {
        on_free(obj);
        PyObject_GC_Del(obj);
    }

    static void PyObject_Free_Wrapper(void * obj) {
        on_free(obj);
        PyObject_Free(obj);
    }

    static bool is_patched(freefunc func) {
        return func == generic_free || 
               func == PyObject_GC_Del_Wrapper ||
               func == PyObject_Free_Wrapper;
    }

    static void patch_free(PyTypeObject * cls) {
        assert(!is_patched(cls->tp_free));
        if (cls->tp_free == PyObject_Free) {
            cls->tp_free = PyObject_Free_Wrapper;
        } else if (cls->tp_free == PyObject_GC_Del) {
            cls->tp_free = PyObject_GC_Del_Wrapper;
        } else {
            freefuncs[cls] = cls->tp_free;
            cls->tp_free = generic_free;
        }
    }

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

    static bool result_to_bool(PyObject * result) {
        if (!result) throw nullptr;
        int status = PyObject_IsTrue(result);
        Py_DECREF(result);
        switch (status) {
            case 0: return false;
            case 1: return true;
            default:
                throw nullptr;
        }
    }
    
    struct ObjectWriter : public ReaderWriterBase {
        
        FILE * file;
        size_t bytes_written;
        size_t messages_written;
        int next_handle;
        PyThreadState * last_thread_state;
        map<PyObject *, int> bindings;
        map<PyObject *, uint16_t> filename_index;
        int binding_counter;
        bool stacktraces;
        int pid;
        bool verbose;
        PyObject * serializer;
        PyObject * enable_when;
        retracesoftware::FastCall thread;
        vectorcallfunc vectorcall;
        PyObject *weakreflist;
        // std::thread flusher;
        // std::condition_variable cv;
        std::mutex write_lock;
        bool magic_markers;

        // std::thread t1(thread_function, 1);

        void write_expected(uint64_t i) {
            if (i < 255) {
                write((uint8_t)i);
            } else {
                write((uint8_t)255);
                write(i);
            }
        }

        template<typename T>
        void write_expected(std::vector<T> vec) {
            write_expected_int(vec.size());
            for (auto elem : vec) {
                write(elem);
            }
        }

        void write_stacktrace() {

            if (stacktraces) {
                static thread_local std::vector<Frame> stack;

                size_t old_size = stack.size();
                size_t skip = update_stack(exclude_stacktrace, stack);
                
                assert(skip <= old_size);

                auto new_frame_elements = std::span(stack).subspan(skip);

                for (auto frame : new_frame_elements) {
                    PyObject * filename = frame.code_object->co_filename;
                    assert(PyUnicode_Check(filename));

                    if (!filename_index.contains(filename)) {
                        write_control(AddFilename);

                        if (normalize_path) {
                            PyObject * normalized = PyObject_CallOneArg(normalize_path, filename);
                            if (!normalized) {  
                                raise(SIGTRAP);
                                throw nullptr;
                            }
                            write(normalized);
                            Py_DECREF(normalized);
                        } else {
                            write(filename);
                        }
                        filename_index[Py_NewRef(filename)] = filename_index_counter++;
                    }
                    // if (!code_objects.contains(frame.code_object)) {
                    //     code_objects[frame.code_object] = code_object_counter++;
                        
                    //     PyObject * hash = PyObject_CallOneArg(hashcode, (PyObject *)frame.code_object);
                    //     if (!hash) throw nullptr;

                    //     write_control(CreateFixedSize(FixedSizeTypes::ADD_CODE_OBJ));
                    //     // for fast lookup, hashcodes might be slow
                    //     write(frame.code_object->co_qualname);
                    //     write(hash);
                    //     Py_DECREF(hash);                            
                    // }
                }

                // one byte
                write_control(Stack);

                // how many to discard
                write_expected(old_size - skip);

                write_expected(new_frame_elements.size());

                for (auto frame : new_frame_elements) {
                    PyObject * filename = frame.code_object->co_filename;
                    
                    write(filename_index[filename]);
                    write(frame.lineno());
                }
                
                write_magic();
            }
        }

        void write_magic() { 
            if (magic_markers) {
                write((uint64_t)MAGIC);
            }
        }

        static PyObject * StreamHandle_vectorcall(StreamHandle * self, PyObject *const * args, size_t nargsf, PyObject* kwnames) {
            
            ObjectWriter * writer = reinterpret_cast<ObjectWriter *>(self->writer);

            std::lock_guard<std::mutex> lock(writer->write_lock);

            if (!writer->file) {
                PyErr_Format(PyExc_RuntimeError, "Cannot write to file: %S as its closed", writer->path);
                return nullptr;
            }
            try {
                if (writer->enabled()) {
                    writer->check_thread();
                    writer->write_stacktrace();

                    writer->write_root_handle_ref(self->index);
                    
                    if (writer->verbose) {
                        PyObject * str = PyObject_Str(self->object);
                        printf("-- %s\n", PyUnicode_AsUTF8(str));
                        Py_DECREF(str);
                    }

                    writer->messages_written++;

                    size_t total_args = PyVectorcall_NARGS(nargsf) + (kwnames ? PyTuple_GET_SIZE(kwnames) : 0);

                    for (size_t i = 0; i < total_args; i++) {
                        writer->write_root(args[i]);
                        if (writer->verbose) {
                            PyObject * str = PyObject_Str(args[i]);
                            printf("-- %s\n", PyUnicode_AsUTF8(str));
                            Py_DECREF(str);
                        }
                        writer->messages_written++;
                    }
                }
                Py_RETURN_NONE;

            } catch (...) {
                return nullptr;
            }
        }
        
        void bind(PyObject * obj, bool ext) {
            check_thread();
            write_stacktrace();

            if (bindings.contains(obj)) {
                PyErr_Format(PyExc_RuntimeError, "object: %S already bound", obj);
                throw nullptr;
            }

            if (!is_patched(Py_TYPE(obj)->tp_free)) {
                patch_free(Py_TYPE(obj));
            }
            bindings[obj] = binding_counter++;

            if (ext) {
                if (!bindings.contains((PyObject *)Py_TYPE(obj))) {
                    PyErr_Format(PyExc_RuntimeError, "to externally bind object: %S, object type: %S must have been bound", obj, Py_TYPE(obj));
                    throw nullptr;
                }
                write(FixedSizeTypes::EXT_BIND);
                write_lookup(bindings[(PyObject *)Py_TYPE(obj)]);

            } else {
                write(Bind);
            }
            write_magic();
        }

        void write_delete(int id) {
            if (verbose) {
                printf("DELETE - %i, delta - %i\n", id, next_handle - id - 1);
            }
            int delta = next_handle - id;

            assert (delta > 0);

            write_unsigned_number(SizedTypes::DELETE, delta - 1);
        }

        static void StreamHandle_dealloc(StreamHandle* self) {
            
            reinterpret_cast<ObjectWriter *>(self->writer)->write_delete(self->index);

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
            write(NewHandle);
            write(obj);

            if (verbose) {
                PyObject * str = PyObject_Str(obj);
                printf("-- %s\n", PyUnicode_AsUTF8(str));
                Py_DECREF(str);
            }
            // messages_written++;
            return stream_handle(next_handle++, verbose ? obj : nullptr);
        }

        void write_str(PyObject * obj) {
            Py_ssize_t size;
            const char * utf8 = PyUnicode_AsUTF8AndSize(obj, &size);
            write_size(SizedTypes::STR, (int)size);
            write((const uint8_t *)utf8, size);
        }

        void write_serialized(PyObject * obj) {

            PyObject * res;

            if (PyGC_IsEnabled()) {
                PyGC_Disable();
                res = PyObject_CallOneArg(serializer, obj);
                PyGC_Enable();
            } else {
                res = PyObject_CallOneArg(serializer, obj);
            }

            if (!res) {
                throw nullptr;
            } else {
                if (PyBytes_Check(res)) {
                    write_pickled(res);
                } else {
                    write(res);
                }
                Py_DECREF(res);
            }            
            // else if (!PyBytes_Check(res)) {
            //     PyErr_Format(PyExc_TypeError, "serializer function %S return an object which was not bytes: %S", serializer, res);
            //     Py_DECREF(res);
            //     throw nullptr;
            // } else {
            //     write_pickled(res);
            //     Py_DECREF(res);
            // }
        }

        void write_bignum(PyObject * pylong) {

            // _PyLong_NumBits()
            Py_ssize_t nbits = _PyLong_NumBits(pylong);
            size_t expected = (nbits + 7) / 8;

            uint8_t *bignum = (uint8_t *)malloc(expected);
        
            if (!bignum) {
                PyErr_SetString(PyExc_MemoryError, "bignum malloc failed.");
                throw nullptr;
            }

            // Safely get the entire value.
            int little_endian = 0;
            int is_signed = 1;

            int bytes = _PyLong_AsByteArray(reinterpret_cast<PyLongObject *>(pylong), bignum, expected, little_endian, is_signed);

            if (bytes < 0) {  // Exception has been set.
                free(bignum);
                throw nullptr;
            }
            
            else if ((size_t)bytes > expected) {  // This should not be possible.
                PyErr_SetString(PyExc_RuntimeError,
                    "Unexpected bignum truncation after a size check.");
                free(bignum);
                throw nullptr;
            }

            try {
                write_size(SizedTypes::BIGINT, expected);
                write((const uint8_t *) bignum, expected);
                // The expected success given the above pre-check.
                // ... use bignum ...
                free(bignum);
            } catch (...) {
                free(bignum);
                throw;
            }
        }

        void write_int(PyObject * obj) {
            long l = PyLong_AsLong(obj);
            if (PyErr_Occurred()) {
                if (PyErr_ExceptionMatches(PyExc_OverflowError)) {
                    PyErr_Clear();
                    write_bignum(obj);
                } else {
                    throw nullptr;
                }
            } else {
                write_sized_int(l);
            }
        }

        void write_bytes_data(PyObject * obj) {
            write((const uint8_t *)PyBytes_AsString(obj), PyBytes_GET_SIZE(obj));
        }

        void write_pickled(PyObject * bytes) {

            // assert(PyType_Check(bytes, &PyBytes_Type));
            write_size(SizedTypes::PICKLED, PyBytes_GET_SIZE(bytes));
            write_bytes_data(bytes);
        }
        
        // void write_pickle(PyObject * obj) {

        //     auto bytes = PyObjectPtr(dumps(obj));

        //     if (!bytes.get()) {
        //         PyErr_Print();
        //         raise(SIGTRAP);
        //         throw std::exception();
        //     }
        //     assert(Py_TYPE(bytes.get()) == &PyBytes_Type);

        //     write_size(SizedTypes::PICKLED, PyBytes_GET_SIZE(bytes.get()));
        //     write_bytes_data(bytes.get());
        // }

        void write_float(PyObject * obj) {
            write(FixedSizeTypes::FLOAT);
            write(PyFloat_AsDouble(obj));
        }

        void write_bytes(PyObject * obj) {
            size_t len = PyBytes_GET_SIZE(obj);
            write_size(SizedTypes::BYTES, len);
            if (len > 0) {
                write_bytes_data(obj);
            } else {
                raise(SIGTRAP);
            }
        }

        void write_dict(PyObject * obj) {
            assert(PyDict_Check(obj));

            write_dict_header(PyDict_Size(obj));

            Py_ssize_t pos = 0;
            PyObject *key, *value;

            while (PyDict_Next(obj, &pos, &key, &value)) {
                write(key);
                write(value);
            }
        }

        void write_tuple(PyObject * obj) {
            assert(PyTuple_Check(obj));

            write_tuple_header(PyTuple_GET_SIZE(obj));

            for (int i = 0; i < PyTuple_GET_SIZE(obj); i++) {
                assert(PyTuple_GET_ITEM(obj, i) != obj);
                write(PyTuple_GET_ITEM(obj, i));
            }
        }

        // void write_global_ref(PyObject * obj) {
        //     assert(PyTuple_Check(obj));

        //     write(FixedSizeTypes::GLOBAL);
        //     write_tuple(obj);
        // }

        void write_list(PyObject * obj) {
            assert(PyList_Check(obj));
            write_size(SizedTypes::LIST, PyList_GET_SIZE(obj));
            
            for (int i = 0; i < PyList_GET_SIZE(obj); i++) {
                write(PyList_GET_ITEM(obj, i));
            }
        }

        void write_bool(PyObject * obj) {
            write(obj == Py_True ? FixedSizeTypes::TRUE : FixedSizeTypes::FALSE);
        }

        // void write_ref(PyObject * reference) {
        //     write(FixedSizeTypes::REF);
        //     write_pointer((void *)reference);
        // }

        void write_memory_view(PyObject * obj) {
            Py_buffer *view = PyMemoryView_GET_BUFFER(obj);
            assert(view->readonly);

            write_size(SizedTypes::BYTES, view->len);
            write((const uint8_t *)view->buf, view->len);
        }

        // PyObject * get_global_id(PyObject * obj) {
        //     PyObject * res = PyObject_CallOneArg(m_global_lookup, obj);
        //     if (!res) {
        //         throw std::exception();
        //     }
        //     return res;
        // }

        static FILE * open(PyObject * path, bool append) {

            if (PyCallable_Check(path)) {
                PyObject * res = PyObject_CallNoArgs(path);

                if (!res) throw nullptr;

                FILE * file = open(res, append);
                
                Py_DECREF(res);

                return file;

            } else {

                PyObject * path_str = PyObject_Str(path);
                if (!path_str) throw nullptr;

                // int fd = open(PyUnicode_AsUTF8(path_str), O_WRONLY | O_CREAT | O_EXCL, 0644);
                
                const char * mode = append ? "ab" : "wb";

                FILE * file = fopen(PyUnicode_AsUTF8(path_str), mode);

                Py_DECREF(path_str);

                if (!file) {
                    PyErr_Format(PyExc_IOError, "Could not open file: %S, mode: %s for writing, error: %s", path, mode, strerror(errno));
                    throw nullptr;
                }

                int fd = fileno(file);
                
                if (flock(fd, LOCK_EX | LOCK_NB) == -1) {
                    fprintf(stderr, "TRIED TO LOCK AN ALREADY LOCKED FILE!!!!\n");

                    PyErr_Format(PyExc_IOError, "Could not lock file: %S for exclusive access, error: %s", path, strerror(errno));
                    // perror("flock");
                    // // Handle locking failure: another process holds the lock
                    fclose(file);
                    throw nullptr;
                }
                return file;
            }
        }

        inline void write(Control control) {
            write(control.raw);
        }

        inline void write(FixedSizeTypes obj) {
            if (verbose) {
                printf("%s ", FixedSizeTypes_Name(obj));
            }
            write(create_fixed_size(obj));
        }

        inline void write(const uint8_t * bytes, Py_ssize_t size) {

            assert(bytes);
            assert(file);

            if (pid && pid != getpid()) {
                fprintf(stderr, "TRIED TO WRITE TO FILE WHERE PID ISN'T OWNED!!!!!\n");
            }

            assert (!pid || pid == getpid());

            int written = fwrite(bytes, 1, size, file);

            bytes_written += written;

            if (written < size) {
                std::ostringstream error;

                error << "Write failed, tried to write: " << size << " bytes but wrote: " << written << " bytes";

                throw error.str();
            }
        }

        inline void write(uint8_t value) {
            write(&value, sizeof(value));
        }

        inline void write(int8_t value) {
            return write((uint8_t)value);
        }

        inline void write(uint16_t value) {
            uint8_t buffer[sizeof(value)];
            buffer[0] = (uint8_t)value;
            buffer[1] = (uint8_t)(value >> 8);
            write(buffer, sizeof(value));
        }

        inline void write(int16_t value) {
            return write((uint16_t)value);
        }

        inline void write(uint32_t value) {
            uint8_t buffer[sizeof(value)];
            buffer[0] = (uint8_t)value;
            buffer[1] = (uint8_t)(value >> 8);
            buffer[2] = (uint8_t)(value >> 16);
            buffer[3] = (uint8_t)(value >> 24);

            write(buffer, sizeof(value));
        }

        inline void write(int32_t value) {
            write((uint32_t)value);
        }

        inline void write(uint64_t value) {
            uint8_t buffer[sizeof(value)];
            buffer[0] = (uint8_t)value;
            buffer[1] = (uint8_t)(value >> 8);
            buffer[2] = (uint8_t)(value >> 16);
            buffer[3] = (uint8_t)(value >> 24);
            buffer[4] = (uint8_t)(value >> 32);
            buffer[5] = (uint8_t)(value >> 40);
            buffer[6] = (uint8_t)(value >> 48);
            buffer[7] = (uint8_t)(value >> 56);
            write(buffer, sizeof(value));
        }

        void write(int64_t value) {
            write((uint64_t)value);
        }

        void write(double d) {
            write(*(uint64_t *)&d);
        }

        void write_control(Control value) {
            write(value.raw);
        }

        // void write_control(uint8_t value) {
        //     write(value);
        // }

        void write_size(SizedTypes type, Py_ssize_t size) {
            assert (type < 16);

            if (verbose) {
                printf("%s(%i) ", SizedTypes_Name(type), size);
            }

            Control control;
            control.Sized.type = type;

            if (size <= 11) {
                control.Sized.size = (Sizes)size;
                write_control(control);
            } else {
                if (size < UINT8_MAX) {
                    control.Sized.size = Sizes::ONE_BYTE_SIZE;
                    write_control(control);
                    write((int8_t)size);
                } else if (size < UINT16_MAX) { 
                    control.Sized.size = Sizes::TWO_BYTE_SIZE;
                    write_control(control);
                    write((int16_t)size);
                } else if (size < UINT32_MAX) { 
                    control.Sized.size = Sizes::FOUR_BYTE_SIZE;
                    write_control(control);
                    write((int32_t)size);
                } else {
                    control.Sized.size = Sizes::EIGHT_BYTE_SIZE;
                    write_control(control);
                    write((int64_t)size);
                }
            }
        }

        inline size_t get_bytes_written() const { return bytes_written; }

        void write_pointer(void * ptr) {
            write(*(uint64_t *)&ptr);
        }

        void write_tuple_header(size_t size) {
            write_size(SizedTypes::TUPLE, size);
        }

        void write_dict_header(size_t size) {
            write_size(SizedTypes::DICT, size);
        }

        // void write_bytes_written(uint64_t bytes_written) {
        //     write_control(RootTypes::BYTES_WRITTEN);
        //     write(bytes_written);
        // }

        void write_unsigned_number(SizedTypes type, uint64_t l) {
            write_size(type, l);
        }

        void write_sized_int(int64_t l) {
            if (l >= 0) {
                write_unsigned_number(SizedTypes::UINT, l);
            } else if (l == -1) {
                write_control(CreateFixedSize(FixedSizeTypes::NEG1));
            } else {
                write_control(CreateFixedSize(FixedSizeTypes::INT64));
                write(l);
            }
        }

        void operator()(const char * cstr) {
            int size = strlen(cstr) + 1;

            write_size(SizedTypes::STR, size);
            write((const uint8_t *)cstr, size);
        }

        // inline void operator()(RootTypes root) {
        //     write((uint8_t)root);
        // }

        void write_root_handle_ref(int handle) {
            write_handle_ref(handle);
            write_magic();
        }

        void write_handle_ref(int handle) {
            write_unsigned_number(SizedTypes::HANDLE, handle);
        }

        void write_stream_handle(PyObject * obj) {
            assert (Py_TYPE(obj) == &StreamHandle_Type);

            StreamHandle * handle = reinterpret_cast<StreamHandle *>(obj);

            write_handle_ref(handle->index);

            if (verbose) {
                PyObject * str = PyObject_Str(handle->object);
                printf("-- %s\n", PyUnicode_AsUTF8(str));
                Py_DECREF(str);
            }
        }

        // PyObject * foo(PyObject * obj) {
        //     return bar_vectorcall(bar, &obj, 1, nullptr);
        // }

        //     if (memo) {
        //         PyObject * memoized = memo_vectorcall(memo, &obj, 1, nullptr);
        //         if (!memoized) {
        //             throw nullptr;
        //         }
        //         else if (memoized != Py_None) {
                    
        //             write(obj, nullptr);

        //             return true;
        //         }
        //     }
        //     return false;
        // }

        void write_lookup(int ref) {
            write_unsigned_number(SizedTypes::BINDING, ref);   
        }

        void write_root(PyObject * obj) {
            write(obj);
            write_magic();
        }

        void write(PyObject * obj) {
            
            // {
            //     PyObject * s = PyObject_Str(obj);
            //     if (s) {
            //         printf("C++ write: %s\n", PyUnicode_AsUTF8(s));
            //         Py_DECREF(s);
            //     } else {
            //         PyErr_Clear();
            //         printf("C++ write type: %s\n", Py_TYPE(obj)->tp_name);
            //     }
            // }
            
            assert(obj);
            // if (obj == nullptr) write(FixedSizeTypes::C_NULL);

            if (obj == Py_None) write(FixedSizeTypes::NONE);

            else if (Py_TYPE(obj) == &PyUnicode_Type) write_str(obj);
            else if (Py_TYPE(obj) == &PyLong_Type) write_int(obj);

            // else if (PyObject_TypeCheck(obj, &Proxy_Type)) {

            //     write(FixedSizeTypes::REF);
            //     // check if lookup has an entry for Py_TYPE(obj)
            //     if (!write_lookup((PyObject *)Py_TYPE(obj))) {
            //         PyErr_Format(PyExc_RuntimeError, "TODO");
            //         throw nullptr;
            //     }

            //     // write(FixedSizeTypes::REF);
            //     // if (PyObject_TypeCheck(obj, &RetraceTracked_Type)) {
            //     //     write_pointer(obj);
            //     // } else {
            //     //     write_pointer(Py_TYPE(obj));
            //     // }
            // }

            // else if (Py_TYPE(obj) == &Reference_Type) write_reference(obj);
            else if (Py_TYPE(obj) == &PyBytes_Type) write_bytes(obj);
            else if (Py_TYPE(obj) == &PyBool_Type) write_bool(obj);
            else if (Py_TYPE(obj) == &PyTuple_Type) write_tuple(obj);
            else if (Py_TYPE(obj) == &PyList_Type) write_list(obj);
            else if (Py_TYPE(obj) == &PyDict_Type) write_dict(obj);

            else if (bindings.contains(obj)) write_lookup(bindings[obj]);

            // else if (Py_TYPE(obj) == &GlobalRef_Type) write_global_ref(obj);
            // else if (Py_TYPE(obj) == &PyType_Type) ;
            // else if (Py_TYPE(obj) == &Pickled_Type) write_pickled(obj);
            else if (Py_TYPE(obj) == &PyFloat_Type) write_float(obj);

            // else if (placeholders.contains(obj)) write_handle_ref(placeholders[obj]);

            else if (Py_TYPE(obj) == &PyMemoryView_Type) write_memory_view(obj);
            else if (Py_TYPE(obj) == &StreamHandle_Type) write_stream_handle(obj);


            // else if (type_serializers.contains(Py_TYPE(obj))) {
            //     PyObject * res = type_serializers[Py_TYPE(obj)](obj);
            //     if (!res) throw nullptr;
            //     write(res);
            //     Py_DECREF(res);
            // }

            // else if (Py_TYPE(obj) == &StableSet_Type) write_stableset(obj);
            // else if (Py_TYPE(obj) == &StableFrozenSet_Type) write_stablefrozenset(obj);
            // else if (enumtype && PyObject_TypeCheck(obj, enumtype)) write_enum(obj);

            else write_serialized(obj);
            // // dispatch[&ExtReference_Type] = &Writer::write_ext_reference;
            // dispatch[&PyType_Type] = &ObjectWriter::write_type;
        }

        // static PyObject * py_write_dict(ObjectWriter * self, PyObject *const *args, Py_ssize_t nargs, PyObject *kwnames) {
        //     if (nargs > 0) {
        //         PyErr_SetString(PyExc_TypeError, "write_dict only accepts keyword arguments");
        //         return nullptr;
        //     }
        //     if (!kwnames) {
        //         self->write_dict_header(0);    
        //     } else {
        //         self->write_dict_header(PyTuple_GET_SIZE(kwnames));
        //         for (int i = 0; i < PyTuple_GET_SIZE(kwnames); i++) {
        //             self->write(PyTuple_GET_ITEM(kwnames, i));
        //             self->write(args[i]);
        //         }
        //     }
        //     self->messages_written++;
        //     Py_RETURN_NONE;
        // }

        // static PyObject * py_write_tuple(ObjectWriter * self, PyObject *const *args, Py_ssize_t nargs) {
        //     self->write_tuple_header(nargs);
        //     for (int i = 0; i < nargs; i++) {
        //         write(args[i]);
        //     }
        //     self->messages_written++;
        // }

        void write_all(PyObject*const * args, size_t nargs) {
            // size_t before = bytes_written;
            check_thread();
            write_stacktrace();

            for (size_t i = 0; i < nargs; i++) {
                write_root(args[i]);

                if (verbose) {
                    PyObject * str = PyObject_Str(args[i]);
                    printf("written: %s\n", PyUnicode_AsUTF8(str));
                    Py_DECREF(str);
                }
                messages_written++;
            }
            // if (magic_markers) {
            //     write_control(Checksum);
            //     write_expected(bytes_written - before);
            // }
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
            std::lock_guard<std::mutex> lock(self->write_lock);

            if (kwnames) {
                PyErr_SetString(PyExc_TypeError, "ObjectWriter does not accept keyword arguments");
                return nullptr;
            }
            
            if (!self->file) {
                PyErr_Format(PyExc_RuntimeError, "Cannot write to file: %S as its closed", self->path);
                return nullptr;
            }

            try {    
                if (self->enabled()) {            
                    size_t nargs = PyVectorcall_NARGS(nargsf);

                    if (self->file) {
                        self->write_all(args, nargs);
                    } else {
                        self->file = open(self->path, true);
                        try {
                            self->write_all(args, nargs);
                            fclose(self->file);
                            self->file = nullptr;
                        } catch (...) {
                            fclose(self->file);
                            self->file = nullptr;
                        }
                    }
                }
                Py_RETURN_NONE;

            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_flush(ObjectWriter * self, PyObject* unused) {
            std::lock_guard<std::mutex> lock(self->write_lock);
            try {
                if (self->file) fflush(self->file);
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_close(ObjectWriter * self, PyObject* unused) {
            std::lock_guard<std::mutex> lock(self->write_lock);
            if (!self->file) {
                PyErr_Format(PyExc_RuntimeError, "File %S is already closed", self->file);
                return nullptr;
            }

            if (self->file) {
                fclose(self->file);
                self->file = nullptr;
            }
            Py_RETURN_NONE;
        }

        static PyObject * py_reopen(ObjectWriter * self, PyObject* unused) {
            std::lock_guard<std::mutex> lock(self->write_lock);
            if (self->file) {
                PyErr_Format(PyExc_RuntimeError, "File %S is already opened", self->file);
                return nullptr;
            }
            
            try {
                self->file = open(self->path, true);
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_store_hash_secret(ObjectWriter * self, PyObject* unused) {                
            try {
                void *secret = &_Py_HashSecret;
                self->write((const uint8_t *)secret, sizeof(_Py_HashSecret_t));
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        // static PyObject * py_write(ObjectWriter * self, PyObject* obj) {
        //     try {
        //         return self->handle(obj, true);
        //     } catch (...) {
        //         return nullptr;
        //     }
        // }

        // static PyObject * WeakRefCallback_vectorcall(WeakRefCallback * self, PyObject *const * args, size_t nargsf, PyObject* kwnames) {
            
        //     ObjectWriter * writer = reinterpret_cast<ObjectWriter *>(self->writer);

        //     assert(writer->placeholders.contains(self->handle));

        //     writer->write_delete(writer->placeholders[self->handle]);
        //     writer->placeholders.erase(self->handle);

        //     Py_DECREF(args[0]);
        //     Py_RETURN_NONE;
        // }

        // PyObject * weakref_callback(PyObject* handle) {
        //     WeakRefCallback * self = (WeakRefCallback *)WeakRefCallback_Type.tp_alloc(&WeakRefCallback_Type, 0);
        //     if (!self) return nullptr;

        //     self->writer = Py_NewRef(this);
        //     self->handle = handle;
        //     self->vectorcall = (vectorcallfunc)WeakRefCallback_vectorcall;
            
        //     return (PyObject *)self;
        // }

        // static PyObject * py_placeholder(ObjectWriter * self, PyObject* obj) {

        //     PyObject * callback = self->weakref_callback(obj);

        //     if (!PyWeakref_NewRef(obj, callback)) {
        //         Py_DECREF(callback);
        //         return nullptr;
        //     }
        //     Py_DECREF(callback);
            
        //     self->write(FixedSizeTypes::PLACEHOLDER);
        //     self->placeholders[obj] = self->next_handle++;

        //     Py_RETURN_NONE;
        // }

        static PyObject * py_handle(ObjectWriter * self, PyObject* obj) {
            std::lock_guard<std::mutex> lock(self->write_lock);
            try {
                return self->handle(obj);
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_bind(ObjectWriter * self, PyObject* obj) {
            std::lock_guard<std::mutex> lock(self->write_lock);
            try {
                self->bind(obj, false);
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_ext_bind(ObjectWriter * self, PyObject* obj) {
            std::lock_guard<std::mutex> lock(self->write_lock);
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
            PyObject * serializer;
            PyObject * thread = nullptr;
            PyObject * normalize_path = nullptr;;

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
            self->serializer = Py_NewRef(serializer);
            self->thread = thread ? retracesoftware::FastCall(thread) : retracesoftware::FastCall();
            Py_XINCREF(thread);

            self->bytes_written = self->messages_written = 0;
            // enumtype(enumtype);
            self->next_handle = 0;
            
            // new (&self->placeholders) map<PyObject *, uint64_t>();
            // new (&self->type_serializers) map<PyTypeObject *, retracesoftware::FastCall>();

            self->file = open(path, false);
            self->vectorcall = reinterpret_cast<vectorcallfunc>(ObjectWriter::py_vectorcall);
            self->last_thread_state = PyThreadState_Get();
            self->binding_counter = 0;
            self->filename_index_counter = 0;
            self->normalize_path = Py_XNewRef(normalize_path);
            self->enable_when = nullptr;
            // self->stack_stop_at = 0;

            new (&self->bindings) map<PyObject *, int>();
            new (&self->filename_index) map<PyObject *, uint16_t>();
            new (&self->write_lock) std::mutex();

            new (&self->exclude_stacktrace) set<PyFunctionObject *>();

            writers.push_back(self);

            // new (&self->cv) std::condition_variable();
            // new (&self->flusher) std::thread(&ObjectWriter::flush_loop, self);

            return 0;
        }

        void check_thread() {
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
                write(ThreadSwitch);
                write(thread_handle);
            }
        }

        static int traverse(ObjectWriter* self, visitproc visit, void* arg) {
            // Py_VISIT(self->m_global_lookup);
            Py_VISIT(self->thread.callable);
            Py_VISIT(self->path);
            Py_VISIT(self->serializer);
            Py_VISIT(self->normalize_path);
            return 0;
        }

        static int clear(ObjectWriter* self) {
            // Py_CLEAR(self->name_cache);
            Py_CLEAR(self->thread.callable);
            Py_CLEAR(self->path);
            Py_CLEAR(self->serializer);
            Py_CLEAR(self->normalize_path);
            return 0;
        }

        static void dealloc(ObjectWriter* self) {
            // lock 
            if (self->file) {
                fclose(self->file);
                self->file = nullptr;
            }
            
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

        void object_freed(PyObject * obj) {
            if (file) {
                auto it = bindings.find(obj);

                // is there an integer binding for this?
                if (it != bindings.end()) {
                    check_thread();
                    write_unsigned_number(SizedTypes::BINDING_DELETE, it->second);
                    bindings.erase(it);
                }
            }
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
            else {
                switch (PyObject_RichCompareBool(self->path, value, Py_EQ)) {
                case 1: return 0;
                case 0: {

                    // if there is an existing object writer, destroy it
                    if (self->file) {
                        fclose(self->file);
                        self->file = nullptr;
                    }
                    Py_DECREF(self->path);
                    self->path = Py_NewRef(value);
                    
                    if (!PyCallable_Check(value)) {
                        try {
                            self->file = open(self->path, true);

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
    };

    static void on_free(void * obj) {
        for (ObjectWriter * writer : writers) {
            writer->object_freed((PyObject *)obj);
        }
    }

    // PyTypeObject WeakRefCallback_Type = {
    //     .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
    //     .tp_name = MODULE "WeakRefCallback",
    //     .tp_basicsize = sizeof(WeakRefCallback),
    //     .tp_itemsize = 0,
    //     .tp_dealloc = generic_gc_dealloc,
    //     .tp_vectorcall_offset = OFFSET_OF_MEMBER(WeakRefCallback, vectorcall),
    //     .tp_call = PyVectorcall_Call,
    //     .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_HAVE_VECTORCALL,
    //     .tp_doc = "TODO",
    //     .tp_traverse = (traverseproc)WeakRefCallback::traverse,
    //     .tp_clear = (inquiry)WeakRefCallback::clear,
    // };

    PyMemberDef StreamHandle_members[] = {
        {"index", T_ULONGLONG, OFFSET_OF_MEMBER(StreamHandle, index), READONLY, "TODO"},
        {NULL}
    };

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
        {"close", (PyCFunction)ObjectWriter::py_close, METH_NOARGS, "TODO"},
        {"reopen", (PyCFunction)ObjectWriter::py_reopen, METH_NOARGS, "TODO"},
        {"bind", (PyCFunction)ObjectWriter::py_bind, METH_O, "TODO"},
        {"ext_bind", (PyCFunction)ObjectWriter::py_ext_bind, METH_O, "TODO"},
        {"store_hash_secret", (PyCFunction)ObjectWriter::py_store_hash_secret, METH_NOARGS, "TODO"},
        {"exclude_from_stacktrace", (PyCFunction)ReaderWriterBase::py_exclude_from_stacktrace, METH_O, "TODO"},

        // {"unique", (PyCFunction)ObjectWriter::py_unique, METH_O, "TODO"},
        // {"delete", (PyCFunction)ObjectWriter::py_delete, METH_O, "TODO"},

        // {"tuple", (PyCFunction)ObjectWriter::py_write_tuple, METH_FASTCALL, "TODO"},
        // {"dict", (PyCFunction)ObjectWriter::, METH_FASTCALL | METH_KEYWORDS, "TODO"},
        {NULL}  // Sentinel
    };

    static PyMemberDef members[] = {
        {"bytes_written", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectWriter, bytes_written), READONLY, "TODO"},
        {"messages_written", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectWriter, messages_written), READONLY, "TODO"},
        // {"stack_stop_at", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectWriter, stack_stop_at), 0, "TODO"},
        {"verbose", T_BOOL, OFFSET_OF_MEMBER(ObjectWriter, verbose), 0, "TODO"},
        {"normalize_path", T_OBJECT, OFFSET_OF_MEMBER(ObjectWriter, normalize_path), 0, "TODO"},
        {"enable_when", T_OBJECT, OFFSET_OF_MEMBER(ObjectWriter, enable_when), 0, "TODO"},

        // {"path", T_OBJECT, OFFSET_OF_MEMBER(Writer, path), READONLY, "TODO"},
        // {"on_pid_change", T_OBJECT_EX, OFFSET_OF_MEMBER(Writer, on_pid_change), 0, "TODO"},
        {NULL}  /* Sentinel */
    };

    static PyGetSetDef getset[] = {
        {"path", (getter)ObjectWriter::path_getter, (setter)ObjectWriter::path_setter, "TODO", NULL},
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
        .tp_weaklistoffset = offsetof(ObjectWriter, weakreflist),
        .tp_methods = methods,
        .tp_members = members,
        .tp_getset = getset,
        .tp_init = (initproc)ObjectWriter::init,
        .tp_new = PyType_GenericNew,
    };
}