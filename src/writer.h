#include "stream.h"
#include "wireformat.h"
#include <sys/fcntl.h>
#include <sys/file.h>
#include <system_error>
#include <cerrno>
#include <sstream>
#include <span>

namespace retracesoftware_stream {

    static FILE * open(const char * path, bool append) {

        assert(path);

        const char * mode = append ? "ab" : "wb";

        FILE * file = fopen(path, mode);

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

    int StreamHandle_index(PyObject *);

    class PrimitiveStream {
        FILE * file;
        size_t bytes_written = 0;
        int write_timeout;
        bool verbose = false;

    public:
        PrimitiveStream(const char * path, int write_timeout) : 
            file(open(path, false)),
            write_timeout(write_timeout) {}

        ~PrimitiveStream() { close(); }

        inline size_t get_bytes_written() const { return this->bytes_written; }

        void change_output(const char * path) {
            if (file) {
                fclose(file);
                file = nullptr;
            }
            file = open(path, true);
        }

        void close() {
            if (file) {
                fclose(file);
                file = nullptr;
            }
        }

        void write_handle_ref(int handle) {
            write_unsigned_number(SizedTypes::HANDLE, handle);
        }

        void write_size(SizedTypes type, Py_ssize_t size) {
            assert (type < 16);

            if (verbose) {
                printf("%s(%i) ", SizedTypes_Name(type), (int)size);
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

        inline void write(const uint8_t * bytes, Py_ssize_t size) {

            assert(bytes);
            assert(file);

            // if (pid && pid != getpid()) {
            //     fprintf(stderr, "TRIED TO WRITE TO FILE WHERE PID ISN'T OWNED!!!!!\n");
            // }

            // assert (!pid || pid == getpid());

            int written = fwrite(bytes, 1, size, file);

            bytes_written += written;

            if (written < size) {
                std::ostringstream error;

                error << "Write failed, tried to write: " << size << " bytes but wrote: " << written << " bytes";

                throw error.str();
            }
        }

        void write_lookup(int ref) {
            write_unsigned_number(SizedTypes::BINDING, ref);   
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

        inline void write(Control control) {
            write(control.raw);
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

        void write_str(PyObject * obj) {
            Py_ssize_t size;
            const char * utf8 = PyUnicode_AsUTF8AndSize(obj, &size);
            write_size(SizedTypes::STR, (int)size);
            write((const uint8_t *)utf8, size);
        }

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

        void write_float(PyObject * obj) {
            write(FixedSizeTypes::FLOAT);
            write(PyFloat_AsDouble(obj));
        }

        void write_bytes(PyObject * obj) {
            write_size(SizedTypes::BYTES, PyBytes_GET_SIZE(obj));
            write_bytes_data(obj);
        }

        void write_bytes_data(PyObject * obj) {
            write((const uint8_t *)PyBytes_AsString(obj), PyBytes_GET_SIZE(obj));
        }

        void write_pickled(PyObject * bytes) {

            // assert(PyType_Check(bytes, &PyBytes_Type));
            write_size(SizedTypes::PICKLED, PyBytes_GET_SIZE(bytes));
            write_bytes_data(bytes);
        }

        void write_bool(PyObject * obj) {
            write(obj == Py_True ? FixedSizeTypes::TRUE : FixedSizeTypes::FALSE);
        }

        void write_memory_view(PyObject * obj) {
            Py_buffer *view = PyMemoryView_GET_BUFFER(obj);
            assert(view->readonly);

            write_size(SizedTypes::BYTES, view->len);
            write((const uint8_t *)view->buf, view->len);
        }

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

        inline void write(FixedSizeTypes obj) {
            if (verbose) {
                printf("%s ", FixedSizeTypes_Name(obj));
            }
            write(create_fixed_size(obj));
        }

        bool is_closed() { return file == nullptr; }

        void flush() { 
            if (file) {
                fflush(file);
            }
        }
    };

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

    class MessageStream {
        PrimitiveStream stream;
        PyObject * serializer;
        map<PyObject *, int> bindings;
        int binding_counter = 0;
        PyObject * normalize_path = nullptr;
        map<PyObject *, uint16_t> filename_index;
        int filename_index_counter = 0;

        // PyThreadState * last_thread_state = nullptr;
        // retracesoftware::FastCall thread;

        void write_stream_handle(PyObject * obj) {
            // assert (Py_TYPE(obj) == &StreamHandle_Type);

            // StreamHandle * handle = reinterpret_cast<StreamHandle *>(obj);

            stream.write_handle_ref(StreamHandle_index(obj));

            // if (verbose) {
            //     PyObject * str = PyObject_Str(handle->object);
            //     printf("-- %s\n", PyUnicode_AsUTF8(str));
            //     Py_DECREF(str);
            // }
        }

        void write_dict(PyObject * obj) {
            assert(PyDict_Check(obj));

            stream.write_dict_header(PyDict_Size(obj));

            Py_ssize_t pos = 0;
            PyObject *key, *value;

            while (PyDict_Next(obj, &pos, &key, &value)) {
                write(key);
                write(value);
            }
        }

        void write_tuple(PyObject * obj) {
            assert(PyTuple_Check(obj));

            stream.write_tuple_header(PyTuple_GET_SIZE(obj));

            for (int i = 0; i < PyTuple_GET_SIZE(obj); i++) {
                assert(PyTuple_GET_ITEM(obj, i) != obj);
                write(PyTuple_GET_ITEM(obj, i));
            }
        }

        void write_list(PyObject * obj) {
            assert(PyList_Check(obj));
            stream.write_size(SizedTypes::LIST, PyList_GET_SIZE(obj));
            
            for (int i = 0; i < PyList_GET_SIZE(obj); i++) {
                write(PyList_GET_ITEM(obj, i));
            }
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
                    stream.write_pickled(res);
                } else {
                    write(res);
                }
                Py_DECREF(res);
            }
        }

    public:
        
        MessageStream(
            const char * path,
            int write_timeout,
            PyObject * serializer) : 
            stream(path, write_timeout), 
            serializer(Py_XNewRef(serializer)) {


            // thread(retracesoftware::FastCall(thread)) {
            // Py_XINCREF(thread);
            // last_thread_state = PyThreadState_Get();
        }

        ~MessageStream() {
            Py_XDECREF(serializer);
        }

        void traverse(visitproc visit, void* arg) {
            if (serializer) visit(serializer, arg);
            if (normalize_path) visit(normalize_path, arg);
        }

        void set_normalize_path(PyObject * normalize_path) {
            Py_XDECREF(this->normalize_path);
            this->normalize_path = Py_XNewRef(normalize_path);
        }

        void write_stacktrace(const set<PyFunctionObject *>& exclude_stacktrace) {

            static thread_local std::vector<Frame> stack;

            size_t old_size = stack.size();
            size_t skip = update_stack(exclude_stacktrace, stack);
            
            assert(skip <= old_size);

            auto new_frame_elements = std::span(stack).subspan(skip);

            for (auto frame : new_frame_elements) {
                PyObject * filename = frame.code_object->co_filename;
                assert(PyUnicode_Check(filename));

                if (!filename_index.contains(filename)) {
                    stream.write_control(AddFilename);

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
            }

            // one byte
            stream.write_control(Stack);

            // how many to discard
            stream.write_expected(old_size - skip);

            stream.write_expected(new_frame_elements.size());

            for (auto frame : new_frame_elements) {
                PyObject * filename = frame.code_object->co_filename;
                
                stream.write(filename_index[filename]);
                stream.write(frame.lineno());
            }
        }

        void write_handle_delete(int delta) {
            stream.write_unsigned_number(SizedTypes::DELETE, delta);
        }

        void write_new_handle(PyObject * obj) {
            stream.write(NewHandle);
            write(obj);
        }

        void write(PyObject * obj) {
                        
            assert(obj);

            if (obj == Py_None) stream.write(FixedSizeTypes::NONE);

            else if (Py_TYPE(obj) == &StreamHandle_Type) write_stream_handle(obj);
            else if (Py_TYPE(obj) == &PyUnicode_Type) stream.write_str(obj);
            else if (Py_TYPE(obj) == &PyLong_Type) stream.write_int(obj);

            else if (Py_TYPE(obj) == &PyBytes_Type) stream.write_bytes(obj);
            else if (Py_TYPE(obj) == &PyBool_Type) stream.write_bool(obj);
            else if (Py_TYPE(obj) == &PyTuple_Type) write_tuple(obj);
            else if (Py_TYPE(obj) == &PyList_Type) write_list(obj);
            else if (Py_TYPE(obj) == &PyDict_Type) write_dict(obj);

            else if (bindings.contains(obj)) stream.write_lookup(bindings[obj]);

            else if (Py_TYPE(obj) == &PyFloat_Type) stream.write_float(obj);

            else if (Py_TYPE(obj) == &PyMemoryView_Type) stream.write_memory_view(obj);

            else write_serialized(obj);
        }

        void bind(PyObject * obj, bool ext) {

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
                stream.write(FixedSizeTypes::EXT_BIND);
                int ref = bindings[(PyObject *)Py_TYPE(obj)];
                stream.write_lookup(ref);

            } else {
                stream.write(Bind);
            }
            // write_magic();
        }

        void object_freed(PyObject * obj) {
            // if (file) {
                auto it = bindings.find(obj);

                // is there an integer binding for this?
                if (it != bindings.end()) {
                    // check_thread();
                    stream.write_unsigned_number(SizedTypes::BINDING_DELETE, it->second);
                    bindings.erase(it);
                }
            // }
        }

        void write_magic() { 
            stream.write((uint64_t)MAGIC);
        }

        void write_thread_switch(PyObject * thread_handle) {
            stream.write(ThreadSwitch);
            write(thread_handle);
        }

        inline size_t get_bytes_written() const { return stream.get_bytes_written(); }

        void change_output(const char * path) {
            stream.change_output(path);
        }

        bool is_closed() { return stream.is_closed(); }

        void close() { stream.close(); }

        void flush() { stream.flush(); }
    };
}