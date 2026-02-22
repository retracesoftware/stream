#include "stream.h"
#include "wireformat.h"
#include <vector>
#include <cstring>
#include <chrono>
#include <cerrno>

#ifndef _WIN32
#include <unistd.h>
#endif

#ifndef PIPE_BUF
#define PIPE_BUF 512
#endif

namespace retracesoftware_stream {

    // Wire format is little-endian.  On LE systems these are identity
    // functions that the compiler will elide entirely.  On BE systems
    // they byte-swap so the on-disk format is always LE.
    inline uint16_t to_le(uint16_t v) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        return __builtin_bswap16(v);
#else
        return v;
#endif
    }
    inline uint32_t to_le(uint32_t v) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        return __builtin_bswap32(v);
#else
        return v;
#endif
    }
    inline uint64_t to_le(uint64_t v) {
#if defined(__BYTE_ORDER__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        return __builtin_bswap64(v);
#else
        return v;
#endif
    }

    int StreamHandle_index(PyObject *);

    using OutputFn = void(*)(void* ctx, const uint8_t* data, size_t size);

    // PID-framed output: writes [pid:4][len:2][payload] frames to an fd.
    // buf_size is clamped to MAX_FRAME so the 2-byte length field is always
    // sufficient and syscall overhead is negligible at 64 KB.
    struct PidFramedOutput {
        static constexpr size_t FRAME_HEADER_SIZE = 6;
        static constexpr size_t MAX_FRAME = 65536;

        int fd;
        size_t buf_size;
        std::vector<uint8_t> frame_buf;

        PidFramedOutput(int fd, size_t buf_size = PIPE_BUF)
            : fd(fd),
              buf_size(std::min(buf_size, MAX_FRAME)),
              frame_buf(this->buf_size) {
            stamp_pid();
        }

        void stamp_pid() {
            uint32_t pid = (uint32_t)::getpid();
            frame_buf[0] = (uint8_t)(pid);
            frame_buf[1] = (uint8_t)(pid >> 8);
            frame_buf[2] = (uint8_t)(pid >> 16);
            frame_buf[3] = (uint8_t)(pid >> 24);
        }

        static void write(void* ctx, const uint8_t* data, size_t size) {
            auto* self = static_cast<PidFramedOutput*>(ctx);
            size_t max_payload = self->buf_size - FRAME_HEADER_SIZE;
            while (size > 0) {
                uint16_t chunk = (uint16_t)std::min(size, max_payload);
                self->frame_buf[4] = (uint8_t)(chunk);
                self->frame_buf[5] = (uint8_t)(chunk >> 8);
                memcpy(self->frame_buf.data() + FRAME_HEADER_SIZE, data, chunk);

                size_t frame_size = FRAME_HEADER_SIZE + chunk;
                while (true) {
                    ssize_t written = ::write(self->fd, self->frame_buf.data(), frame_size);
                    if (written < 0) {
                        if (errno == EINTR) continue;
                        break;
                    }
                    break;
                }
                data += chunk;
                size -= chunk;
            }
        }
    };

    class PrimitiveStream {
        std::vector<uint8_t> buffer;
        size_t bytes_written = 0;
        OutputFn output_fn = nullptr;
        void* output_ctx = nullptr;
        bool verbose = false;
        bool closed = false;

    public:
        PrimitiveStream() = default;

        PrimitiveStream(OutputFn fn, void* ctx) : output_fn(fn), output_ctx(ctx) {
            buffer.reserve(65536);
        }

        ~PrimitiveStream() { close(); }

        inline size_t get_bytes_written() const { return this->bytes_written; }

        void close() {
            if (!closed && output_fn) {
                flush();
            }
            closed = true;
        }

        bool is_closed() const { return closed || !output_fn; }

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
            buffer.insert(buffer.end(), bytes, bytes + size);
            bytes_written += size;
        }

        void write_lookup(int ref) {
            write_unsigned_number(SizedTypes::BINDING, ref);   
        }

        inline void write(uint8_t value) {
            buffer.push_back(value);
            bytes_written++;
        }

        inline void write(int8_t value) {
            write((uint8_t)value);
        }

        inline void write(uint16_t value) {
            value = to_le(value);
            write(reinterpret_cast<const uint8_t*>(&value), (Py_ssize_t)sizeof(value));
        }

        inline void write(int16_t value) {
            write((uint16_t)value);
        }

        inline void write(uint32_t value) {
            value = to_le(value);
            write(reinterpret_cast<const uint8_t*>(&value), (Py_ssize_t)sizeof(value));
        }

        inline void write(int32_t value) {
            write((uint32_t)value);
        }

        inline void write(uint64_t value) {
            value = to_le(value);
            write(reinterpret_cast<const uint8_t*>(&value), (Py_ssize_t)sizeof(value));
        }

        void write(int64_t value) {
            write((uint64_t)value);
        }

        void write(double d) {
            uint64_t bits;
            memcpy(&bits, &d, sizeof(bits));
            write(bits);
        }

        void write_control(Control value) {
            write(value.raw);
        }

        inline void write(Control control) {
            write(control.raw);
        }

        void write_bignum(PyObject * pylong) {

            Py_ssize_t nbits = _PyLong_NumBits(pylong);
            size_t expected = (nbits + 7) / 8;

            uint8_t *bignum = (uint8_t *)malloc(expected);
        
            if (!bignum) {
                PyErr_SetString(PyExc_MemoryError, "bignum malloc failed.");
                throw nullptr;
            }

            int little_endian = 0;
            int is_signed = 1;

            int bytes = _PyLong_AsByteArray(reinterpret_cast<PyLongObject *>(pylong), bignum, expected, little_endian, is_signed);

            if (bytes < 0) {
                free(bignum);
                throw nullptr;
            }
            
            else if ((size_t)bytes > expected) {
                PyErr_SetString(PyExc_RuntimeError,
                    "Unexpected bignum truncation after a size check.");
                free(bignum);
                throw nullptr;
            }

            try {
                write_size(SizedTypes::BIGINT, expected);
                write((const uint8_t *) bignum, expected);
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

        void flush() {
            if (!output_fn || buffer.empty()) return;
            output_fn(output_ctx, buffer.data(), buffer.size());
            buffer.clear();
        }
    };

    static map<PyTypeObject *, freefunc> freefuncs;

    void on_free(void * obj);

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

        // Index for interned strings - allows deduplication using pointer identity
        map<PyObject *, uint16_t> interned_index;
        uint16_t interned_counter = 0;

        static constexpr int MAX_WRITE_DEPTH = 64;
        int write_depth = 0;

        struct DepthGuard {
            int& depth;
            DepthGuard(int& d) : depth(d) { ++depth; }
            ~DepthGuard() { --depth; }
        };

        static PyObject* pickle_dumps() {
            static PyObject* dumps = nullptr;
            if (!dumps) {
                PyObject* mod = PyImport_ImportModule("pickle");
                if (!mod) return nullptr;
                dumps = PyObject_GetAttrString(mod, "dumps");
                Py_DECREF(mod);
            }
            return dumps;
        }

        void pickle_fallback(PyObject * obj) {
            PyObject* dumps = pickle_dumps();
            if (dumps) {
                PyObject* pickled = PyObject_CallOneArg(dumps, obj);
                if (pickled) {
                    stream.write_pickled(pickled);
                    Py_DECREF(pickled);
                    return;
                }
                PyErr_Clear();
            }
            stream.write(FixedSizeTypes::NONE);
        }

        void write_dict(PyObject * obj) {
            assert(PyDict_Check(obj));

            if (write_depth >= MAX_WRITE_DEPTH) {
                pickle_fallback(obj);
                return;
            }
            DepthGuard guard(write_depth);

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
                write(PyTuple_GET_ITEM(obj, i));
            }
        }

        void write_list(PyObject * obj) {
            assert(PyList_Check(obj));

            if (write_depth >= MAX_WRITE_DEPTH) {
                pickle_fallback(obj);
                return;
            }
            DepthGuard guard(write_depth);

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
        
        MessageStream() = default;

        MessageStream(OutputFn fn, void* ctx, PyObject * serializer) :
            stream(fn, ctx),
            serializer(Py_XNewRef(serializer)) {
        }

        ~MessageStream() {
            Py_XDECREF(serializer);
            for (auto& [key, value] : interned_index) {
                Py_DECREF(key);
            }
        }

        void traverse(visitproc visit, void* arg) {
            if (serializer) visit(serializer, arg);
            for (auto& [key, value] : interned_index) {
                visit(key, arg);
            }
        }

        void gc_clear() {
            Py_CLEAR(serializer);
            for (auto& [key, value] : interned_index) {
                Py_DECREF(key);
            }
            interned_index.clear();
        }

        bool is_bound(PyObject * obj) const {
            return bindings.contains(obj);
        }

        void write_control(Control c) {
            stream.write(c);
        }

        void write_handle_delete(int delta) {
            stream.write_unsigned_number(SizedTypes::DELETE, delta);
        }

        void write_handle_ref_by_index(int index) {
            stream.write_handle_ref(index);
        }

        void write_new_handle(PyObject * obj) {
            stream.write(NewHandle);
            write(obj);
        }

        void write_stream_handle(PyObject * obj) {
            assert (Py_TYPE(obj) == &StreamHandle_Type);
            stream.write_handle_ref(StreamHandle_index(obj));
        }

        void write_string(PyObject * obj) {
            assert(PyUnicode_Check(obj));
            
            // Check if string is interned - if so, we can deduplicate using pointer identity
            if (PyUnicode_CHECK_INTERNED(obj)) {
                auto it = interned_index.find(obj);
                if (it != interned_index.end()) {
                    // Already seen this interned string - write reference
                    stream.write_size(SizedTypes::STR_REF, it->second);
                    return;
                }
                // First occurrence of interned string - add to index
                interned_index[Py_NewRef(obj)] = interned_counter;
            }
            // Write full string and increment counter
            // Counter increments for ALL strings so reader indices match
            stream.write_str(obj);
            interned_counter++;
        }

        void write(PyObject * obj) {
                        
            assert(obj);

            if (obj == Py_None) stream.write(FixedSizeTypes::NONE);

            else if (Py_TYPE(obj) == &StreamHandle_Type) write_stream_handle(obj);
            else if (Py_TYPE(obj) == &PyUnicode_Type) write_string(obj);
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
                PyErr_Format(PyExc_RuntimeError, "<%s object at %p> already bound", Py_TYPE(obj)->tp_name, (void *)obj);
                throw nullptr;
            }

            if (!is_patched(Py_TYPE(obj)->tp_free)) {
                patch_free(Py_TYPE(obj));
            }
            bindings[obj] = binding_counter++;

            if (ext) {
                if (!bindings.contains((PyObject *)Py_TYPE(obj))) {
                    PyErr_Format(PyExc_RuntimeError, "to externally bind <%s object at %p>, object type %s must have been bound first", Py_TYPE(obj)->tp_name, (void *)obj, Py_TYPE(obj)->tp_name);
                    throw nullptr;
                }
                stream.write(FixedSizeTypes::EXT_BIND);
                int ref = bindings[(PyObject *)Py_TYPE(obj)];
                stream.write_lookup(ref);

            } else {
                stream.write(Bind);
            }
        }

        bool object_freed(PyObject * obj) {
            auto it = bindings.find(obj);

            // is there an integer binding for this?
            if (it != bindings.end()) {
                stream.write_unsigned_number(SizedTypes::BINDING_DELETE, it->second);
                bindings.erase(it);
                return true;
            }
            return false;
        }

        void write_thread_switch(PyObject * thread_handle) {
            stream.write(ThreadSwitch);
            write(thread_handle);
        }

        inline size_t get_bytes_written() const { return stream.get_bytes_written(); }

        bool is_closed() const { return stream.is_closed(); }

        void close() { stream.close(); }

        void flush() { stream.flush(); }

        void write_pre_pickled(PyObject* bytes_obj) {
            assert(PyBytes_Check(bytes_obj));
            stream.write_pickled(bytes_obj);
        }

        void write_list_header(size_t n) { stream.write_size(SizedTypes::LIST, n); }
        void write_tuple_header(size_t n) { stream.write_tuple_header(n); }
        void write_dict_header(size_t n) { stream.write_dict_header(n); }
    };
}
