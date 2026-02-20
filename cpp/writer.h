#include "stream.h"
#include "wireformat.h"
#include "bufferslot.h"
#include <vector>
#include <cstring>
#include <chrono>

#if defined(__GNUC__) || defined(__clang__)
#define STREAM_LIKELY(x)   __builtin_expect(!!(x), 1)
#else
#define STREAM_LIKELY(x)   (x)
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

    class PrimitiveStream {
        static constexpr size_t BUFFER_SIZE = BUFFER_SLOT_SIZE;

        BufferSlot* slots[2] = {nullptr, nullptr};
        int active = 0;
        size_t write_pos = 0;
        size_t bytes_written = 0;
        PyObject* output_callback = nullptr;
        bool verbose = false;

        size_t message_boundary = 0;
        std::vector<uint8_t> overflow;
        std::vector<uint8_t>* large_write_buffer = nullptr;

    public:
        uint64_t dropped_messages = 0;
        // -1 = wait forever, 0 = drop immediately, >0 = wait up to N ns then drop
        int64_t backpressure_timeout_ns = -1;

        PrimitiveStream() = default;

        PrimitiveStream(PyObject* output_callback) :
            output_callback(Py_XNewRef(output_callback)) {

            slots[0] = (BufferSlot*)BufferSlot_Type.tp_alloc(&BufferSlot_Type, 0);
            slots[1] = (BufferSlot*)BufferSlot_Type.tp_alloc(&BufferSlot_Type, 0);

            if (slots[0]) {
                slots[0]->in_use.store(false, std::memory_order_relaxed);
                slots[0]->used = 0;
                slots[0]->message_count = 0;
            }
            if (slots[1]) {
                slots[1]->in_use.store(false, std::memory_order_relaxed);
                slots[1]->used = 0;
                slots[1]->message_count = 0;
            }
            if (!slots[0] || !slots[1]) {
                Py_XDECREF(slots[0]);
                Py_XDECREF(slots[1]);
                slots[0] = slots[1] = nullptr;
                Py_XDECREF(output_callback);
                this->output_callback = nullptr;
                throw nullptr;
            }
        }

        ~PrimitiveStream() { close(); }

        inline size_t get_bytes_written() const { return this->bytes_written; }

        void traverse(visitproc visit, void* arg) {
            if (output_callback) visit(output_callback, arg);
            if (slots[0]) visit((PyObject*)slots[0], arg);
            if (slots[1]) visit((PyObject*)slots[1], arg);
        }

        void gc_clear() {
            Py_CLEAR(output_callback);
            if (slots[0]) { Py_DECREF(slots[0]); slots[0] = nullptr; }
            if (slots[1]) { Py_DECREF(slots[1]); slots[1] = nullptr; }
        }

        void close() {
            if (output_callback) {
                flush();
                Py_CLEAR(output_callback);
            }
            if (slots[0]) { Py_DECREF(slots[0]); slots[0] = nullptr; }
            if (slots[1]) { Py_DECREF(slots[1]); slots[1] = nullptr; }
        }

        // Returns true if the slot became available, false if timeout expired (should drop).
        // Caller must hold the GIL on entry; GIL is released during the wait.
        inline bool wait_for_slot(int slot_idx) {
            if (backpressure_timeout_ns == 0) return false;
            if (backpressure_timeout_ns < 0) {
                Py_BEGIN_ALLOW_THREADS
                while (slots[slot_idx]->in_use.load(std::memory_order_acquire)) {}
                Py_END_ALLOW_THREADS
                return true;
            }
            auto deadline = std::chrono::steady_clock::now()
                          + std::chrono::nanoseconds(backpressure_timeout_ns);
            Py_BEGIN_ALLOW_THREADS
            while (slots[slot_idx]->in_use.load(std::memory_order_acquire)) {
                if (std::chrono::steady_clock::now() >= deadline) break;
            }
            Py_END_ALLOW_THREADS
            return !slots[slot_idx]->in_use.load(std::memory_order_acquire);
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

            if (large_write_buffer) {
                large_write_buffer->insert(large_write_buffer->end(), bytes, bytes + size);
                bytes_written += size;
                return;
            }

            assert(output_callback);

            if (write_pos + (size_t)size > BUFFER_SIZE) {
                if (message_boundary > 0) {
                    size_t tail = write_pos - message_boundary;

                    int full = active;
                    slots[full]->used = message_boundary;

                    active = 1 - active;
                    if (slots[active]->in_use.load(std::memory_order_acquire)) {
                        if (!wait_for_slot(active)) {
                            dropped_messages += slots[full]->message_count;
                            memmove(slots[full]->data,
                                    slots[full]->data + message_boundary, tail);
                            write_pos = tail;
                            message_boundary = 0;
                            slots[full]->message_count = 0;
                            active = full;
                            goto do_write;
                        }
                    }

                    memcpy(slots[active]->data, slots[full]->data + message_boundary, tail);
                    write_pos = tail;
                    message_boundary = 0;
                    slots[full]->message_count = 0;

                    PyObject* mv = PyMemoryView_FromObject((PyObject*)slots[full]);
                    if (!mv) throw nullptr;
                    PyObject* result = PyObject_CallOneArg(output_callback, mv);
                    Py_DECREF(mv);
                    if (!result) throw nullptr;
                    Py_DECREF(result);

                } else {
                    overflow.assign(slots[active]->data,
                                    slots[active]->data + write_pos);
                    large_write_buffer = &overflow;
                    write_pos = 0;

                    large_write_buffer->insert(large_write_buffer->end(),
                                               bytes, bytes + size);
                    bytes_written += size;
                    return;
                }
            }

        do_write:
            if (write_pos + (size_t)size > BUFFER_SIZE) {
                write(bytes, size);
                return;
            }

            memcpy(slots[active]->data + write_pos, bytes, size);
            write_pos += size;
            bytes_written += size;
        }

        void write_lookup(int ref) {
            write_unsigned_number(SizedTypes::BINDING, ref);   
        }

        inline void write(uint8_t value) {
            if (STREAM_LIKELY(!large_write_buffer && write_pos < BUFFER_SIZE)) {
                slots[active]->data[write_pos++] = value;
                bytes_written++;
                return;
            }
            write(&value, sizeof(value));
        }

        inline void write(int8_t value) {
            write((uint8_t)value);
        }

        inline void write(uint16_t value) {
            value = to_le(value);
            if (STREAM_LIKELY(!large_write_buffer && write_pos + sizeof(value) <= BUFFER_SIZE)) {
                memcpy(slots[active]->data + write_pos, &value, sizeof(value));
                write_pos += sizeof(value);
                bytes_written += sizeof(value);
                return;
            }
            write(reinterpret_cast<const uint8_t*>(&value), (Py_ssize_t)sizeof(value));
        }

        inline void write(int16_t value) {
            write((uint16_t)value);
        }

        inline void write(uint32_t value) {
            value = to_le(value);
            if (STREAM_LIKELY(!large_write_buffer && write_pos + sizeof(value) <= BUFFER_SIZE)) {
                memcpy(slots[active]->data + write_pos, &value, sizeof(value));
                write_pos += sizeof(value);
                bytes_written += sizeof(value);
                return;
            }
            write(reinterpret_cast<const uint8_t*>(&value), (Py_ssize_t)sizeof(value));
        }

        inline void write(int32_t value) {
            write((uint32_t)value);
        }

        inline void write(uint64_t value) {
            value = to_le(value);
            if (STREAM_LIKELY(!large_write_buffer && write_pos + sizeof(value) <= BUFFER_SIZE)) {
                memcpy(slots[active]->data + write_pos, &value, sizeof(value));
                write_pos += sizeof(value);
                bytes_written += sizeof(value);
                return;
            }
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

            if (STREAM_LIKELY(!verbose && size <= 11 && !large_write_buffer
                              && write_pos + 1 + (size_t)size <= BUFFER_SIZE)) {
                Control control;
                control.Sized.type = SizedTypes::STR;
                control.Sized.size = (Sizes)size;
                uint8_t* dst = slots[active]->data + write_pos;
                dst[0] = control.raw;
                memcpy(dst + 1, utf8, size);
                write_pos += 1 + size;
                bytes_written += 1 + size;
                return;
            }

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

        bool is_closed() const { return output_callback == nullptr; }

        void mark_message_boundary() {
            if (large_write_buffer) {
                PyObject* bytes_obj = PyBytes_FromStringAndSize(
                    (const char*)large_write_buffer->data(),
                    large_write_buffer->size());
                large_write_buffer->clear();
                large_write_buffer = nullptr;
                if (!bytes_obj) throw nullptr;

                PyObject* result = PyObject_CallOneArg(output_callback, bytes_obj);
                Py_DECREF(bytes_obj);
                if (!result) throw nullptr;
                Py_DECREF(result);
            }
            message_boundary = write_pos;
            if (slots[active]) slots[active]->message_count++;
        }

        void flush() {
            if (write_pos == 0 || !output_callback) return;

            int full = active;
            slots[full]->used = write_pos;

            active = 1 - active;
            if (slots[active]->in_use.load(std::memory_order_acquire)) {
                if (!wait_for_slot(active)) {
                    dropped_messages += slots[full]->message_count;
                    write_pos = 0;
                    message_boundary = 0;
                    slots[full]->message_count = 0;
                    active = full;
                    return;
                }
            }
            write_pos = 0;
            message_boundary = 0;
            slots[full]->message_count = 0;

            PyObject* mv = PyMemoryView_FromObject((PyObject*)slots[full]);
            if (!mv) throw nullptr;

            PyObject* result = PyObject_CallOneArg(output_callback, mv);
            Py_DECREF(mv);

            if (!result) throw nullptr;
            Py_DECREF(result);
        }

        PyObject* get_output_callback() const { return output_callback; }

        void set_output_callback(PyObject* cb) {
            Py_XDECREF(output_callback);
            output_callback = Py_XNewRef(cb);
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

        MessageStream(
            PyObject * output_callback,
            PyObject * serializer) : 
            stream(output_callback), 
            serializer(Py_XNewRef(serializer)) {
        }

        ~MessageStream() {
            Py_XDECREF(serializer);
            for (auto& [key, value] : interned_index) {
                Py_DECREF(key);
            }
        }

        void traverse(visitproc visit, void* arg) {
            stream.traverse(visit, arg);
            if (serializer) visit(serializer, arg);
            for (auto& [key, value] : interned_index) {
                visit(key, arg);
            }
        }

        void gc_clear() {
            stream.gc_clear();
            Py_CLEAR(serializer);
            for (auto& [key, value] : interned_index) {
                Py_DECREF(key);
            }
            interned_index.clear();
        }

        bool is_bound(PyObject * obj) const {
            return bindings.contains(obj);
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

        PyObject* get_output_callback() const { return stream.get_output_callback(); }

        void set_output_callback(PyObject* cb) { stream.set_output_callback(cb); }

        void mark_message_boundary() { stream.mark_message_boundary(); }

        uint64_t get_dropped_messages() const { return stream.dropped_messages; }
        void reset_dropped_messages() { stream.dropped_messages = 0; }

        int64_t get_backpressure_timeout_ns() const { return stream.backpressure_timeout_ns; }
        void set_backpressure_timeout_ns(int64_t ns) { stream.backpressure_timeout_ns = ns; }

        void write_dropped_marker(uint64_t count) {
            stream.write(Dropped);
            stream.write_sized_int(count);
        }

        void write_pre_pickled(PyObject* bytes_obj) {
            assert(PyBytes_Check(bytes_obj));
            stream.write_pickled(bytes_obj);
        }
    };
}
