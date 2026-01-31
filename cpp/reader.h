#include "stream.h"
#include "wireformat.h"
#include <chrono>
#include <functional>
#include <stdexcept>
#include <utility>
#include <thread>
#include <chrono>
#include <stdexcept>
#include <sstream>

namespace retracesoftware_stream {

    class MessageStream;

    using Stacktrace = std::vector<CodeLocation>;
    using StacktraceHandler = std::function<void (Stacktrace&)>;

    using PyFunc1 = std::function<PyObject * (PyObject *)>;

    using ThreadMap = map<PyObject *, PyThreadState *>;

    class ScopeGuard {
    public:
        // Accept any callable (lambda, function pointer, etc.)
        template<typename Callable>
        explicit ScopeGuard(Callable&& func) 
            : _func(std::forward<Callable>(func)) {}

        // Destructor executes the lambda
        ~ScopeGuard() {
            _func();
        }

        // Prevent copying (RAII should be unique)
        ScopeGuard(const ScopeGuard&) = delete;
        ScopeGuard& operator=(const ScopeGuard&) = delete;

    private:
        std::function<void ()> _func;
    };

    class PrimitiveStream {
        FILE * file;
        size_t bytes_read = 0;
        int read_timeout;
        
    public:
        PrimitiveStream(FILE * file, int read_timeout) : file(file), read_timeout(read_timeout) {}
        ~PrimitiveStream() { close(); }

        void set_file(FILE * f) {
            close();
            file = f;
        }

        void close() {
            if (file) {
                fclose(file);
                file = nullptr;
            }
        }

        // void read_magic() {
        //     uint64_t magic = read<uint64_t>();
        //     if (magic != MAGIC) {
        //         raise(SIGTRAP);
        //         PyErr_Format(PyExc_RuntimeError, "Bad internal exception reading tracefile, next element wasnt MAGIC");
        //         throw nullptr;
        //     }
        // }

        void read(uint8_t * bytes, size_t size) {
            size_t read = fread(bytes, sizeof(uint8_t), size, file);

            if (read < size) {
                std::this_thread::sleep_for(std::chrono::milliseconds(read_timeout));

                read += fread(bytes + read, sizeof(uint8_t), size - read, file);

                if (read < size) {

                    std::stringstream message;
    
                    // 2. Insert all parts of the dynamic message
                    message << "Could not read: " << (size - read) << " bytes from tracefile with timeout: " << read_timeout 
                            << " millseconds";

                    throw std::runtime_error(message.str());
                }
            }
            bytes_read += size;
        }

        PyObject * read_bytes(size_t size) {

            if (size == 0) {
                return PyBytes_FromStringAndSize(NULL, 0);
            } else {
                auto bytes_obj = PyObjectPtr(PyBytes_FromStringAndSize(NULL, size));

                if (!bytes_obj.get()) {
                    PyErr_NoMemory();
                    throw nullptr;
                }

                read((uint8_t *)PyBytes_AS_STRING(bytes_obj.get()), size);

                return Py_NewRef(bytes_obj.get());
            }
        }

        uint64_t read_expected_int() {
            uint8_t i = read<uint8_t>();
            
            return i == 255 ? read<uint64_t>() : (uint64_t)i;
        }

        size_t read_unsigned_number(Control control) {
            switch (control.Sized.size) {
                case ONE_BYTE_SIZE:
                    return (size_t)read<uint8_t>();
                case TWO_BYTE_SIZE:
                    return (size_t)read<uint16_t>();
                case FOUR_BYTE_SIZE:
                    return (size_t)read<uint32_t>();
                case EIGHT_BYTE_SIZE:
                    return (size_t)read<uint64_t>();
                default:
                    return (size_t)(control.Sized.size);
            }
        }

        Control read_control() {
            return Control(read<uint8_t>());
        }

        uint64_t read_uint() {
            Control control = read_control();

            if (control.Sized.type != SizedTypes::UINT) {
                PyErr_Format(PyExc_RuntimeError, "TODO");
                throw nullptr;
            }
            return read_unsigned_number(control);
        }

        PyObject * read_str(size_t size) {

            static thread_local int8_t scratch[1024];

            if (size >= sizeof(scratch)) {
                int8_t * buffer = (int8_t *)malloc(size + 1);

                if (!buffer) {
                    PyErr_Format(PyExc_MemoryError, "Error allocating: %i bytes", size + 1);
                    throw std::exception();
                }

                read((uint8_t *)buffer, size);
                buffer[size] = '\0';

                PyObject * str = PyUnicode_DecodeUTF8((char *)buffer, size, "strict");

                // PyObject * str = PyUnicode_FromString((char *)buffer);

                free(buffer);
                assert(str);

                return str;

            } else {
            
                read((uint8_t *)scratch, size);
                scratch[size] = '\0';

                PyObject * decoded = PyUnicode_DecodeUTF8((char *)scratch, size, "strict");

                if (!decoded) {
                    raise(SIGTRAP);
                    assert (PyErr_Occurred());
                    throw std::exception();
                }

                return decoded;
                // return PyUnicode_DecodeUTF8((char *)scratch, size, "strict");
                // return PyUnicode_FromString((char *)scratch);
            }
        }

        template<typename T>
        T read() {
            if constexpr (std::is_same_v<T, int8_t>) {
                return (int8_t)read<uint8_t>();
            }
            else if constexpr (std::is_same_v<T, uint8_t>) {
                uint8_t byte;
                read(&byte, 1);
                return byte;
            }
            else if constexpr (std::is_same_v<T, int16_t>) {
                return (int16_t)read<uint16_t>();
            }
            else if constexpr (std::is_same_v<T, uint16_t>) {
                uint8_t buffer[sizeof(uint16_t)];
                read(buffer, sizeof(buffer));

                uint16_t value = (uint16_t)buffer[0];
                value |= ((uint16_t)buffer[1] << 8);
                return value;
            }
            else if constexpr (std::is_same_v<T, int32_t>) {
                return (int16_t)read<uint32_t>();
            }
            else if constexpr (std::is_same_v<T, uint32_t>) {
                uint8_t buffer[sizeof(uint32_t)];
                read(buffer, sizeof(buffer));

                uint32_t value = (uint32_t)buffer[0];
                value |= ((uint32_t)buffer[1] << 8);
                value |= ((uint32_t)buffer[2] << 16);
                value |= ((uint32_t)buffer[3] << 24);
                return value;
            }
            else if constexpr (std::is_same_v<T, int64_t>) {
                return (int64_t)read<uint64_t>();
            }
            else if constexpr (std::is_same_v<T, uint64_t>) {
                uint8_t buffer[sizeof(uint64_t)];
                read(buffer, sizeof(buffer));

                uint64_t value = (uint64_t)buffer[0];
                value |= ((uint64_t)buffer[1] << 8);
                value |= ((uint64_t)buffer[2] << 16);
                value |= ((uint64_t)buffer[3] << 24);
                value |= ((uint64_t)buffer[4] << 32);
                value |= ((uint64_t)buffer[5] << 40);
                value |= ((uint64_t)buffer[6] << 48);
                value |= ((uint64_t)buffer[7] << 56);
                return value;
            }

            else if constexpr (std::is_same_v<T, double>) {
                uint64_t raw = read<uint64_t>();
                return *(double *)&raw;
            }
            else {
                static_assert(std::is_void_v<T>, "Unsupported type in read<T>()");
                // static_assert(false, "read<T>() not specialized for this type");
            }
        }
    };

    class IndexedLookup {
        map<int, PyObject *> lookup;
        int next_index = 0;
    public:
    
        IndexedLookup() {}

        IndexedLookup(const IndexedLookup&) = delete;
        IndexedLookup& operator=(const IndexedLookup&) = delete;

        PyObject * operator[](size_t index) const {
            return lookup.at(index);
        }

        void add(PyObject * element) {
            lookup[next_index++] = Py_NewRef(element);
        }
        
        void remove(int index) {
            assert(index >= 0 && index < next_index);
            assert(lookup.contains(index));

            Py_DECREF(lookup.at(index));
            lookup.erase(index);
        }

        void remove_from_end(int from_end) {
            assert (from_end >= 0);
            int index = next_index - 1 - from_end;
            assert (index >= 0);
            Py_DECREF(lookup.at(index));
            lookup.erase(index);
        }

        ~IndexedLookup() {
            PyGILState_STATE gstate = PyGILState_Ensure();
            for (const auto& [key, value] : lookup) {
                Py_DECREF(value);
            }
            PyGILState_Release(gstate);
        }        
    };


    class MessageStream {
        PrimitiveStream stream;
        std::function<void (PyObject *, const Stacktrace&)> on_stack;
        PyFunc1 transform;
        PyFunc1 deserializer;
        std::function<void (ThreadMap &)> on_timeout;
        bool magic_markers = false;

        Control loaded_control = ThreadSwitch;

        int timeout = 0;
        bool verbose = false;

        PyObject * active_thread = nullptr;
        IndexedLookup lookup;
        IndexedLookup index2filename;
        IndexedLookup bindings;
        std::condition_variable waiting;
        map<PyObject *, Stacktrace> stacks;
        std::mutex mtx;
        map<PyObject *, PyThreadState *> pending;
        size_t message_counter = 0;
        
        PyObject * read_pickled(size_t size) {
            PyObject * bytes = stream.read_bytes(size);

            PyObject * deserialized = deserializer(bytes);

            Py_DECREF(bytes);

            return deserialized;
        }

        PyObject * read() { return read(stream.read_control()); }

        PyObject * read(Control control) {
            return control.Sized.type == SizedTypes::FIXED_SIZE
                ? read_fixedsize(control.Fixed.type)
                : read_sized(control);
        }

        PyObject * store_handle() {
            assert(!PyGILState_Check());

            retracesoftware::GILGuard guard;

            PyObject * ref = read();

            PyObject * new_ref = transform(ref);

            Py_DECREF(ref);

            lookup.add(new_ref);
            return new_ref;
        }

        PyObject * read_list(size_t size) {
            auto list = PyObjectPtr(PyList_New(size));

            if (!list.get()) { throw nullptr; }

            for (size_t i = 0; i < size; i++) {
                PyList_SetItem(list.get(), i, read());
            }
            return Py_NewRef(list.get());
        }

        PyObject * read_tuple(size_t size) {
            assert (!PyErr_Occurred());

            auto tuple = PyObjectPtr(PyTuple_New(size));

            if (!tuple.get()) {
                throw std::exception();
            }

            for (size_t i = 0; i < size; i++) {
                assert (!PyErr_Occurred());

                PyObject * item = read();

                assert(item);

                if (PyTuple_SetItem(tuple.get(), i, item) == -1) {
                    throw std::exception();
                }
            }
            return Py_NewRef(tuple.get());
        }

        PyObject * read_dict(size_t size) {

            auto dict = PyObjectPtr(PyDict_New());

            if (!dict.get()) {
                throw std::exception();
            }

            while (size--) {
                auto key = PyObjectPtr(read());

                if (!key.get()) {
                    assert(PyErr_Occurred());
                    throw std::exception();
                }

                auto value = PyObjectPtr(read());

                if (!value.get()) {
                    assert(PyErr_Occurred());
                    throw std::exception();
                }

                if (PyDict_SetItem(dict.get(), key.get(), value.get()) == -1) {
                    assert(PyErr_Occurred());
                    throw std::exception();
                }
            }
            return Py_NewRef(dict.get());
        }

        void read_ext_bind() {
            if (verbose) printf("consumed EXT_BIND\n");

            PyTypeObject * cls = (PyTypeObject *)read();
            
            if (!PyType_Check((PyObject *)cls)) {
                PyErr_Format(PyExc_TypeError, "Expected next item read to be a type but was: %S", cls);
                throw nullptr;
            }
            PyObject * empty = PyTuple_New(0);

            PyObject * instance = cls->tp_new(cls, empty, nullptr);

            Py_DECREF(empty);
            Py_DECREF(cls);

            // PyObject * instance = PyObject_CallNoArgs(cls);
            if (!instance) {
                throw nullptr;
            }
            bindings.add(instance);
            check_magic();
        }

        PyObject * read_fixedsize(FixedSizeTypes type) {
            assert (!PyErr_Occurred());

            if (verbose) printf("%s ", FixedSizeTypes_Name(type));

            switch (type) {
                case FixedSizeTypes::NONE: 
                    return Py_NewRef(Py_None);
                case FixedSizeTypes::TRUE:
                    return Py_NewRef(Py_True);
                case FixedSizeTypes::FALSE: 
                    return Py_NewRef(Py_False);
                case FixedSizeTypes::NEG1: 
                    return PyLong_FromLong(-1);
                // case FixedSizeTypes::BIND: return Py_NewRef(bind_singleton);
                case FixedSizeTypes::FLOAT:
                    return PyFloat_FromDouble(stream.read<double>());
                case FixedSizeTypes::INT64:
                    return PyLong_FromLongLong(stream.read<int64_t>());
                // case FixedSizeTypes::INLINE_NEW_HANDLE: return Py_NewRef(store_handle());

                case FixedSizeTypes::EXT_BIND:
                    read_ext_bind();
                    return read();

                default:
                    raise(SIGTRAP);

                    const char * name = FixedSizeTypes_Name(static_cast<FixedSizeTypes>(type));

                    if (name) {
                        PyErr_Format(PyExc_RuntimeError, "unhandled subtype: %s for FixedSized", name);
                    } else {
                        PyErr_Format(PyExc_RuntimeError, "Unknown subtype: %i for FixedSized", type);
                    }
                    assert(PyErr_Occurred());
                    throw nullptr;
            };
        }

        // void read_binding_delete(size_t size) {
        //     if (verbose) printf("consumed BINDING_DELETE\n");

        //     int64_t index = size;
        //     auto it = bindings.find(index);

        //     if (it != bindings.end()) {
        //         Py_DECREF(it->second);
        //         bindings.erase(it);
        //     }
        // }

        PyObject * read_sized(Control control) {

            size_t size = stream.read_unsigned_number(control);

            switch (control.Sized.type) {
                case SizedTypes::UINT: 
                    return PyLong_FromLongLong(size);
                case SizedTypes::HANDLE: 
                    return Py_NewRef(lookup[size]);
                case SizedTypes::BINDING: 
                    return Py_NewRef(bindings[size]);
                case SizedTypes::BYTES: return stream.read_bytes(size);
                case SizedTypes::LIST: return read_list(size);
                case SizedTypes::DICT: return read_dict(size);
                case SizedTypes::TUPLE: return read_tuple(size);
                case SizedTypes::STR: return stream.read_str(size);
                case SizedTypes::PICKLED: return read_pickled(size);

                default:
                    PyErr_Format(PyExc_RuntimeError, "unknown sized type: %i", control.Sized.type);
                    throw nullptr;
            } 
        }

        bool is_current_thread(PyObject * thread) {
            assert(!PyGILState_Check());
            retracesoftware::GILGuard guard;
            switch (PyObject_RichCompareBool(active_thread, thread, Py_EQ)) {
                case 1: return true;
                case 0: return false;
                default: throw nullptr;
            }
        }

        PyThreadState * threadstate() {
            assert(!PyGILState_Check());
            retracesoftware::GILGuard guard;
            return PyThreadState_Get();
        }

        bool safe_wait(std::unique_lock<std::mutex> &lock, PyObject * thread) {
            assert(thread);
            assert(!PyGILState_Check());
            // {
            //     PyObject * s = PyObject_Str(thread);
            //     printf("In safe wait: %s\n", PyUnicode_AsUTF8(s));
            //     Py_DECREF(s);
            // }
            // retracesoftware::GILReleaseGuard guard;
            return waiting.wait_for(lock, std::chrono::milliseconds(timeout), [this, thread]() { return is_current_thread(thread); });
        }

        Control next_control(std::unique_lock<std::mutex> &lock, PyObject * thread) {
            assert(!PyGILState_Check());
            if (!active_thread) active_thread = Py_NewRef(thread);

            // Q1 - ok are we the correct thread?
            if (loaded_control == Empty) {
                loaded_control = stream.read_control();

                if (loaded_control == ThreadSwitch) {
                    Py_XDECREF(active_thread);
                    active_thread = Py_NewRef(safe_read());
                    loaded_control = Empty;
                }
                if (pending.size() > 0) {
                    waiting.notify_all();
                }
                return next_control(lock, thread);

            } else if (is_current_thread(thread)) {
                Control control = loaded_control;
                loaded_control = Empty;
                return control;
            } else {
                ScopeGuard guard([this, thread] { pending.erase(thread); });

                pending[thread] = threadstate();
                
                if (safe_wait(lock, thread)) {
                    return consume(lock, thread);
                } else {
                    on_timeout(pending);
                    throw nullptr;
                }
            }
        }

        Stacktrace read_stack(PyObject * thread) {
            std::vector<CodeLocation> &stack = stacks[thread];

            size_t to_drop = stream.read_expected_int();

            assert(stack.size() >= to_drop);
            while (to_drop--) stack.pop_back();

            uint64_t size = stream.read_expected_int();
            while (size--) {
                PyObject * filename = index2filename[(int)stream.read<uint16_t>()];
                uint16_t lineno = stream.read<uint16_t>();
                stack.push_back(CodeLocation(filename, lineno));
            }
            return stack;
        }

        void check_magic() {
            if (magic_markers) {
                uint64_t marker = stream.read<uint64_t>();
                if (marker != MAGIC) {
                    raise(SIGTRAP);
                }
            }
        }

        PyObject * safe_read() {
            assert(!PyGILState_Check());
            retracesoftware::GILGuard acquire;
            return read();
        }

        void handle_stack(PyObject * thread) {
            retracesoftware::GILGuard guard;

            Stacktrace record = read_stack(thread);
            check_magic();
            on_stack(thread, record);
        }

        void handle_binding_delete(Control control) {
            retracesoftware::GILGuard guard;

            size_t size = stream.read_unsigned_number(control);
            bindings.remove(size);
        }

        Control consume(std::unique_lock<std::mutex> &lock, PyObject * thread) {
            assert(!PyGILState_Check());

            Control control = next_control(lock, thread);

            message_counter++;

            if (control == Stack) {
                handle_stack(thread);
                return consume(lock, thread);
            }
            else if (control == NewHandle) {
                // if (verbose) printf("consumed NEW_HANDLE\n");
                store_handle();
                return consume(lock, thread);
            }
            else if (control == AddFilename) {
                index2filename.add(safe_read());
                return consume(lock, thread);
            }
            else if (is_binding_delete(control)) {
                handle_binding_delete(control);
                return consume(lock, thread);
            }
            else if (is_delete(control)) {
                size_t size = stream.read_unsigned_number(control);
                bindings.remove_from_end(size);
                // bindings.remove_from_end(size);
                return consume(lock, thread);
            } else {
                return control;
            }
        }

        PyObject * read_root(Control control) {
            assert(!PyGILState_Check());

            retracesoftware::GILGuard acquire;
            PyObject * result = read(control);
            check_magic();
            return result;
        }

        public:
        // bool magic_markers;
        // FILE * file;
        // size_t bytes_read;
        // std::mutex mtx;
        // Control loaded_control;
        // int num_waiting;
        // int timeout;
        // PyObject * active_thread;
        // std::condition_variable waiting;

        MessageStream(
            FILE * file,
            int read_timeout,
            std::function<void (PyObject *, const Stacktrace&)> on_stack,
            PyFunc1 transform,
            PyFunc1 deserializer,
            std::function<void (ThreadMap&)> on_timeout,
            bool magic_markers) :
            stream(file, read_timeout),
            on_stack(on_stack),
            transform(transform),
            deserializer(deserializer),
            on_timeout(on_timeout),
            magic_markers(magic_markers) {}
            
        ~MessageStream() {
            Py_DECREF(active_thread);
        }

        
        PyObject * next(PyObject * thread) {

            // check we hold the GIL
            assert (PyGILState_Check());
            
            retracesoftware::GILReleaseGuard release;
            std::unique_lock<std::mutex> lock(mtx);
            
            Control control = consume(lock, thread);

            if (control == Bind) {
                throw std::runtime_error("expected result but got BIND");
            }
            // retracesoftware::GILGuard guard;
            return read_root(control);
        }

        void set_file(FILE * file) {
            stream.set_file(file);
        }

        void set_timeout(int timeout) {
            this->timeout = timeout;
        }

        void close() {
            stream.close();
        }

        void bind(PyObject * thread, PyObject * binding, std::function<void (PyObject *)> async_consumer) {

            // check we hold the GIL
            assert (PyGILState_Check());
            
            retracesoftware::GILReleaseGuard release;

            std::unique_lock<std::mutex> lock(mtx);

            Control control = consume(lock, thread);

            while (control != Bind) {
                PyObject * next = read_root(control);
                check_magic();

                try {
                    {
                        retracesoftware::GILGuard acquire;
                        async_consumer(next);
                        Py_DECREF(next);
                    }
                    control = consume(lock, thread);
                } catch (...) {
                    Py_DECREF(next);
                    throw;
                }
                // if (async_consumer(next)) {
                //     control = consume(lock, thread);
                // } else {

                //     retracesoftware::GILGuard guard;

                //     PyObject * s = PyObject_Str(unexpected);

                //     printf("expected BIND but got: %s\n", PyUnicode_AsUTF8(s));
                // }

            }
            bindings.add(binding);
            check_magic();
        }
    };
}