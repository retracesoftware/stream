#include "stream.h"
#include <structmember.h>
#include "wireformat.h"
#include <algorithm>
#include <fcntl.h>
#include <sstream>
#include <sys/file.h>
#include <condition_variable>
#include <mutex>
#include <ranges>

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

    template <typename T>
    class BlockingContainer {
    public:
        // Initialize with the target size (N) that triggers the unblock.
        BlockingContainer(size_t target_size) : target_size_(target_size) {}

        // Producer Method: Adds an element.
        void add(const T& item) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                
                // 1. Add the element
                container_.push_back(item); 
                
                // 2. Check the condition
                if (container_.size() >= target_size_) {
                    // 3. Notify the thread waiting for the container to be full
                    condition_.notify_all();
                }
            }
            // Lock is released here
        }

        // Blocking Method: Blocks until N elements have been added.
        void wait_until_full() {
            std::unique_lock<std::mutex> lock(mutex_);
            
            // Wait until the container's size reaches the target size.
            condition_.wait(lock, [this] { 
                return container_.size() >= target_size_; 
            });
            
            // Lock is automatically released upon exiting the function.
        }

        // Accessor (must be thread-safe for reading)
        std::vector<T> get_elements() {
            std::unique_lock<std::mutex> lock(mutex_);
            return container_;
        }

    private:
        std::mutex mutex_;
        std::condition_variable condition_;
        std::vector<T> container_; // The shared data structure
        const size_t target_size_;  // The target size (N)
    };

    const char * SizedTypes_Name(enum SizedTypes root) {
        switch (root) {
            case SizedTypes::BYTES: return "BYTES";
            case SizedTypes::LIST: return "LIST";
            case SizedTypes::DICT: return "DICT";
            case SizedTypes::TUPLE: return "TUPLE";

            case SizedTypes::STR: return "STR";
            case SizedTypes::PICKLED: return "PICKLED";
            case SizedTypes::UINT: return "UINT";
            case SizedTypes::DELETE: return "DELETE";
            
            case SizedTypes::HANDLE: return "HANDLE";
            case SizedTypes::BIGINT: return "BIGINT";
            case SizedTypes::SET: return "SET";
            case SizedTypes::FROZENSET: return "FROZENSET";

            case SizedTypes::BINDING: return "BINDING";
            case SizedTypes::BINDING_DELETE: return "BINDING_DELETE";
            case SizedTypes::FIXED_SIZE: return "FIXED_SIZE";

            default: return nullptr;
        }
    }

    const char * FixedSizeTypes_Name(enum FixedSizeTypes root) {
        switch (root) {
            case FixedSizeTypes::NONE: return "NONE";
            // case FixedSizeTypes::C_NULL: return "C_NULL";
            case FixedSizeTypes::TRUE: return "TRUE";
            case FixedSizeTypes::FALSE: return "FALSE";
            case FixedSizeTypes::NEW_HANDLE: return "NEW_HANDLE";
            case FixedSizeTypes::REF: return "REF";
            case FixedSizeTypes::NEG1: return "NEG1";
            case FixedSizeTypes::INT64: return "INT64";
            case FixedSizeTypes::BIND: return "BIND";
            case FixedSizeTypes::EXT_BIND: return "EXT_BIND";
            case FixedSizeTypes::STACK: return "STACK";
            case FixedSizeTypes::ADD_FILENAME: return "ADD_FILENAME";

            default: return nullptr;
        }
    }

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
    
    static bool equal(PyObject * a, PyObject * b) {
        retracesoftware::GILGuard guard;

        switch (PyObject_RichCompareBool(a, b, Py_EQ)) {
            case 0: return false;
            case 1: return true;
            default:
                assert(PyErr_Occurred());
                throw nullptr;
        };
    }

    struct ObjectReader : public ReaderWriterBase {
        
        FILE * file;
        size_t bytes_read;
        size_t messages_read;
        int next_handle;
        // PyTypeObject * enumtype;
        std::mutex mtx;
        std::condition_variable wakeup;
        int next_control;
        map<int, PyObject *> bindings;
        map<uint16_t, PyObject *> index2filename;
        PyObject * bind_singleton;
        int binding_counter;
        PyObject * on_stack_difference;
        int pid;

        map<PyCodeObject *, Py_hash_t> obj_to_hash;

        map<uint64_t, PyObject *> lookup;
        map<PyObject *, PyObject *> normalized_paths;

        // map<PyTypeObject *, PyObject *> type_deserializers;

        PyObject * deserializer;
        PyObject * transform;
        PyObject * thread;
        PyObject * active_thread;
        // vectorcallfunc vectorcall;

        PyObject * pending_reads;
        PyObject * stacktraces;

        PyObject * next;
        bool verbose;
        // map<PyObject *, PyObject *> pending_reads;
        
        void read(uint8_t * bytes, Py_ssize_t size) {
            fread(bytes, sizeof(uint8_t), size, file);
            bytes_read += size;
        }

        template<typename T>
        T _read() {
            if constexpr (std::is_same_v<T, int8_t>) {
                return (int8_t)_read<uint8_t>();
            }
            else if constexpr (std::is_same_v<T, uint8_t>) {
                uint8_t byte;
                read(&byte, 1);
                return byte;
            }
            else if constexpr (std::is_same_v<T, int16_t>) {
                return (int16_t)_read<uint16_t>();
            }
            else if constexpr (std::is_same_v<T, uint16_t>) {
                uint8_t buffer[sizeof(uint16_t)];
                read(buffer, sizeof(buffer));

                uint16_t value = (uint16_t)buffer[0];
                value |= ((uint16_t)buffer[1] << 8);
                return value;
            }
            else if constexpr (std::is_same_v<T, int32_t>) {
                return (int16_t)_read<uint32_t>();
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
                return (int64_t)_read<uint64_t>();
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
                uint64_t raw = _read<uint64_t>();
                return *(double *)&raw;
            }
            else {
                static_assert(std::is_void_v<T>, "Unsupported type in read<T>()");
                // static_assert(false, "read<T>() not specialized for this type");
            }
        }

        PyObject * read_handle(int64_t index) {
            assert(index < next_handle);
            assert(lookup.contains(index));

            return Py_NewRef(lookup[index]);
        }

        PyObject * read_binding(int64_t index) {

            assert(bindings.contains(index));
            return Py_NewRef(bindings[index]);
            // assert(index < next_handle);
            // assert(lookup.contains(index));
            // return Py_NewRef(lookup[index]);
        }

        size_t read_unsigned_number(Control control) {

            switch (control.Sized.size) {
                case ONE_BYTE_SIZE:
                    return (size_t)_read<uint8_t>();
                case TWO_BYTE_SIZE:
                    return (size_t)_read<uint16_t>();
                case FOUR_BYTE_SIZE:
                    return (size_t)_read<uint32_t>();
                case EIGHT_BYTE_SIZE:
                    return (size_t)_read<uint64_t>();
                default:
                    return (size_t)(control.Sized.size);
            }
        }

        PyObject * read_bytes(size_t size) {

            auto bytes_obj = PyObjectPtr(PyBytes_FromStringAndSize(NULL, size));

            if (!bytes_obj.get()) {
                PyErr_NoMemory();
                throw nullptr;
            }

            read((uint8_t *)PyBytes_AS_STRING(bytes_obj.get()), size);

            return Py_NewRef(bytes_obj.get());
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

        PyObject * read_pickled(size_t size) {
            PyObject * bytes = read_bytes(size);

            PyObject * deserialized = PyObject_CallOneArg(deserializer, bytes);

            Py_DECREF(bytes);

            if (!deserialized) {
                // PyErr_Print();
                throw nullptr;
            }
            return deserialized;
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

        void read_delete(size_t size) {
            if (verbose) printf("consumed DELETE for index: %lu, next handle: %lu\n", size, next_handle);

            int64_t from_end = size + 1;
            assert(from_end <= next_handle);
            uint64_t offset = next_handle - from_end;
            
            assert(lookup.contains(offset));
            assert(lookup[offset]);

            Py_DECREF(lookup[offset]);
            lookup.erase(offset);
        }

        void read_binding_delete(size_t size) {
            if (verbose) printf("consumed BINDING_DELETE\n");

            int64_t index = size;
            auto it = bindings.find(index);

            if (it != bindings.end()) {
                Py_DECREF(it->second);
                bindings.erase(it);
            }
        }

        uint64_t read_uint() {
            Control control = read_control();

            if (control.Sized.type != SizedTypes::UINT) {
                PyErr_Format(PyExc_RuntimeError, "TODO");
                throw nullptr;
            }
            return read_unsigned_number(control);
        }

        PyObject * read_sized(Control control) {

            // assert ((control & 0xF) != SizedTypes::DEL);
            size_t size = read_unsigned_number(control);

            if (verbose) printf("%s(%i) ", SizedTypes_Name(control.Sized.type), size);

            switch (control.Sized.type) {
                case SizedTypes::UINT: 
                    return PyLong_FromLongLong(size);
                case SizedTypes::HANDLE: 
                    return read_handle(size);
                case SizedTypes::BINDING: 
                    return read_binding(size);
                case SizedTypes::BYTES: return read_bytes(size);
                case SizedTypes::LIST: return read_list(size);
                case SizedTypes::DICT: return read_dict(size);
                case SizedTypes::TUPLE: return read_tuple(size);
                case SizedTypes::STR: return read_str(size);
                case SizedTypes::PICKLED: return read_pickled(size);

                case SizedTypes::BINDING_DELETE:
                    read_binding_delete(size);
                    return read();

                case SizedTypes::DELETE:
                    read_delete(size);
                    return read();

                default:
                    PyErr_Format(PyExc_RuntimeError, "unknown sized type: %i", control.Sized.type);
                    throw nullptr;
            } 
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
            bindings[binding_counter++] = instance;
        }

        uint64_t read_expected_int() {
            uint8_t i = _read<uint8_t>();
            
            return i == 255 ? _read<uint64_t>() : (uint64_t)i;
        }

        template<typename T>
        std::vector<T> read_expected_vector_of()
        {
            size_t size = read_expected_int();
            std::vector<T> result;
            result.reserve(size);

            while (size--) {
                result.push_back(_read<T>());
            }
            return result;
        }

        void read_add_filename() {        
            index2filename[filename_index_counter++] = read();
        }

        void on_difference(std::vector<CodeLocation> previous, std::vector<CodeLocation> record, std::vector<CodeLocation> replay) {
            auto pythonize = [](std::vector<CodeLocation> frames) { 
                return to_pylist(frames | std::views::transform(&CodeLocation::as_tuple));
            };

            PyUniquePtr py_previous(pythonize(previous));
            PyUniquePtr py_record(pythonize(record));
            PyUniquePtr py_replay(pythonize(replay));

            PyObject * res = PyObject_CallFunctionObjArgs(on_stack_difference, py_previous.get(), py_record.get(), py_replay.get(), nullptr);
            Py_XDECREF(res);
            if (!res) throw nullptr;
        }

        std::vector<CodeLocation> read_record_stack() {
            static thread_local std::vector<CodeLocation> stack;
            size_t to_drop = read_expected_int();

            assert(stack.size() >= to_drop);
            while (to_drop--) stack.pop_back();

            uint64_t size = read_expected_int();
            while (size--) {
                stack.push_back(CodeLocation(index2filename[_read<uint16_t>()], _read<uint16_t>()));
            }
            return stack;
        }

        std::vector<CodeLocation> read_replay_stack() {
            static thread_local std::vector<Frame> stack;
            static thread_local std::vector<CodeLocation> locations;

            size_t common = update_stack(exclude_stacktrace, stack);

            while (locations.size() > common) locations.pop_back();

            for (Frame frame : stack | std::views::drop(common)) {
                CodeLocation location = frame.location();
                if (normalize_path) {
                    if (!normalized_paths.contains(location.filename)) {
                        PyObject * normalized = PyObject_CallOneArg(normalize_path, location.filename);
                        if (!normalized) throw nullptr;
                        normalized_paths[Py_NewRef(location.filename)] = normalized;
                    }
                    location.filename = normalized_paths[location.filename];
                }
                locations.push_back(location);
            }
            return locations;
        }

        void read_stack() {

            static thread_local std::vector<CodeLocation> previous;

            auto record = read_record_stack();
            auto replay = read_replay_stack();

            if (record == replay) {
                previous = record;
            } else {
                on_difference(previous, record, replay);
                previous.clear();
            }
        }

        void read_thread_switch() {
            if (verbose) printf("consumed THREAD_SWITCH\n");

            Py_XDECREF(active_thread);
            active_thread = read();

            if (verbose) {
                PyObject * str = PyObject_Str(active_thread);
                if (verbose) printf("consumed THREAD_SWITCH %s\n", PyUnicode_AsUTF8(str));
                Py_DECREF(str);
            }
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
                    return PyFloat_FromDouble(_read<double>());
                case FixedSizeTypes::INT64:
                    return PyLong_FromLongLong(_read<int64_t>());
                // case FixedSizeTypes::INLINE_NEW_HANDLE: return Py_NewRef(store_handle());

                case FixedSizeTypes::EXT_BIND:
                    read_ext_bind();
                    return read();

                case FixedSizeTypes::ADD_FILENAME:
                    read_add_filename();
                    return read();

                case FixedSizeTypes::NEW_HANDLE:
                    if (verbose) printf("consumed NEW_HANDLE\n");
                    store_handle();
                    return read();

                case FixedSizeTypes::THREAD_SWITCH:
                    read_thread_switch();
                    return read();

                case FixedSizeTypes::BIND:
                    return bind_singleton;

                // case FixedSizeTypes::CODEOBJ:
                //     read_codeobj();
                //     return read();

                case FixedSizeTypes::STACK:
                    read_stack();
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

        int64_t read_sized_number(Control control) {
            switch (control.Sized.size) {
                case ONE_BYTE_SIZE:
                    return (int64_t)_read<int8_t>();
                case TWO_BYTE_SIZE:
                    return (int64_t)_read<int16_t>();
                case FOUR_BYTE_SIZE:
                    return (int64_t)_read<int32_t>();
                case EIGHT_BYTE_SIZE:
                    return _read<int64_t>();
                default:
                    return (int64_t)(control.Sized.size);
            }
        }

        PyObject * store_handle() {            
            PyObject * ref = read();

            if (transform) {
                PyObject * new_ref = PyObject_CallOneArg(transform, ref);

                Py_DECREF(ref);
                if (!new_ref) {
                    throw nullptr;
                }
                ref = new_ref;
            }
            lookup[next_handle++] = ref;
            return ref;
        }

        // bool consume(Control control) {
            
        //     if (fixed_size_type(control) == FixedSizeTypes::THREAD_SWITCH) {
        //         return true;
        //     }
        //     return false;
        // }

        Control read_control() {
            Control c;
            c.raw = _read<uint8_t>();
            return c;
        }

        void read_magic() {
            uint64_t magic = _read<uint64_t>();
            if (magic != MAGIC) {
                raise(SIGTRAP);
                PyErr_Format(PyExc_RuntimeError, "Bad internal exception reading tracefile, next element wasnt MAGIC");
                throw nullptr;
            }
        }

        PyObject * read_root() {
            
            if (magic_markers) read_magic();

            // while (consume(control)) {
            //     control = read_control();
            // }
            PyObject * res = read();

            if (verbose) {
                PyObject * str = PyObject_Str(res);
                if (!str) {
                    raise(SIGTRAP);
                }
                printf("root: %s\n", PyUnicode_AsUTF8(str));
                Py_DECREF(str);
            }
            return res;
        }

        PyObject * read(Control control) {
            return control.Sized.type == SizedTypes::FIXED_SIZE
                ? read_fixedsize(control.Fixed.type)
                : read_sized(control);
        }

        PyObject * read() { 
            return read(read_control());
        }

        static void raise_timeout(PyObject * pending) {
            // 1. Create the custom data object (a tuple containing the duration and code)
            // The first element is the main message string, followed by the custom data.
            PyObject *data_tuple = Py_BuildValue(
                "(s, O)",
                "Operation timed out.", // Main message (will be e.args[0]) 
                pending);
            
            if (!data_tuple) {
                // Py_BuildValue failed (and set an exception)
                return; 
            }

            // 2. Set the TimeoutError exception and pass the custom data tuple.
            // PyErr_SetObject "steals" a reference to the data_tuple.
            PyErr_SetObject(PyExc_TimeoutError, data_tuple);

            // 3. Decrement the reference count of the data object.
            Py_DECREF(data_tuple);
        }

        PyObject * create_stacktrace(PyObject * current_thread) {
            PyObject * func = PyDict_GetItem(pending_reads, current_thread);

            return PyCallable_Check(func) ? PyObject_CallNoArgs(func) : Py_NewRef(Py_None);
        }

        void set_stacktrace(PyObject * current_thread) {
            assert (PyGILState_Check());

            PyObject * trace = create_stacktrace(current_thread);
            if (!trace) throw nullptr;
            PyDict_SetItem(stacktraces, current_thread, trace);
            Py_DECREF(trace);
        }

        void on_thread_timeout(std::unique_lock<std::mutex> &lock, PyObject * current_thread) {
            {
                retracesoftware::GILGuard guard;

                if (!stacktraces) {
                    stacktraces = PyDict_New();
                }
                set_stacktrace(current_thread);
            }

            auto pred = [this]() {
                retracesoftware::GILGuard guard();
                return PyDict_Size(stacktraces) == PyDict_Size(pending_reads);
            };

            wakeup.notify_all();
            wakeup.wait(lock, pred);

            {
                retracesoftware::GILGuard guard;
                PyErr_Format(PyExc_TimeoutError, "Thread %S timed out", current_thread);
            }
            // raise_timeout(stacktraces);
            // stacktraces = nullptr;
            throw nullptr;
        }

        // uint8_t next_root_control(int timeout_seconds, PyObject * current_thread) {
        //     retracesoftware::GILReleaseGuard guard;
        //     std::unique_lock<std::mutex> lock(mtx);
            
        //     load_next_control();

        //     if (!equal(current_thread, active_thread)) {

        //         auto pred = [this, current_thread]() {
        //             retracesoftware::GILGuard guard;

        //             // ok we have the GIL
        //             if (stacktraces) {

        //                 switch (PyDict_Contains(stacktraces, current_thread)) {
        //                     case 0:
        //                         set_stacktrace(current_thread);
        //                         wakeup.notify_all();
        //                         break;
        //                     case 1:
        //                         break;
        //                     default:
        //                         throw nullptr;
        //                 }
        //                 return false;
        //             } else {
        //                 load_next_control();
        //                 return equal(current_thread, active_thread);
        //             }
        //         };

        //         if (!wakeup.wait_for(lock, std::chrono::seconds(timeout_seconds), pred)) {
        //             on_thread_timeout(lock, current_thread);
        //         }
                
        //         assert (next_control != -1);
        //     }

        //     uint8_t control = next_control;
        //     next_control = -1;
        //     wakeup.notify_all();
        //     return control;
        // }

        PyObject* read_next(int timeout_seconds, PyObject * stacktrace) {
            // should release the GIL first
            PyObject * current_thread = PyObject_CallNoArgs(thread);
            if (!current_thread) return nullptr;

            PyDict_SetItem(pending_reads, current_thread, stacktrace);
            Py_DECREF(current_thread);

            retracesoftware::GILReleaseGuard guard;
            // GIL is now released

            std::unique_lock<std::mutex> lock(mtx);

            try {
                if (!next) {
                    retracesoftware::GILGuard guard;
                    next = read_root();
                    wakeup.notify_all();
                }

                if (!equal(current_thread, active_thread)) {

                    auto pred = [this, current_thread]() {
                        retracesoftware::GILGuard guard;

                        // ok we have the GIL
                        if (stacktraces) {

                            switch (PyDict_Contains(stacktraces, current_thread)) {
                                case 0:
                                    set_stacktrace(current_thread);
                                    wakeup.notify_all();
                                    break;
                                case 1:
                                    break;
                                default:
                                    throw nullptr;
                            }
                            return false;
                        } else {
                            if (!next) {
                                next = read_root();
                                wakeup.notify_all();
                            }                    
                            return equal(current_thread, active_thread);
                        }
                    };

                    if (!wakeup.wait_for(lock, std::chrono::seconds(timeout_seconds), pred)) {
                        on_thread_timeout(lock, current_thread);
                    }
                }

                PyObject * res = next;
                next = nullptr;
                wakeup.notify_all();

                messages_read++;
                        
                {
                    retracesoftware::GILGuard guard;
                    PyDict_DelItem(pending_reads, current_thread);
                }
                return res;
            } catch (...) {
                retracesoftware::GILGuard guard;
                assert (PyErr_Occurred());
                PyDict_DelItem(pending_reads, current_thread);
                return nullptr;
            }
        }

        static PyObject * py_bind(ObjectReader *self, PyObject* args, PyObject * kwds) {
            PyObject * obj;
            PyObject * stacktrace = Py_None;
            int timeout_seconds = 5;

            static const char* kwlist[] = {
                "obj",
                "timeout_seconds", 
                "stacktrace", 
                nullptr};  // Keywords allowed

            if (!PyArg_ParseTupleAndKeywords(args, kwds, "O|IO", (char **)kwlist, 
                &obj,
                &timeout_seconds,
                &stacktrace)) {
                
                return nullptr;
            }

            PyObject * res = self->read_next(timeout_seconds, stacktrace);

            if (res == self->bind_singleton) {
                self->bindings[self->binding_counter++] = Py_NewRef(obj);
                Py_RETURN_NONE;
            } else if (res) {
                PyErr_Format(PyExc_TypeError, "Expected bind as next call but got: %S", res);
                return nullptr;
            }
        }

        static PyObject* py_call(ObjectReader* self, PyObject* args, PyObject * kwds) {

            PyObject * stacktrace = Py_None;
            int timeout_seconds = 5;

            static const char* kwlist[] = {
                "timeout_seconds", 
                "stacktrace", 
                nullptr};  // Keywords allowed

            if (!PyArg_ParseTupleAndKeywords(args, kwds, "|IO", (char **)kwlist, 
                &timeout_seconds,
                &stacktrace)) {
                
                return nullptr;
            }

            PyObject * res = self->read_next(timeout_seconds, stacktrace);

            if (res == self->bind_singleton) {
                PyErr_Format(PyExc_TypeError, "Got unexpected bind as next call");
                return nullptr;
            } else {
                return res;
            }
        }

        static PyObject* create(PyTypeObject* type, PyObject*, PyObject*) {
            auto* self = reinterpret_cast<ObjectReader *>(type->tp_alloc(type, 0));
            if (!self) return nullptr;

            // Construct the std::set in-place
            new (&self->lookup) map<uint64_t, PyObject*>();
            // new (&self->pending_reads) map<PyThreadState *, PyObject*>();

            // new (&self->type_deserializers) map<PyTypeObject *, PyObject*>();
            new (&self->mtx) std::mutex();
            new (&self->wakeup) std::condition_variable();
            
            self->next_control = -1;

            return reinterpret_cast<PyObject*>(self);
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

                self->thread = Py_XNewRef(thread);
                self->path = Py_NewRef(path);
                self->deserializer = Py_NewRef(deserializer);
                self->bind_singleton = PyObject_New(PyObject, &PyBaseObject_Type);
                self->bytes_read = self->messages_read = 0;
                // enumtype(enumtype);
                self->next_handle = 0;
                self->transform = transform != Py_None ? Py_XNewRef(transform) : nullptr;
                self->verbose = verbose;
                self->normalize_path = Py_XNewRef(normalize_path);
                self->magic_markers = magic_markers;
                self->on_stack_difference = Py_NewRef(on_stack_difference);
                // self->vectorcall = reinterpret_cast<vectorcallfunc>(ObjectReader::py_vectorcall);
                self->pending_reads = PyDict_New();
                self->stacktraces = nullptr;
                self->next = nullptr;
                
                if (thread) {
                    self->active_thread = PyObject_CallNoArgs(thread);
                    if (!self->active_thread) return -1;
                }

                self->binding_counter = self->filename_index_counter = 0;

                new (&self->bindings) map<int, PyObject *>();
                new (&self->obj_to_hash) map<PyCodeObject *, Py_hash_t>();
                new (&self->index2filename) map<uint16_t, PyObject *>();
                new (&self->exclude_stacktrace) set<PyFunctionObject *>();
                new (&self->normalized_paths) map<PyObject *, PyObject *>();

                self->file = open(path);

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
            Py_VISIT(self->bind_singleton);
            Py_VISIT(self->on_stack_difference);
            Py_VISIT(self->normalize_path);

            for (const auto& [key, value] : self->lookup) {
                Py_VISIT(value);
            }
            // Py_VISIT(self->name_cache);
            return 0;
        }

        static int clear(ObjectReader* self) {
            // Py_CLEAR(self->name_cache);
            Py_CLEAR(self->thread);
            Py_CLEAR(self->path);
            Py_CLEAR(self->deserializer);
            Py_CLEAR(self->bind_singleton);
            Py_CLEAR(self->on_stack_difference);
            Py_CLEAR(self->normalize_path);

            for (const auto& [key, value] : self->lookup) {
                Py_DECREF(value);
            }
            self->lookup.clear();

            return 0;
        }

        static void dealloc(ObjectReader* self) {
            if (self->file) {
                fclose(self->file);
                self->file = nullptr;
            }
            
            PyObject_GC_UnTrack(self);
            clear(self);

            self->lookup.~map<uint64_t, PyObject*>();

            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
        }

        static PyObject * next_control_getter(ObjectReader *self, void *closure) {
            if (self->next_control == -1) {
                Py_RETURN_NONE;
            }

            return PyLong_FromLong(self->next_control);
        }
        
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
            if (self->file) {
                fclose(self->file);
                self->file = nullptr;
            }
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

                    // if there is an existing object writer, destroy it
                    if (self->file) {
                        fclose(self->file);
                        self->file = nullptr;
                    }
                    Py_DECREF(self->path);
                    self->path = Py_NewRef(value);
                    
                    if (!PyCallable_Check(value)) {
                        try {
                            self->file = open(self->path);

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

        static PyObject * py_wake_pending(ObjectReader * self, PyObject* unused) {
            self->wakeup.notify_all();
            Py_RETURN_NONE;
        }

        static PyObject * py_load_hash_secret(ObjectReader * self, PyObject* unused) {                
            try {
                self->read((uint8_t *)&_Py_HashSecret, sizeof(_Py_HashSecret_t));
                Py_RETURN_NONE;
            } catch (...) {
                return nullptr;
            }

        }
    };

    static PyMethodDef methods[] = {
        {"load_hash_secret", (PyCFunction)ObjectReader::py_load_hash_secret, METH_NOARGS, "TODO"},
        {"wake_pending", (PyCFunction)ObjectReader::py_wake_pending, METH_NOARGS, "TODO"},
        {"bind", (PyCFunction)ObjectReader::py_bind, METH_VARARGS | METH_KEYWORDS, "TODO"},
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
        {"bytes_read", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectReader, bytes_read), READONLY, "TODO"},
        {"messages_read", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectReader, messages_read), READONLY, "TODO"},
        {"stacktraces", T_OBJECT, OFFSET_OF_MEMBER(ObjectReader, stacktraces), READONLY, "TODO"},
        {"active_thread", T_OBJECT, OFFSET_OF_MEMBER(ObjectReader, active_thread), READONLY, "TODO"},
        {"verbose", T_BOOL, OFFSET_OF_MEMBER(ObjectReader, verbose), 0, "TODO"},
        // {"pending_reads", T_OBJECT, OFFSET_OF_MEMBER(ObjectReader, pending_reads), READONLY, "TODO"},
        // {"path", T_OBJECT, OFFSET_OF_MEMBER(Writer, path), READONLY, "TODO"},
        // {"on_pid_change", T_OBJECT_EX, OFFSET_OF_MEMBER(Writer, on_pid_change), 0, "TODO"},
        {NULL}  /* Sentinel */
    };

    static PyGetSetDef getset[] = {
        {"path", (getter)ObjectReader::path_getter, (setter)ObjectReader::path_setter, "TODO", NULL},
        {"next_control", (getter)ObjectReader::next_control_getter, nullptr, "TODO", NULL},
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
        // .tp_vectorcall_offset = OFFSET_OF_MEMBER(ObjectReader, vectorcall),
        .tp_call = (ternaryfunc)ObjectReader::py_call,
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