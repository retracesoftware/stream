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
#include <structmember.h>

namespace retracesoftware_stream {

    static FILE * open(PyObject * path) {
        // int fd = open(PyUnicode_AsUTF8(path_str), O_WRONLY | O_CREAT | O_EXCL, 0644);
        
        FILE * file = fopen(PyUnicode_AsUTF8(path), "rb");

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

    struct ObjectStream : public PyObject {
        FILE * file = nullptr;
        size_t bytes_read = 0;
        size_t messages_read = 0;
        int read_timeout = 0;
        bool magic_markers = false;
        vectorcallfunc vectorcall;
        std::vector<PyObject *> handles;
        std::vector<PyObject *> filenames;

        map<int, PyObject *> bindings;
        bool pending_bind = false;
        int binding_counter = 0;
        PyObject * create_pickled = nullptr;

        PyObject * bind_singleton;
        PyObject * create_stack_delta;
        PyObject * create_thread_switch;
        bool verbose = false;

        static int init(ObjectStream * self, PyObject* args, PyObject* kwds) {
            
            PyObject * path;
            PyObject * create_pickled;
            PyObject * bind_singleton;
            PyObject * create_stack_delta;
            PyObject * create_thread_switch;

            int magic_markers = 0;
            int read_timeout = 0;
            int verbose = 0;

            static const char* kwlist[] = {
                "path", 
                "deserialize",
                "bind_singleton",
                "create_stack_delta",
                "on_thread_switch",
                "read_timeout",
                "magic_markers",
                "verbose",
                nullptr};  // Keywords allowed

            if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!OOOOKpp", (char **)kwlist, 
                &PyUnicode_Type, &path, 
                &create_pickled,
                &bind_singleton,
                &create_stack_delta,
                &create_thread_switch,
                &read_timeout,
                &magic_markers,
                &verbose)) {
                return -1;
            }

            new (&self->handles) std::vector<PyObject *>();
            new (&self->filenames) std::vector<PyObject *>();
            new (&self->bindings) map<int, PyObject *>();

            self->create_pickled = Py_NewRef(create_pickled);
            self->bind_singleton = Py_NewRef(bind_singleton);
            self->create_stack_delta = Py_NewRef(create_stack_delta);
            self->create_thread_switch = Py_NewRef(create_thread_switch);
            self->read_timeout = read_timeout;
            self->verbose = verbose;

            self->vectorcall = (vectorcallfunc)call;

            try {
                self->file = open(path);
            } catch (...) {
                return -1;
            }
            return 0;
        }

        void close() {
            if (file) {
                fclose(file);
                file = nullptr;
            }
        }

        static void dealloc(ObjectStream* self) {
            // if (self->file) {
            //     fclose(self->file);
            //     self->file = nullptr;
            // }
            
            // self->handles.~std::vector<PyObject *>();

            PyObject_GC_UnTrack(self);
            clear(self);

            self->handles.std::vector<PyObject *>::~vector();
            self->filenames.std::vector<PyObject *>::~vector();
            self->bindings.~map<int, PyObject *>();

            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
        }

        static int traverse(ObjectStream* self, visitproc visit, void* arg) {
            Py_VISIT(self->create_pickled);
            Py_VISIT(self->bind_singleton);
            Py_VISIT(self->create_stack_delta);
            Py_VISIT(self->create_thread_switch);

            return 0;
        }

        static int clear(ObjectStream* self) {

            for (auto elem : self->handles) {
                Py_XDECREF(elem);
            }
            self->handles.clear();

            for (auto elem : self->filenames) {
                Py_XDECREF(elem);
            }
            self->filenames.clear();

            Py_CLEAR(self->create_pickled);
            Py_CLEAR(self->bind_singleton);
            Py_CLEAR(self->create_stack_delta);
            Py_CLEAR(self->create_thread_switch);

            return 0;
        }

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

        Control read_control() {
            return Control(read<uint8_t>());
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

        PyObject * read_list(size_t size) {
            auto list = PyObjectPtr(PyList_New(size));

            if (!list.get()) throw nullptr;

            for (size_t i = 0; i < size; i++) {
                PyList_SetItem(list.get(), i, read());
            }
            return Py_NewRef(list.get());
        }

        PyObject * read_tuple(size_t size) {
            auto tuple = PyObjectPtr(PyTuple_New(size));

            if (!tuple.get()) throw nullptr;

            for (size_t i = 0; i < size; i++) {
                if (PyTuple_SetItem(tuple.get(), i, read()) == -1) {
                    throw nullptr;
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

        PyObject * read_str(size_t size) {

            static thread_local int8_t scratch[1024];

            if (size >= sizeof(scratch)) {
                int8_t * buffer = (int8_t *)malloc(size + 1);

                if (!buffer) {
                    PyErr_Format(PyExc_MemoryError, "Error allocating: %zu bytes", size + 1);
                    throw nullptr;
                }

                read((uint8_t *)buffer, size);
                buffer[size] = '\0';

                PyObject * str = PyUnicode_DecodeUTF8((char *)buffer, size, "strict");

                free(buffer);

                if (!str) throw nullptr;
                return str;

            } else {
            
                read((uint8_t *)scratch, size);
                scratch[size] = '\0';

                PyObject * decoded = PyUnicode_DecodeUTF8((char *)scratch, size, "strict");

                if (!decoded) throw nullptr;
                return decoded;
            }
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

        PyObject * read_pickled(size_t size) {
            PyObject * bytes = read_bytes(size);

            PyObject * res = PyObject_CallOneArg(create_pickled, bytes);

            Py_DECREF(bytes);

            if (!res) throw nullptr;
            return res;
        }

        static PyObject * create_indexed(PyObject * factory, size_t index) {
            PyObject * i = PyLong_FromLong(index);
            if (!i) throw nullptr;
            PyObject * res = PyObject_CallOneArg(factory, i);
            Py_DECREF(i);
            if (!res) throw nullptr;
            return res;
        }

        PyObject * read_sized(Control control) {

            size_t size = read_unsigned_number(control);

            switch (control.Sized.type) {
                case SizedTypes::UINT: {
                    PyObject * result = PyLong_FromLongLong(size);
                    if (!result) throw nullptr;
                    return result;
                }
                case SizedTypes::HANDLE:
                    return Py_NewRef(handles[size]);
                case SizedTypes::BINDING:
                    return Py_NewRef(bindings[size]);
                case SizedTypes::BYTES: return read_bytes(size);
                case SizedTypes::LIST: return read_list(size);
                case SizedTypes::DICT: return read_dict(size);
                case SizedTypes::TUPLE: return read_tuple(size);
                case SizedTypes::STR: return read_str(size);
                case SizedTypes::PICKLED: return read_pickled(size);
                default:
                    PyErr_Format(PyExc_RuntimeError, "unknown sized type: %i", control.Sized.type);
                    throw nullptr;
            }
        }

        PyObject * create_from_next(PyObject * factory) {
            PyObject * next = read();
            if (!next) return nullptr;

            PyObject * result = PyObject_CallOneArg(factory, next);

            Py_DECREF(next);
            return result;
        }

        uint64_t read_expected_int() {
            uint8_t i = read<uint8_t>();
            
            return i == 255 ? read<uint64_t>() : (uint64_t)i;
        }

        PyObject * read_stack_delta() {

            int size = read_expected_int();
            
            PyObject * stack = PyList_New(size);
            if (!stack) throw nullptr;

            for (size_t i = 0; i < size; i++) {
                PyObject * filename = filenames[read<uint16_t>()];

                int l = read<uint16_t>();

                if (verbose) {
                    printf("  %s:%i\n", PyUnicode_AsUTF8(filename), l);
                }

                PyObject * lineno = PyLong_FromLong(l);
                if (!lineno) throw nullptr;

                PyObject * frame = PyTuple_Pack(2, filename, lineno);
                Py_DECREF(lineno);
                if (!frame) throw nullptr;

                PyList_SET_ITEM(stack, i, frame);
            }
            return stack;
        }

        PyObject * read_fixedsize(FixedSizeTypes type) {
            assert (!PyErr_Occurred());

            // if (verbose) printf("%s ", FixedSizeTypes_Name(type));

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
                    return PyFloat_FromDouble(read<double>());
                case FixedSizeTypes::INT64:
                    return PyLong_FromLongLong(read<int64_t>());
                
                default:
                    const char * name = FixedSizeTypes_Name(static_cast<FixedSizeTypes>(type));

                    if (name) {
                        PyErr_Format(PyExc_RuntimeError, 
                            "Unexpected control byte '%s' where object was expected. "
                            "This usually indicates a version mismatch between recording and replay. "
                            "Please re-record with the current version of retracesoftware.stream.", name);
                    } else {
                        PyErr_Format(PyExc_RuntimeError, 
                            "Unknown FixedSizeType: %i. "
                            "This usually indicates a version mismatch between recording and replay.", type);
                    }
                    throw nullptr;
            };
        }

        PyObject * read() {
            return read(read_control());
        }

        PyObject * read(Control control) {
            return control.Sized.type == SizedTypes::FIXED_SIZE
                ? read_fixedsize(control.Fixed.type)
                : read_sized(control);
        }

        PyObject * read_ext_bind() {

            PyTypeObject * cls = (PyTypeObject *)read();
            
            if (!cls) {
                if (!PyErr_Occurred()) {
                    PyErr_SetString(PyExc_RuntimeError, "read() returned NULL without setting exception in read_ext_bind");
                }
                throw nullptr;
            }

            if (!PyType_Check((PyObject *)cls)) {
                PyErr_Format(PyExc_TypeError, "Expected next item read to be a type but was: %S", cls);
                Py_DECREF(cls);
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
            return instance;
        }

        Control consume() {
            Control control = read_control();
            
            if (control == NewHandle) {
                if (verbose) printf("Retrace - ObjectStream[%lu] - Consumed NEW_HANDLE\n", messages_read);

                handles.push_back(read());
                messages_read++;
                return consume();
            } else if (control == AddFilename) {
                if (verbose) printf("Retrace - ObjectStream[%lu] - Consumed ADD_FILENAME\n", messages_read);

                filenames.push_back(read());
                messages_read++;
                return consume();
            } else if (control.Sized.type == SizedTypes::DELETE) {
                if (verbose) printf("Retrace - ObjectStream[%lu] - Consumed DELETE\n", messages_read);

                size_t size = read_unsigned_number(control);
                int index = handles.size() - 1 - size;
                Py_DECREF(handles[index]);
                handles[index] = nullptr;
                messages_read++;
                return consume();
            } else if (control.Sized.type == SizedTypes::BINDING_DELETE) {
                if (verbose) printf("Retrace - ObjectStream[%lu] - Consumed BINDING_DELETE\n", messages_read);

                size_t index = read_unsigned_number(control);
                Py_DECREF(bindings[index]);
                bindings.erase(index);
                messages_read++;
                return consume();
            } else if (control == ExtBind) {                
                if (verbose) printf("Retrace - ObjectStream[%lu] - Consumed EXT_BIND\n", messages_read);

                bindings[binding_counter++] = read_ext_bind();
                messages_read++;
                return consume();
            } else {
                return control;
            }
        }

        bool bind(PyObject * binding) {
            if (!pending_bind) {
                PyErr_Format(PyExc_RuntimeError, "Trying to bind when no pending bind");
                return false;
            } 
            bindings[binding_counter++] = Py_NewRef(binding);
            pending_bind = false;
            return true;
        }

        static PyObject * py_bind(ObjectStream * self, PyObject * binding) {
            try {
                if (!self->bind(binding)) 
                    return nullptr;
            } catch (...) {
                assert(PyErr_Occurred());
                return nullptr;
            }
            Py_RETURN_NONE;
        }

        static PyObject * py_close(ObjectStream * self, PyObject * usused) {
            self->close();
            Py_RETURN_NONE;
        }

        PyObject * next() {
            if (pending_bind) {
                PyErr_Format(PyExc_RuntimeError, "Can't reading next as unbound pending bind");
                return nullptr;
            }

            Control control = consume();

            if (control == Stack) {
                int to_drop = read_expected_int();

                if (verbose) {
                    printf("Retrace - ObjectStream[%lu] - Consumed STACK - drop: %i\n", messages_read, to_drop);
                }

                PyObject * stack_delta = read_stack_delta();

                messages_read++;

                PyObject * py_to_drop = PyLong_FromLong(to_drop);
                PyObject * result = PyObject_CallFunctionObjArgs(create_stack_delta, py_to_drop, stack_delta, nullptr);
                Py_DECREF(py_to_drop);
                Py_DECREF(stack_delta);
                return result;
            }
            if (control == ThreadSwitch) {

                PyObject * thread = read();

                if (verbose) {
                    PyObject * s = PyObject_Str(thread);
                    printf("Retrace - ObjectStream[%lu] - Consumed THREAD_SWITCH(%s)\n", messages_read, PyUnicode_AsUTF8(s));
                    Py_DECREF(s);
                }
                messages_read++;
                PyObject * result = PyObject_CallOneArg(create_thread_switch, thread);
                Py_DECREF(thread);
                return result;
            }
            if (control == Bind) {
                if (verbose) printf("Retrace - ObjectStream[%lu] - Read BIND\n", messages_read);

                pending_bind = true;
                messages_read++;
                return Py_NewRef(bind_singleton);
            }
            else {
                PyObject * result = read(control);

                if (verbose) {
                    PyObject * s = PyObject_Str(result);
                    printf("Retrace - ObjectStream[%lu] - Read: %s\n", messages_read, PyUnicode_AsUTF8(s));
                    Py_DECREF(s);
                }
                messages_read++;
                return result;
            }
        }

        static PyObject* call(ObjectStream *self, PyObject *const *args, size_t nargsf, PyObject *kwnames) {
            try {
                return self->next();
            } catch (...) {
                if (!PyErr_Occurred()) {
                    raise(SIGTRAP);
                }
                return nullptr;
            }
        }
    };

    static PyMemberDef members[] = {
        // {"bytes_read", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectReader, bytes_read), READONLY, "TODO"},
        // {"messages_read", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectReader, messages_read), READONLY, "TODO"},
        // {"stacktraces", T_OBJECT, OFFSET_OF_MEMBER(ObjectReader, stacktraces), READONLY, "TODO"},
        // {"active_thread", T_OBJECT, OFFSET_OF_MEMBER(ObjectReader, active_thread), READONLY, "TODO"},
        // {"stack_stop_at", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectReader, stack_stop_at), 0, "TODO"},
        // {"pending_reads", T_OBJECT, OFFSET_OF_MEMBER(ObjectReader, pending_reads), READONLY, "TODO"},
        // {"path", T_OBJECT, OFFSET_OF_MEMBER(Writer, path), READONLY, "TODO"},
        {"read_timeout", T_ULONG, OFFSET_OF_MEMBER(ObjectStream, read_timeout), 0, "TODO"},
        {"bytes_read", T_ULONG, OFFSET_OF_MEMBER(ObjectStream, bytes_read), READONLY, "TODO"},
        {"messages_read", T_ULONG, OFFSET_OF_MEMBER(ObjectStream, messages_read), READONLY, "TODO"},
        {"pending_bind", T_BOOL, OFFSET_OF_MEMBER(ObjectStream, pending_bind), READONLY, "TODO"},
        {"verbose", T_BOOL, OFFSET_OF_MEMBER(ObjectStream, verbose), 0, "TODO"},
        {NULL}  /* Sentinel */
    };

    static PyGetSetDef getset[] = {
        // {"next_control", (getter)ObjectReader::next_control_getter, nullptr, "TODO", NULL},
        // {"next_control", (getter)ObjectReader::next_control_getter, nullptr, "TODO", NULL},
        // {"pending", (getter)ObjectReader::pending_getter, nullptr, "TODO", NULL},
        // {"thread_number", (getter)Writer::thread_getter, (setter)Writer::thread_setter, "TODO", NULL},
        {NULL}  // Sentinel
    };

    static PyMethodDef methods[] = {
        {"bind", (PyCFunction)ObjectStream::py_bind, METH_O, "TODO"},
        {"close", (PyCFunction)ObjectStream::py_close, METH_NOARGS, "TODO"},
        // {"exclude_from_stacktrace", (PyCFunction)ReaderWriterBase::py_exclude_from_stacktrace, METH_O, "TODO"},
        {NULL}  // Sentinel
    };

    PyTypeObject ObjectStream_Type = {
        .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = MODULE "ObjectStreamReader",
        .tp_basicsize = sizeof(ObjectStream),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)ObjectStream::dealloc,
        .tp_vectorcall_offset = OFFSET_OF_MEMBER(ObjectStream, vectorcall),
        .tp_call = PyVectorcall_Call,
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_VECTORCALL,
        .tp_doc = "TODO",
        .tp_traverse = (traverseproc)ObjectStream::traverse,
        .tp_clear = (inquiry)ObjectStream::clear,
        .tp_methods = methods,
        .tp_members = members,
        .tp_getset = getset,
        .tp_init = (initproc)ObjectStream::init,
        .tp_new = PyType_GenericNew,
    };
}