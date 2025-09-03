#include "stream.h"
#include <structmember.h>
#include "unordered_dense.h"
#include "wireformat.h"
#include <algorithm>
#include <fcntl.h>
#include <sstream>
#include <sys/file.h>

using namespace ankerl::unordered_dense;

namespace retracesoftware_stream {

    const char * FixedSizeTypes_Name(enum FixedSizeTypes root) {
        switch (root) {
            case FixedSizeTypes::NONE: return "NONE";
            // case FixedSizeTypes::C_NULL: return "C_NULL";
            case FixedSizeTypes::TRUE: return "TRUE";
            case FixedSizeTypes::FALSE: return "FALSE";
            case FixedSizeTypes::NEW_HANDLE: return "NEW_HANDLE";
            case FixedSizeTypes::REF: return "REF";
            // case FixedSizeTypes::PLACEHOLDER: return "PLACEHOLDER";

            // case FixedSizeTypes::EXTREF: return "EXTREF";
            // case FixedSizeTypes::CACHE_LOOKUP: return "CACHE_LOOKUP";
            // case FixedSizeTypes::CACHE_ADD: return "CACHE_ADD";
            // case FixedSizeTypes::METHOD_DESCRIPTOR: return "METHOD_DESCRIPTOR";
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

    struct ObjectReader : public PyObject {
        
        FILE * file;
        size_t bytes_read;
        size_t messages_read;
        int next_handle;
        // PyTypeObject * enumtype;

        int pid;

        map<uint64_t, PyObject *> lookup;
        // map<PyTypeObject *, PyObject *> type_deserializers;

        PyObject * deserializer;
        PyObject * path;
        PyObject * transform;

        vectorcallfunc vectorcall;

        void read(uint8_t * bytes, Py_ssize_t size) {
            fread(bytes, sizeof(uint8_t), size, file);
            bytes_read += size;
        }

        inline uint8_t read_uint8() {
            uint8_t byte;
            read(&byte, 1);
            return byte;
        }

        inline int8_t read_int8() {
            return (int8_t)read_uint8();
        }

        inline uint16_t read_uint16() {
            uint8_t buffer[sizeof(uint16_t)];
            read(buffer, sizeof(buffer));

            uint16_t value = (uint16_t)buffer[0];
            value |= ((uint16_t)buffer[1] << 8);
            return value;
        }

        inline int16_t read_int16() {
            return (int16_t)read_uint16();
        }

        inline uint32_t read_uint32() {
            uint8_t buffer[sizeof(uint32_t)];
            read(buffer, sizeof(buffer));

            uint32_t value = (uint32_t)buffer[0];
            value |= ((uint32_t)buffer[1] << 8);
            value |= ((uint32_t)buffer[2] << 16);
            value |= ((uint32_t)buffer[3] << 24);
            return value;
        }

        inline int32_t read_int32() {
            return (int32_t)read_uint32();
        }

        inline uint64_t read_uint64() {
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

        inline int64_t read_int64() {
            return (int64_t)read_uint64();
        }

        double read_float() {
            uint64_t raw = read_uint64();
            return *(double *)&raw;
        }

        PyObject * read_handle(int64_t index) {
            assert(index < next_handle);
            assert(lookup.contains(index));

            return Py_NewRef(lookup[index]);
        }

        size_t read_unsigned_number(uint8_t control) {

            switch (control & 0xF0) {
                case ONE_BYTE_SIZE:
                    return (size_t)read_uint8();
                case TWO_BYTE_SIZE:
                    return (size_t)read_uint16();
                case FOUR_BYTE_SIZE:
                    return (size_t)read_uint32();
                case EIGHT_BYTE_SIZE:
                    return (size_t)read_uint64();
                default:
                    return (size_t)(control >> 4);
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

            if (!deserialized) throw nullptr;
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

        PyObject * read_sized(uint8_t control) {

            // assert ((control & 0xF) != SizedTypes::DEL);
            size_t size = read_unsigned_number(control);

            switch (control & 0xF) {
                case SizedTypes::UINT: return PyLong_FromLongLong(size);
                case SizedTypes::HANDLE: return read_handle(size);

                case SizedTypes::BYTES: return read_bytes(size);
                case SizedTypes::LIST: return read_list(size);
                case SizedTypes::DICT: return read_dict(size);
                case SizedTypes::TUPLE: return read_tuple(size);
                case SizedTypes::STR: return read_str(size);
                case SizedTypes::PICKLED: return read_pickled(size);
                default:
                    PyErr_Format(PyExc_RuntimeError, "unknown sized type: %i", control & 0xF);
                    throw nullptr;
            } 
        }

        PyObject * read_fixedsize(uint8_t control) {
            assert (!PyErr_Occurred());

            switch (control) {
                case FixedSizeTypes::NONE: return Py_NewRef(Py_None);
                case FixedSizeTypes::TRUE: return Py_NewRef(Py_True);
                case FixedSizeTypes::FALSE: return Py_NewRef(Py_False);
                case FixedSizeTypes::NEG1: return PyLong_FromLong(-1);
                case FixedSizeTypes::FLOAT: return PyFloat_FromDouble(read_float());
                case FixedSizeTypes::INLINE_NEW_HANDLE: return Py_NewRef(store_handle());
                default:
                    raise(SIGTRAP);

                    const char * name = FixedSizeTypes_Name(static_cast<FixedSizeTypes>(control & 0xF));

                    if (name) {
                        PyErr_Format(PyExc_RuntimeError, "unhandled subtype: %s for FixedSized", name);
                    } else {
                        PyErr_Format(PyExc_RuntimeError, "Unknown subtype: %i for FixedSized", control);
                    }
                    assert(PyErr_Occurred());
                    throw nullptr;
            };
        }

        bool is_delete(uint8_t control) {
            return (control & 0xF0) != FIXED_SIZE && (control & 0xF) == SizedTypes::DELETE;
        }

        int64_t read_sized_number(uint8_t control) {
            switch (control & 0xF0) {
                case ONE_BYTE_SIZE:
                    return (int64_t)read_int8();
                case TWO_BYTE_SIZE:
                    return (int64_t)read_int16();
                case FOUR_BYTE_SIZE:
                    return (int64_t)read_int32();
                case EIGHT_BYTE_SIZE:
                    return (int64_t)read_int64();
                default:
                    return (int64_t)((control >> 4) & 0x0F);
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

        bool consume(uint8_t control) {
            if (is_delete(control)) {
                int64_t from_end = read_sized_number(control) + 1;
                assert(from_end <= next_handle);
                uint64_t offset = next_handle - from_end;
                
                assert(lookup.contains(offset));
                Py_DECREF(lookup[offset]);
                lookup.erase(offset);
                return true;
            } else if ((control & 0xF0) == FIXED_SIZE && (control & 0xF) == FixedSizeTypes::NEW_HANDLE) {
                store_handle();
                return true;
            }
            return false;
        }

        uint8_t next_control() {
            uint8_t control = read_uint8();
            
            while (consume(control)) {
                control = read_uint8();
            }
            return control;
        }

        PyObject * read() {
            uint8_t control = next_control();
            
            return (control & 0xF0) == FIXED_SIZE
                ? read_fixedsize(control & 0xF)
                : read_sized(control);
        }

        // static PyObject * py_supply(ObjectReader *self, PyObject * target) {
        //     uint8_t control = self->next_control();

        //     if (control != (0xF0 | FixedSizeTypes::PLACEHOLDER)) {
        //         PyErr_Format(PyExc_RuntimeError, "Expected next element to be a PLACEHOLDER but was...");
        //         return nullptr;
        //     }
        //     self->lookup[self->next_handle++] = Py_NewRef(target);
        //     return Py_NewRef(target);
        // }

        static PyObject* py_vectorcall(ObjectReader* self, PyObject*const * args, size_t nargsf, PyObject* kwnames) {
            if (kwnames || PyVectorcall_NARGS(nargsf) > 0) {
                PyErr_SetString(PyExc_TypeError, "ObjectReader takes no arguments");
                return nullptr;
            }
            try {
                PyObject * result = self->read();
                self->messages_read++;
                return result;
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject* create(PyTypeObject* type, PyObject*, PyObject*) {
            auto* self = reinterpret_cast<ObjectReader *>(type->tp_alloc(type, 0));
            if (!self) return nullptr;

            // Construct the std::set in-place
            new (&self->lookup) map<uint64_t, PyObject*>();
            // new (&self->type_deserializers) map<PyTypeObject *, PyObject*>();

            return reinterpret_cast<PyObject*>(self);
        }

        static int init(ObjectReader * self, PyObject* args, PyObject* kwds) {

            try {
                PyObject * path;
                PyObject * deserializer;
                PyObject * transform = nullptr;

                static const char* kwlist[] = {"path", "deserializer", "transform", nullptr};  // Keywords allowed

                if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO|O", (char **)kwlist, &path, &deserializer, &transform)) {
                    return -1;  
                    // Return NULL to propagate the parsing error
                }

                self->path = Py_NewRef(path);
                self->deserializer = Py_NewRef(deserializer);
                self->bytes_read = self->messages_read = 0;
                // enumtype(enumtype);
                self->next_handle = 0;
                self->transform = transform != Py_None ? Py_XNewRef(transform) : nullptr;

                self->vectorcall = reinterpret_cast<vectorcallfunc>(ObjectReader::py_vectorcall);
                
                self->file = open(path);

            } catch (...) {
                return -1;
            }
            return 0;
        }

        static int traverse(ObjectReader* self, visitproc visit, void* arg) {
            // Py_VISIT(self->m_global_lookup);
            Py_VISIT(self->path);
            Py_VISIT(self->deserializer);

            for (const auto& [key, value] : self->lookup) {
                Py_VISIT(value);
            }
            // Py_VISIT(self->name_cache);
            return 0;
        }

        static int clear(ObjectReader* self) {
            // Py_CLEAR(self->name_cache);
            Py_CLEAR(self->path);
            Py_CLEAR(self->deserializer);

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

        static PyObject * path_getter(ObjectReader *self, void *closure) {
            return Py_NewRef(self->path);
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
    };

    static PyMethodDef methods[] = {
        // {"supply", (PyCFunction)ObjectReader::py_supply, METH_O, "supply the placeholder"},
        // {"intern", (PyCFunction)ObjectWriter::py_intern, METH_FASTCALL, "TODO"},
        // {"replace", (PyCFunction)ObjectWriter::py_replace, METH_VARARGS | METH_KEYWORDS, "TODO"},
        // {"unique", (PyCFunction)ObjectWriter::py_unique, METH_O, "TODO"},
        // {"delete", (PyCFunction)ObjectWriter::py_delete, METH_O, "TODO"},

        // {"tuple", (PyCFunction)ObjectWriter::py_write_tuple, METH_FASTCALL, "TODO"},
        // {"dict", (PyCFunction)ObjectWriter::, METH_FASTCALL | METH_KEYWORDS, "TODO"},

        {NULL}  // Sentinel
    };

    static PyMemberDef members[] = {
        {"bytes_read", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectReader, bytes_read), READONLY, "TODO"},
        {"messages_read", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectReader, messages_read), READONLY, "TODO"},
        // {"path", T_OBJECT, OFFSET_OF_MEMBER(Writer, path), READONLY, "TODO"},
        // {"on_pid_change", T_OBJECT_EX, OFFSET_OF_MEMBER(Writer, on_pid_change), 0, "TODO"},
        {NULL}  /* Sentinel */
    };

    static PyGetSetDef getset[] = {
        {"path", (getter)ObjectReader::path_getter, (setter)ObjectReader::path_setter, "TODO", NULL},
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
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_VECTORCALL,
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