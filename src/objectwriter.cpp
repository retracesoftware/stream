#include "stream.h"
#include <structmember.h>
#include "wireformat.h"
#include <algorithm>
#include <fcntl.h>
#include <sstream>
#include <sys/file.h>
#include "unordered_dense.h"

using namespace ankerl::unordered_dense;

namespace retracesoftware_stream {

    struct StreamHandle : public PyObject {
        int index;
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

    struct ObjectWriter : public PyObject {
        
        FILE * file;
        size_t bytes_written;
        size_t messages_written;
        int next_handle;

        map<PyObject *, uint64_t> placeholders;
        // map<PyTypeObject *, retracesoftware::FastCall> type_serializers;

        // PyTypeObject * enumtype;

        int pid;

        // 1. add a type serializer for target type
        // 2. tests the intern value for resultant type serializer
        // serializer can return 

        // map<PyObject *, int> lookup;
        // map<PyTypeObject *, std::pair<vectorcallfunc, PyObject *> type_serializers;

        // PyObject * fast_lookup[5];
        PyObject * serializer;
        PyObject * path;
        vectorcallfunc vectorcall;

        static PyObject * StreamHandle_vectorcall(StreamHandle * self, PyObject *const * args, size_t nargsf, PyObject* kwnames) {
            
            try {
                ObjectWriter * writer = reinterpret_cast<ObjectWriter *>(self->writer);

                writer->write_handle_ref(self->index);

                size_t total_args = PyVectorcall_NARGS(nargsf) + (kwnames ? PyTuple_GET_SIZE(kwnames) : 0);

                for (size_t i = 0; i < total_args; i++) {
                    writer->write(args[i]);
                }

                return Py_NewRef(total_args == 1 ? args[0] : Py_None);

            } catch (...) {
                return nullptr;
            }
        }

        void write_delete(int id) {
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

        PyObject * stream_handle(int index) {

            StreamHandle * self = (StreamHandle *)StreamHandle_Type.tp_alloc(&StreamHandle_Type, 0);
            if (!self) return nullptr;

            self->writer = Py_NewRef(this);
            self->index = index;
            self->vectorcall = (vectorcallfunc)StreamHandle_vectorcall;
            
            return (PyObject *)self;
        }

        PyObject * handle(PyObject * obj, bool lookup) {
            write(lookup ? FixedSizeTypes::INLINE_NEW_HANDLE : FixedSizeTypes::NEW_HANDLE);
            write(obj);
            return stream_handle(next_handle++);
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
            write_size(SizedTypes::BYTES, PyBytes_GET_SIZE(obj));
            write_bytes_data(obj);
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

        void write_ref(PyObject * reference) {
            write(FixedSizeTypes::REF);
            write_pointer((void *)reference);
        }

        // void write_reference(PyObject * reference) {
        //     assert(Py_TYPE(reference) == &Reference_Type);
        //     write_ref((PyObject *)Reference_GetPointer(reference));
        // }

        // PyObject * fullname(PyObject * obj) {
        //     static PyObject * __module__ = nullptr;
        //     if (!__module__) __module__ = PyUnicode_InternFromString("__module__");

        //     static PyObject * __name__ = nullptr;
        //     if (!__name__) __name__ = PyUnicode_InternFromString("__name__");

        //     auto m = PyObject_GetAttr(obj, __module__);

        //     if (!m) {
        //         throw std::exception();
        //     }

        //     auto n = PyObject_GetAttr(obj, __name__);

        //     if (!n) {
        //         Py_DECREF(m);
        //         throw std::exception();
        //     }
        //     PyObject * fullname = PyTuple_Pack(2, m, n);
        //     return fullname;
        // }
        
        // static PyObject * target_typename() {
        //     static PyObject * name = nullptr;
        //     if (!name) name = PyUnicode_InternFromString("__retrace_target_typename__");
        //     return name;
        // }

        // void write_int_proxy(PyObject * proxy) {
        //     write_control(FixedSizeTypes::REF);
        //     write_pointer(proxy);
        // }

        // void write_stableset(PyObject * obj) {
        //     write_size(SizedTypes::SET, PySet_GET_SIZE(obj));
            
        //     for (int i = 0; i < PySet_GET_SIZE(obj); i++) {
        //         operator()(StableSet_GetItem(obj, i));
        //     }
        // }

        // void write_stablefrozenset(PyObject * obj) {
        //     write_size(SizedTypes::FROZENSET, PySet_GET_SIZE(obj));
            
        //     for (int i = 0; i < PySet_GET_SIZE(obj); i++) {
        //         operator()(StableSet_GetItem(obj, i));
        //     }
        // }

        // void write_enum(PyObject * obj) {

        //     PyTypeObject * cls = Py_TYPE(obj);

        //     static PyObject * module_attr = nullptr;
        //     if (!module_attr) module_attr = PyUnicode_InternFromString("__module__");

        //     assert(cls->tp_dict);

        //     PyObject * module_name = PyDict_GetItem(cls->tp_dict, module_attr);

        //     assert(module_name);

        //     static PyObject * name_attr = nullptr;
        //     if (!name_attr) name_attr = PyUnicode_InternFromString("name");

        //     PyObject * name = PyObject_GetAttr(obj, name_attr);

        //     if (!name) {
        //         raise(SIGTRAP);
        //         throw std::exception();
        //     }

        //     write(FixedSizeTypes::GLOBAL);
        //     write_tuple_header(3);
        //     operator()(module_name);
        //     operator()(cls->tp_name);
        //     operator()(name);

        //     Py_DECREF(name);
        // }

        // void write_type(PyObject * obj) {

        //     PyTypeObject * cls = reinterpret_cast<PyTypeObject *>(obj);

        //     assert(!PyType_IsSubtype(cls, &Proxy_Type));
        //     write_control(FixedSizeTypes::GLOBAL);

        //     auto mod = PyObjectPtr(PyObject_GetAttrString(obj, "__module__"));
        //     auto name = PyObjectPtr(PyObject_GetAttrString(obj, "__name__"));

        //     write_tuple_header(2);
        //     write(mod.get());
        //     write(name.get());
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

        inline void write(FixedSizeTypes obj) {
            uint8_t control = (uint8_t)obj | FIXED_SIZE;

            write(control);
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

        void write_control(uint8_t value) {
            write(value);
        }

        void write_size(SizedTypes type, Py_ssize_t size) {
            assert (type < 16);

            if (size < 11) {
                write_control(type | (size << 4));
            } else {
                if (size < UINT8_MAX) {
                    write_control(type | ONE_BYTE_SIZE);
                    write((int8_t)size);
                } else if (size < UINT16_MAX) { 
                    write_control(type | TWO_BYTE_SIZE);
                    write((int16_t)size);
                } else if (size < UINT32_MAX) { 
                    write_control(type | FOUR_BYTE_SIZE);
                    write((int32_t)size);
                } else {
                    write_control(type | EIGHT_BYTE_SIZE);
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

        void write_unsigned_number(SizedTypes typ, uint64_t l) {
            if (l < 11) {
                write_control(typ | (l << 4));
            }
            else if (l <= UINT8_MAX) {
                write_control(ONE_BYTE_SIZE | typ);
                write((uint8_t)l);
            }
            else if (l <= UINT16_MAX) {
                write_control(TWO_BYTE_SIZE | typ);
                write((uint16_t)l);
            }
            else if (l <= UINT32_MAX) {
                write_control(FOUR_BYTE_SIZE | typ);
                write((uint32_t)l);
            }
            else {
                write_control(EIGHT_BYTE_SIZE | typ);
                write(l);
            }
        }

        void write_sized_int(int64_t l) {
            if (l == -1) {
                write_control(FIXED_SIZE | FixedSizeTypes::NEG1);
            } else if (l < -1 || l > INT32_MAX) {
                write_control(FIXED_SIZE | FixedSizeTypes::INT64);
                write(l);
            } else {
                write_unsigned_number(SizedTypes::UINT, l);
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

        void write_handle_ref(int handle) {
            write_unsigned_number(SizedTypes::HANDLE, handle);
        }

        void write_stream_handle(PyObject * obj) {
            assert (Py_TYPE(obj) == &StreamHandle_Type);

            StreamHandle * handle = reinterpret_cast<StreamHandle *>(obj);

            write_handle_ref(handle->index);
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

        void write(PyObject * obj) {

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
            // else if (Py_TYPE(obj) == &GlobalRef_Type) write_global_ref(obj);
            // else if (Py_TYPE(obj) == &PyType_Type) ;
            // else if (Py_TYPE(obj) == &Pickled_Type) write_pickled(obj);
            else if (Py_TYPE(obj) == &PyFloat_Type) write_float(obj);

            else if (placeholders.contains(obj)) write_handle_ref(placeholders[obj]);

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
            for (size_t i = 0; i < nargs; i++) {
                write(args[i]);
                messages_written++;
            }
        }

        static PyObject* py_vectorcall(ObjectWriter* self, PyObject*const * args, size_t nargsf, PyObject* kwnames) {
            if (kwnames) {
                PyErr_SetString(PyExc_TypeError, "ObjectWriter does not accept keyword arguments");
                return nullptr;
            }

            try {
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
                return Py_NewRef(nargs == 1 ? args[0] : Py_None);

            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * py_write(ObjectWriter * self, PyObject* obj) {
            try {
                return self->handle(obj, true);
            } catch (...) {
                return nullptr;
            }
        }

        static PyObject * WeakRefCallback_vectorcall(WeakRefCallback * self, PyObject *const * args, size_t nargsf, PyObject* kwnames) {
            
            ObjectWriter * writer = reinterpret_cast<ObjectWriter *>(self->writer);

            assert(writer->placeholders.contains(self->handle));

            writer->write_delete(writer->placeholders[self->handle]);
            writer->placeholders.erase(self->handle);

            Py_DECREF(args[0]);
            Py_RETURN_NONE;
        }

        PyObject * weakref_callback(PyObject* handle) {
            WeakRefCallback * self = (WeakRefCallback *)WeakRefCallback_Type.tp_alloc(&WeakRefCallback_Type, 0);
            if (!self) return nullptr;

            self->writer = Py_NewRef(this);
            self->handle = handle;
            self->vectorcall = (vectorcallfunc)WeakRefCallback_vectorcall;
            
            return (PyObject *)self;
        }

        static PyObject * py_placeholder(ObjectWriter * self, PyObject* obj) {

            PyObject * callback = self->weakref_callback(obj);

            if (!PyWeakref_NewRef(obj, callback)) {
                Py_DECREF(callback);
                return nullptr;
            }
            Py_DECREF(callback);
            
            self->write(FixedSizeTypes::PLACEHOLDER);
            self->placeholders[obj] = self->next_handle++;

            Py_RETURN_NONE;
        }

        static PyObject * py_handle(ObjectWriter * self, PyObject* obj) {
            try {
                return self->handle(obj, false);
            } catch (...) {
                return nullptr;
            }
        }

        // static PyObject * py_add_type_serializer(ObjectWriter * self, PyObject* args, PyObject* kwds) {
        //     PyTypeObject * cls;
        //     PyObject * serializer;

        //     static const char* kwlist[] = {"cls", "serializer", nullptr};  // Keywords allowed

        //     if (!PyArg_ParseTupleAndKeywords(args, kwds, "O!O", (char **)kwlist, &PyType_Type, &cls, &serializer)) {
        //         return nullptr;  
        //         // Return NULL to propagate the parsing error
        //     }
        //     if (self->type_serializers.contains(cls)) {
        //         PyErr_Format(PyExc_RuntimeError, "Could not add serializer for type: %S as serializer for given type already exists", cls);
        //         return nullptr;
        //     }
        //     Py_INCREF(cls);

        //     self->type_serializers[cls] = retracesoftware::FastCall(Py_NewRef(serializer));
        //     assert(self->type_serializers[cls].vectorcall);
        //     assert(self->type_serializers[cls].callable);

        //     Py_RETURN_NONE;
        // }

        static int init(ObjectWriter * self, PyObject* args, PyObject* kwds) {

            PyObject * path;
            PyObject * serializer;

            static const char* kwlist[] = {"path", "serializer", nullptr};  // Keywords allowed

            if (!PyArg_ParseTupleAndKeywords(args, kwds, "OO", (char **)kwlist, &path, &serializer)) {
                return -1;  
                // Return NULL to propagate the parsing error
            }

            self->path = Py_NewRef(path);
            self->serializer = Py_NewRef(serializer);

            self->bytes_written = self->messages_written = 0;
            // enumtype(enumtype);
            self->next_handle = 0;
            
            new (&self->placeholders) map<PyObject *, uint64_t>();
            // new (&self->type_serializers) map<PyTypeObject *, retracesoftware::FastCall>();

            self->file = open(path, false);
            self->vectorcall = reinterpret_cast<vectorcallfunc>(ObjectWriter::py_vectorcall);

            return 0;
        }

        static int traverse(ObjectWriter* self, visitproc visit, void* arg) {
            // Py_VISIT(self->m_global_lookup);
            Py_VISIT(self->path);
            Py_VISIT(self->serializer);

            return 0;
        }

        static int clear(ObjectWriter* self) {
            // Py_CLEAR(self->name_cache);
            Py_CLEAR(self->path);
            Py_CLEAR(self->serializer);

            return 0;
        }

        static void dealloc(ObjectWriter* self) {
            if (self->file) {
                fclose(self->file);
                self->file = nullptr;
            }
            
            PyObject_GC_UnTrack(self);
            clear(self);

            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
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

    PyTypeObject WeakRefCallback_Type = {
        .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = MODULE "WeakRefCallback",
        .tp_basicsize = sizeof(WeakRefCallback),
        .tp_itemsize = 0,
        .tp_dealloc = generic_gc_dealloc,
        .tp_vectorcall_offset = OFFSET_OF_MEMBER(WeakRefCallback, vectorcall),
        .tp_call = PyVectorcall_Call,
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_HAVE_VECTORCALL,
        .tp_doc = "TODO",
        .tp_traverse = (traverseproc)WeakRefCallback::traverse,
        .tp_clear = (inquiry)WeakRefCallback::clear,
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
    };

    static PyMethodDef methods[] = {
        // {"add_type_serializer", (PyCFunction)ObjectWriter::py_add_type_serializer, METH_VARARGS | METH_KEYWORDS, "Creates handle"},
        {"placeholder", (PyCFunction)ObjectWriter::py_placeholder, METH_O, "Creates handle"},
        {"handle", (PyCFunction)ObjectWriter::py_handle, METH_O, "Creates handle"},
        {"write", (PyCFunction)ObjectWriter::py_write, METH_O, "Write's object returning a handle for future writes"},
        // {"unique", (PyCFunction)ObjectWriter::py_unique, METH_O, "TODO"},
        // {"delete", (PyCFunction)ObjectWriter::py_delete, METH_O, "TODO"},

        // {"tuple", (PyCFunction)ObjectWriter::py_write_tuple, METH_FASTCALL, "TODO"},
        // {"dict", (PyCFunction)ObjectWriter::, METH_FASTCALL | METH_KEYWORDS, "TODO"},
        {NULL}  // Sentinel
    };

    static PyMemberDef members[] = {
        {"bytes_written", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectWriter, bytes_written), READONLY, "TODO"},
        {"messages_written", T_ULONGLONG, OFFSET_OF_MEMBER(ObjectWriter, messages_written), READONLY, "TODO"},
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
        .tp_call = PyVectorcall_Call,
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_BASETYPE | Py_TPFLAGS_HAVE_VECTORCALL,
        .tp_doc = "TODO",
        .tp_traverse = (traverseproc)ObjectWriter::traverse,
        .tp_clear = (inquiry)ObjectWriter::clear,
        .tp_methods = methods,
        .tp_members = members,
        .tp_getset = getset,
        .tp_init = (initproc)ObjectWriter::init,
        .tp_new = PyType_GenericNew,
    };
}