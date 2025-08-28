#pragma once
#include <Python.h>
// #include <nanobind/nanobind.h>

#include <functional>
#include <signal.h>
#include <string>
#include <vector>
#include <tuple>

#include "fastcall.h"

#define MODULE "retracesoftware_stream."

#define OFFSET_OF_MEMBER(type, member) \
    ((Py_ssize_t) &reinterpret_cast<const volatile char&>((((type*)0)->member)))

#define SMALL_ARGS 5

// namespace nb = nanobind;

namespace retracesoftware_stream {

    extern PyTypeObject ObjectWriter_Type;
    extern PyTypeObject ObjectReader_Type;
    extern PyTypeObject StreamHandle_Type;
    extern PyTypeObject WeakRefCallback_Type;
    // extern PyTypeObject ObjectReader_Type;

    inline void generic_gc_dealloc(PyObject *self) {

        PyObject_GC_UnTrack(self);

        // if (Py_TYPE(self)->tp_dealloc == generic_gc_dealloc) {
        //     PyObject_GC_UnTrack(self);
        // }
        // else {
        //     raise(SIGTRAP);
        // }

        PyTypeObject *tp = Py_TYPE(self);

        if (tp->tp_weaklistoffset) {
            PyObject_ClearWeakRefs(self);
        }

        if (tp->tp_clear) {
            tp->tp_clear(self);
        }
        tp->tp_free(self);
    }

    class PyObjectPtr {
        PyObject * ptr;
    public:
        inline PyObjectPtr(PyObject * obj) : ptr(obj) {}

        inline PyObject * get() const { return ptr; }
        
        inline PyObject * get_or_none() const { return ptr ? ptr : Py_None; }

        inline PyTypeObject * type() const {
            return Py_TYPE(ptr);
        }

        inline void assert_exact_type(PyTypeObject * cls) {
            if (Py_TYPE(ptr) != cls) {
                PyErr_Format(PyExc_RuntimeError, "Object: %s, was not expected type: %S", ptr, cls);
                throw std::exception();    
            }
        }

        // inline PyObject * resolve() {
        //     auto modname = PyObjectPtr(get_attr("__module__"));
        //     auto name = PyObjectPtr(get_attr("__name__"));

        //     return ::resolve(modname.get(), name.get());
        // }

        inline PyObject * get_attr(const char * name) const {
            PyObject * result = PyObject_GetAttrString(ptr, name);
            if (!result) {
                assert(PyErr_Occurred());
                throw std::exception();
            }
            return result;
        }

        inline PyObject * get_attr(PyObjectPtr &name) const {
            PyObject * result = PyObject_GetAttr(ptr, name.ptr);
            if (!result) {
                assert(PyErr_Occurred());
                throw std::exception();
            }
            return result;
        }

        inline ~PyObjectPtr() {
            Py_XDECREF(ptr);
        }
    };
}
