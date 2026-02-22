#pragma once
#include <Python.h>
#include <cstdint>

namespace retracesoftware_stream {

    // Tagged uint64_t queue protocol (3-bit tag, 64-bit arch).
    //
    // On 64-bit platforms PyObject* and PyThreadState* are 8-byte
    // aligned, leaving the bottom 3 bits free for tagging.
    //
    //   Tag 0b000  TAG_OBJECT   PyObject* (incref'd, persister serializes)
    //   Tag 0b001  TAG_DELETE   PyObject* identity (no incref, deletion notification)
    //   Tag 0b010  TAG_THREAD   PyThreadState* (no incref, thread identity stamp)
    //   Tag 0b011  TAG_COMMAND  non-pointer: bits[3:31] Cmd enum, bits[32:63] payload
    //   0b100-111  reserved

    static constexpr uint64_t TAG_MASK    = 0x7;
    static constexpr uint64_t TAG_OBJECT  = 0;
    static constexpr uint64_t TAG_DELETE  = 1;
    static constexpr uint64_t TAG_THREAD  = 2;
    static constexpr uint64_t TAG_COMMAND = 3;

    inline uint64_t tag_of(uint64_t e)          { return e & TAG_MASK; }
    inline PyObject* as_ptr(uint64_t e)         { return (PyObject*)(e & ~TAG_MASK); }
    inline PyThreadState* as_tstate(uint64_t e) { return (PyThreadState*)(e & ~TAG_MASK); }

    inline uint64_t obj_entry(PyObject* p)         { return (uint64_t)(uintptr_t)p; }
    inline uint64_t delete_entry(PyObject* p)      { return (uint64_t)(uintptr_t)p | TAG_DELETE; }
    inline uint64_t thread_entry(PyThreadState* t) { return (uint64_t)(uintptr_t)t | TAG_THREAD; }

    inline uint64_t cmd_entry(uint32_t cmd, uint32_t len = 0) {
        return ((uint64_t)len << 32) | ((uint64_t)cmd << 3) | TAG_COMMAND;
    }

    inline uint32_t cmd_of(uint64_t e) { return (uint32_t)((e >> 3) & 0x1FFFFFFFU); }
    inline uint32_t len_of(uint64_t e) { return (uint32_t)(e >> 32); }

    inline int64_t estimate_size(PyObject* obj) {
        if (obj == Py_None || obj == Py_True || obj == Py_False) return 0;
        PyTypeObject* tp = Py_TYPE(obj);
        if (tp == &PyLong_Type)   return 28;
        if (tp == &PyFloat_Type)  return 24;
        if (tp == &PyUnicode_Type)
            return (int64_t)(sizeof(PyObject) + PyUnicode_GET_LENGTH(obj));
        if (tp == &PyBytes_Type)
            return (int64_t)(sizeof(PyObject) + PyBytes_GET_SIZE(obj));
        if (tp == &PyMemoryView_Type) {
            Py_buffer* view = PyMemoryView_GET_BUFFER(obj);
            return (int64_t)(sizeof(PyObject) + view->len);
        }
        return 64;
    }

    enum Cmd : uint32_t {
        CMD_BIND,
        CMD_EXT_BIND,
        CMD_NEW_HANDLE,

        CMD_HANDLE_REF,
        CMD_HANDLE_DELETE,

        CMD_FLUSH,
        CMD_SHUTDOWN,

        CMD_PICKLED,

        CMD_LIST,
        CMD_TUPLE,
        CMD_DICT,
    };

}
