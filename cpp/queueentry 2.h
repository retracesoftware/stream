#pragma once
#include <Python.h>
#include <cstdint>

#if PY_VERSION_HEX >= 0x030C0000
    inline bool is_immortal(PyObject* obj) { return _Py_IsImmortal(obj); }
#else
    inline bool is_immortal(PyObject*) { return false; }
#endif

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
    //   Tag 0b100  TAG_PICKLED     PyObject* (incref'd bytes, persister writes pre-pickled)
    //   Tag 0b101  TAG_NEW_HANDLE  PyObject* (incref'd, persister registers new handle)
    //   Tag 0b110  TAG_BIND        PyObject* (incref'd, persister binds type)
    //   Tag 0b111  TAG_EXT_BIND    PyObject* (incref'd, persister binds external type)

    static constexpr uint64_t TAG_MASK    = 0x7;
    static constexpr uint64_t TAG_OBJECT  = 0;
    static constexpr uint64_t TAG_DELETE  = 1;
    static constexpr uint64_t TAG_THREAD  = 2;
    static constexpr uint64_t TAG_PICKLED    = 3;
    static constexpr uint64_t TAG_NEW_HANDLE = 4;
    static constexpr uint64_t TAG_BIND       = 5;
    static constexpr uint64_t TAG_EXT_BIND   = 6;
    static constexpr uint64_t TAG_COMMAND = 7;

    inline uint64_t tag_of(uint64_t e)          { return e & TAG_MASK; }
    inline PyObject* as_ptr(uint64_t e)         { return (PyObject*)(e & ~TAG_MASK); }
    inline PyThreadState* as_tstate(uint64_t e) { return (PyThreadState*)(e & ~TAG_MASK); }

    inline uint64_t obj_entry(PyObject* p)         { return (uint64_t)(uintptr_t)p; }
    inline uint64_t delete_entry(PyObject* p)      { return (uint64_t)(uintptr_t)p | TAG_DELETE; }
    inline uint64_t thread_entry(PyThreadState* t) { return (uint64_t)(uintptr_t)t | TAG_THREAD; }
    inline uint64_t pickled_entry(PyObject* p)      { return (uint64_t)(uintptr_t)p | TAG_PICKLED; }
    inline uint64_t new_handle_entry(PyObject* p)  { return (uint64_t)(uintptr_t)p | TAG_NEW_HANDLE; }
    inline uint64_t bind_entry(PyObject* p)        { return (uint64_t)(uintptr_t)p | TAG_BIND; }
    inline uint64_t ext_bind_entry(PyObject* p)    { return (uint64_t)(uintptr_t)p | TAG_EXT_BIND; }

    inline uint64_t cmd_entry(uint32_t cmd, uint32_t len = 0) {
        return ((uint64_t)len << 32) | ((uint64_t)cmd << 3) | TAG_COMMAND;
    }

    inline uint32_t cmd_of(uint64_t e) { return (uint32_t)((e >> 3) & 0x1FFFFFFFU); }
    inline uint32_t len_of(uint64_t e) { return (uint32_t)(e >> 32); }

    inline int64_t estimate_long_size(PyObject* obj) {
        return 28;
    }

    inline int64_t estimate_float_size(PyObject* obj) {
        return 24;
    }

    inline int64_t estimate_unicode_size(PyObject* obj) {
        return (int64_t)(sizeof(PyObject) + PyUnicode_GET_LENGTH(obj) * PyUnicode_KIND(obj));
    }

    inline int64_t estimate_bytes_size(PyObject* obj) {
        return (int64_t)(sizeof(PyObject) + PyBytes_GET_SIZE(obj));
    }

    inline int64_t estimate_memory_view_size(PyObject* obj) {
        return (int64_t)(sizeof(PyObject) + PyMemoryView_GET_BUFFER(obj)->len);
    }

    inline int64_t estimate_stream_handle_size(PyObject* obj) {
        return 64;
    }

    inline int64_t estimate_size(PyObject* obj) {
        if (is_immortal(obj)) return 0;
        PyTypeObject* tp = Py_TYPE(obj);
        if (tp == &PyLong_Type)    return estimate_long_size(obj);
        if (tp == &PyFloat_Type)   return estimate_float_size(obj);
        if (tp == &PyUnicode_Type) return estimate_unicode_size(obj);
        if (tp == &PyBytes_Type)   return estimate_bytes_size(obj);
        if (tp == &PyMemoryView_Type) return estimate_memory_view_size(obj);
        if (tp == &StreamHandle_Type) return estimate_stream_handle_size(obj);
        if (is_patched(tp->tp_free)) return 64;
        return -1;
    }

    enum Cmd : uint32_t {
        CMD_HANDLE_REF,
        CMD_HANDLE_DELETE,

        CMD_FLUSH,
        CMD_SHUTDOWN,

        CMD_LIST,
        CMD_TUPLE,
        CMD_DICT,
        CMD_HEARTBEAT,
    };

}
