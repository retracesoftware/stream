#pragma once
#include <Python.h>
#include <cstdint>

namespace retracesoftware_stream {

    // Tagged uint64_t queue protocol.
    //
    // Bit 0 = 0: entry is a PyObject* (incref'd, persister serializes).
    // Bit 0 = 1: entry is a command word.
    //            bits [1:31]  = Cmd enum
    //            bits [32:63] = length / integer payload

    enum Cmd : uint32_t {
        CMD_BIND,
        CMD_EXT_BIND,
        CMD_NEW_HANDLE,
        CMD_THREAD_SWITCH,
        CMD_BINDING_DELETE,

        CMD_HANDLE_REF,
        CMD_HANDLE_DELETE,
        CMD_DROPPED,

        CMD_MESSAGE_BOUNDARY,
        CMD_FLUSH,
        CMD_SHUTDOWN,

        CMD_PICKLED,
    };

    inline uint64_t obj_entry(PyObject* p) {
        return (uint64_t)(uintptr_t)p;
    }

    inline uint64_t cmd_entry(uint32_t cmd, uint32_t len = 0) {
        return ((uint64_t)len << 32) | ((uint64_t)cmd << 1) | 1;
    }

    inline bool      is_object(uint64_t e) { return (e & 1) == 0; }
    inline PyObject*  as_ptr(uint64_t e)   { return (PyObject*)(uintptr_t)e; }
    inline uint32_t   cmd_of(uint64_t e)   { return (uint32_t)((e >> 1) & 0x7FFFFFFFU); }
    inline uint32_t   len_of(uint64_t e)   { return (uint32_t)(e >> 32); }

}
