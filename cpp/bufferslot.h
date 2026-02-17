#pragma once
#include <Python.h>
#include <atomic>
#include <cstdint>
#include <cstring>

namespace retracesoftware_stream {

    static constexpr size_t BUFFER_SLOT_SIZE = 65536;

    extern PyTypeObject BufferSlot_Type;

    struct BufferSlot : PyObject {
        std::atomic<bool> in_use;
        size_t used;
        uint64_t message_count;
        alignas(64) uint8_t data[BUFFER_SLOT_SIZE];

        static int getbuffer(BufferSlot* self, Py_buffer* view, int flags) {
            if (view == nullptr) {
                PyErr_SetString(PyExc_BufferError, "NULL Py_buffer pointer");
                return -1;
            }
            self->in_use.store(true, std::memory_order_release);
            return PyBuffer_FillInfo(view, (PyObject*)self,
                                    self->data, (Py_ssize_t)self->used,
                                    1 /* readonly */, flags);
        }

        static void releasebuffer(BufferSlot* self, Py_buffer* view) {
            self->in_use.store(false, std::memory_order_release);
        }

        static void dealloc(BufferSlot* self) {
            Py_TYPE(self)->tp_free((PyObject*)self);
        }
    };

    static PyBufferProcs BufferSlot_as_buffer = {
        .bf_getbuffer = (getbufferproc)BufferSlot::getbuffer,
        .bf_releasebuffer = (releasebufferproc)BufferSlot::releasebuffer,
    };
}
