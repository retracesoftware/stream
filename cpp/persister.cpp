#include "stream.h"
#include "writer.h"
#include "queueentry.h"
#include "vendor/SPSCQueue.h"
#include <structmember.h>
#include <thread>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <string>

#ifndef _WIN32
    #include <unistd.h>
    #include <fcntl.h>
    #include <sys/file.h>
    #include <sys/stat.h>
    #include <limits.h>
#endif

#ifndef PIPE_BUF
#define PIPE_BUF 512
#endif

namespace retracesoftware_stream {

    static constexpr size_t FRAME_HEADER_SIZE = 6;
    static constexpr size_t MAX_FRAME_PAYLOAD = PIPE_BUF - FRAME_HEADER_SIZE;
    static constexpr size_t DEFAULT_QUEUE_CAPACITY = 65536;

    // ── SyncPidWriter ────────────────────────────────────────────
    //
    // Lightweight callable that writes PID-framed data synchronously
    // to a file descriptor.  Used as the output_callback for the
    // persister's internal PrimitiveStream.

    struct SyncPidWriter : PyObject {
        int fd;
        uint8_t frame_buf[PIPE_BUF];

        void stamp_pid() {
            uint32_t pid = (uint32_t)::getpid();
            frame_buf[0] = (uint8_t)(pid);
            frame_buf[1] = (uint8_t)(pid >> 8);
            frame_buf[2] = (uint8_t)(pid >> 16);
            frame_buf[3] = (uint8_t)(pid >> 24);
        }

        void write_framed(const uint8_t* data, size_t size) {
            while (size > 0) {
                uint16_t chunk = (uint16_t)std::min(size, MAX_FRAME_PAYLOAD);
                frame_buf[4] = (uint8_t)(chunk);
                frame_buf[5] = (uint8_t)(chunk >> 8);
                memcpy(frame_buf + FRAME_HEADER_SIZE, data, chunk);

                size_t frame_size = FRAME_HEADER_SIZE + chunk;
                while (true) {
                    ssize_t written = ::write(fd, frame_buf, frame_size);
                    if (written < 0) {
                        if (errno == EINTR) continue;
                        perror("SyncPidWriter: write failed");
                        break;
                    }
                    break;
                }
                data += chunk;
                size -= chunk;
            }
        }

        static PyObject* call(SyncPidWriter* self, PyObject* args, PyObject*) {
            PyObject* data;
            if (!PyArg_ParseTuple(args, "O", &data)) return nullptr;

            const void* ptr;
            size_t sz;

            if (PyMemoryView_Check(data)) {
                Py_buffer* view = PyMemoryView_GET_BUFFER(data);
                ptr = view->buf;
                sz = (size_t)view->len;
            } else if (PyBytes_Check(data)) {
                ptr = PyBytes_AS_STRING(data);
                sz = (size_t)PyBytes_GET_SIZE(data);
            } else {
                PyErr_SetString(PyExc_TypeError, "expected memoryview or bytes");
                return nullptr;
            }

            self->write_framed((const uint8_t*)ptr, sz);
            Py_RETURN_NONE;
        }

        static void dealloc(SyncPidWriter* self) {
            Py_TYPE(self)->tp_free((PyObject*)self);
        }
    };

    PyTypeObject SyncPidWriter_Type = {
        .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = MODULE "SyncPidWriter",
        .tp_basicsize = sizeof(SyncPidWriter),
        .tp_dealloc = (destructor)SyncPidWriter::dealloc,
        .tp_call = (ternaryfunc)SyncPidWriter::call,
        .tp_flags = Py_TPFLAGS_DEFAULT,
    };

    // ── AsyncFilePersister ───────────────────────────────────────
    //
    // Owns the SPSC queue consumer side, a MessageStream for
    // serialization, and a background thread that processes entries.
    // ObjectWriter pushes tagged uint64_t entries; the persister
    // thread deserializes objects and writes PID-framed output.

    struct AsyncFilePersister : PyObject {
        int fd;
        bool is_fifo;
        std::thread writer_thread;
        std::atomic<bool> shutdown_flag;
        bool closed;
        bool thread_started;
        std::string stored_path;

        rigtorp::SPSCQueue<uint64_t>* queue;
        MessageStream* stream;
        SyncPidWriter* sync_writer;

        std::atomic<uint64_t> processed_cursor{0};

        // Spin until the next entry appears, then pop and return it.
        uint64_t consume_next() {
            uint64_t* ep;
            while (!(ep = queue->front())) std::this_thread::yield();
            uint64_t v = *ep;
            queue->pop();
            return v;
        }

        PyObject* consume_ptr() {
            return as_ptr(consume_next());
        }

        static void writer_loop(AsyncFilePersister* self) {
            while (true) {
                uint64_t* ep;
                while (!(ep = self->queue->front())) {
                    if (self->shutdown_flag.load(std::memory_order_acquire)) return;
                    std::this_thread::yield();
                }

                PyGILState_STATE gstate = PyGILState_Ensure();

                while ((ep = self->queue->front())) {
                    uint64_t e = *ep;
                    self->queue->pop();

                    if (is_object(e)) {
                        PyObject* obj = as_ptr(e);
                        try {
                            self->stream->write(obj);
                        } catch (...) { PyErr_Clear(); }
                        Py_DECREF(obj);
                    } else {
                        switch (cmd_of(e)) {
                            case CMD_BIND: {
                                PyObject* obj = self->consume_ptr();
                                try { self->stream->bind(obj, false); } catch (...) { PyErr_Clear(); }
                                Py_DECREF(obj);
                                break;
                            }
                            case CMD_EXT_BIND: {
                                PyObject* obj  = self->consume_ptr();
                                PyObject* type = self->consume_ptr();
                                try { self->stream->bind(obj, true); } catch (...) { PyErr_Clear(); }
                                Py_DECREF(obj);
                                Py_DECREF(type);
                                break;
                            }
                            case CMD_NEW_HANDLE: {
                                PyObject* obj = self->consume_ptr();
                                try { self->stream->write_new_handle(obj); } catch (...) { PyErr_Clear(); }
                                Py_DECREF(obj);
                                break;
                            }
                            case CMD_THREAD_SWITCH: {
                                PyObject* obj = self->consume_ptr();
                                try { self->stream->write_thread_switch(obj); } catch (...) { PyErr_Clear(); }
                                Py_DECREF(obj);
                                break;
                            }
                            case CMD_BINDING_DELETE: {
                                PyObject* obj = self->consume_ptr();
                                try { self->stream->object_freed(obj); } catch (...) { PyErr_Clear(); }
                                break;
                            }
                            case CMD_HANDLE_REF:
                                try { self->stream->write_handle_ref_by_index(len_of(e)); } catch (...) { PyErr_Clear(); }
                                break;
                            case CMD_HANDLE_DELETE:
                                try { self->stream->write_handle_delete(len_of(e)); } catch (...) { PyErr_Clear(); }
                                break;
                            case CMD_DROPPED:
                                try { self->stream->write_dropped_marker(len_of(e)); } catch (...) { PyErr_Clear(); }
                                break;
                            case CMD_MESSAGE_BOUNDARY:
                                try { self->stream->mark_message_boundary(); } catch (...) { PyErr_Clear(); }
                                break;
                            case CMD_FLUSH:
                                try { self->stream->flush(); } catch (...) { PyErr_Clear(); }
                                break;
                            case CMD_PICKLED: {
                                PyObject* bytes_obj = self->consume_ptr();
                                try { self->stream->write_pre_pickled(bytes_obj); } catch (...) { PyErr_Clear(); }
                                Py_DECREF(bytes_obj);
                                break;
                            }
                            case CMD_SHUTDOWN:
                                try { self->stream->flush(); } catch (...) { PyErr_Clear(); }
                                self->processed_cursor.fetch_add(1, std::memory_order_release);
                                PyGILState_Release(gstate);
                                return;
                        }
                    }

                    self->processed_cursor.fetch_add(1, std::memory_order_release);
                }

                PyGILState_Release(gstate);
            }
        }

        rigtorp::SPSCQueue<uint64_t>* setup(PyObject* serializer, size_t capacity) {
            if (queue) return queue;

            queue = new rigtorp::SPSCQueue<uint64_t>(capacity);

            sync_writer = (SyncPidWriter*)SyncPidWriter_Type.tp_alloc(&SyncPidWriter_Type, 0);
            if (!sync_writer) {
                delete queue;
                queue = nullptr;
                return nullptr;
            }
            sync_writer->fd = fd;
            sync_writer->stamp_pid();

            stream = new MessageStream((PyObject*)sync_writer, serializer);

            shutdown_flag.store(false, std::memory_order_release);
            writer_thread = std::thread(writer_loop, this);
            thread_started = true;

            return queue;
        }

        void drain_queue_entries() {
            if (!queue) return;
            while (auto* ep = queue->front()) {
                uint64_t e = *ep;
                if (is_object(e)) {
                    Py_DECREF(as_ptr(e));
                } else {
                    uint32_t cmd = cmd_of(e);
                    int follow = 0;
                    bool do_decref = true;
                    switch (cmd) {
                        case CMD_BIND:
                        case CMD_NEW_HANDLE:
                        case CMD_THREAD_SWITCH:
                        case CMD_PICKLED:        follow = 1; break;
                        case CMD_EXT_BIND:       follow = 2; break;
                        case CMD_BINDING_DELETE:  follow = 1; do_decref = false; break;
                        default:                 break;
                    }
                    queue->pop();
                    for (int i = 0; i < follow; i++) {
                        while (!queue->front()) std::this_thread::yield();
                        if (do_decref) Py_DECREF(as_ptr(*queue->front()));
                        queue->pop();
                    }
                    continue;
                }
                queue->pop();
            }
        }

        void do_close() {
            if (closed) return;
            closed = true;

            shutdown_flag.store(true, std::memory_order_release);

            if (writer_thread.joinable()) {
                Py_BEGIN_ALLOW_THREADS
                writer_thread.join();
                Py_END_ALLOW_THREADS
            }
            thread_started = false;

            drain_queue_entries();

            if (stream) {
                delete stream;
                stream = nullptr;
            }
            Py_XDECREF(sync_writer);
            sync_writer = nullptr;

            if (queue) {
                delete queue;
                queue = nullptr;
            }

            if (fd >= 0) {
                ::close(fd);
                fd = -1;
            }
        }

        static PyObject* py_close(AsyncFilePersister* self, PyObject* unused) {
            self->do_close();
            Py_RETURN_NONE;
        }

        void do_drain() {
            if (closed || !thread_started) return;

            shutdown_flag.store(true, std::memory_order_release);

            if (writer_thread.joinable()) {
                Py_BEGIN_ALLOW_THREADS
                writer_thread.join();
                Py_END_ALLOW_THREADS
            }
            thread_started = false;

            shutdown_flag.store(false, std::memory_order_release);
        }

        void do_resume() {
            if (closed || fd < 0 || !queue) return;
            if (sync_writer) sync_writer->stamp_pid();
            shutdown_flag.store(false, std::memory_order_release);
            writer_thread = std::thread(writer_loop, this);
            thread_started = true;
        }

        static PyObject* py_drain(AsyncFilePersister* self, PyObject* unused) {
            self->do_drain();
            Py_RETURN_NONE;
        }

        static PyObject* py_resume(AsyncFilePersister* self, PyObject* unused) {
            self->do_resume();
            Py_RETURN_NONE;
        }

        static PyObject* tp_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
            AsyncFilePersister* self = (AsyncFilePersister*)type->tp_alloc(type, 0);
            if (self) {
                self->fd = -1;
                self->is_fifo = false;
                self->shutdown_flag.store(false);
                self->closed = true;
                self->thread_started = false;
                self->queue = nullptr;
                self->stream = nullptr;
                self->sync_writer = nullptr;
                self->processed_cursor.store(0);
                new (&self->writer_thread) std::thread();
                new (&self->stored_path) std::string();
            }
            return (PyObject*)self;
        }

        static int init(AsyncFilePersister* self, PyObject* args, PyObject* kwds) {
            const char* path;
            int append = 0;

            static const char* kwlist[] = {"path", "append", nullptr};
            if (!PyArg_ParseTupleAndKeywords(args, kwds, "s|p", (char**)kwlist, &path, &append)) {
                return -1;
            }

            struct stat st;
            bool path_is_fifo = (::stat(path, &st) == 0 && S_ISFIFO(st.st_mode));

            int flags = O_WRONLY;
            if (!path_is_fifo) flags |= O_CREAT | (append ? O_APPEND : O_TRUNC);

            self->fd = ::open(path, flags, 0644);
            if (self->fd < 0) {
                PyErr_Format(PyExc_IOError,
                    "Could not open file: %s for writing: %s", path, strerror(errno));
                return -1;
            }

            self->is_fifo = path_is_fifo;

            if (!path_is_fifo) {
                if (flock(self->fd, LOCK_EX | LOCK_NB) == -1) {
                    PyErr_Format(PyExc_IOError,
                        "Could not lock file: %s for exclusive access: %s", path, strerror(errno));
                    ::close(self->fd);
                    self->fd = -1;
                    return -1;
                }
            }

            self->stored_path = path;
            self->closed = false;

            return 0;
        }

        static void dealloc(AsyncFilePersister* self) {
            self->do_close();

            self->stored_path.~basic_string();
            self->writer_thread.~thread();

            Py_TYPE(self)->tp_free((PyObject*)self);
        }
    };

    static PyObject* AsyncFilePersister_path_getter(PyObject* obj, void*) {
        AsyncFilePersister* self = (AsyncFilePersister*)obj;
        return PyUnicode_FromStringAndSize(
            self->stored_path.c_str(), self->stored_path.size());
    }

    static PyObject* AsyncFilePersister_fd_getter(PyObject* obj, void*) {
        return PyLong_FromLong(((AsyncFilePersister*)obj)->fd);
    }

    static PyObject* AsyncFilePersister_is_fifo_getter(PyObject* obj, void*) {
        return PyBool_FromLong(((AsyncFilePersister*)obj)->is_fifo);
    }

    static PyMethodDef AsyncFilePersister_methods[] = {
        {"close", (PyCFunction)AsyncFilePersister::py_close, METH_NOARGS,
         "Flush pending writes, join writer thread, close file"},
        {"drain", (PyCFunction)AsyncFilePersister::py_drain, METH_NOARGS,
         "Drain queue and stop writer thread, keeping the fd open"},
        {"resume", (PyCFunction)AsyncFilePersister::py_resume, METH_NOARGS,
         "Start a new writer thread on the existing fd"},
        {NULL}
    };

    static PyGetSetDef AsyncFilePersister_getset[] = {
        {"path", AsyncFilePersister_path_getter, nullptr, "File path", NULL},
        {"fd", AsyncFilePersister_fd_getter, nullptr, "Underlying file descriptor", NULL},
        {"is_fifo", AsyncFilePersister_is_fifo_getter, nullptr, "True if the output is a named pipe", NULL},
        {NULL}
    };

    void* AsyncFilePersister_setup(PyObject* persister, PyObject* serializer, size_t capacity) {
        if (Py_TYPE(persister) != &AsyncFilePersister_Type) {
            PyErr_SetString(PyExc_TypeError, "expected AsyncFilePersister");
            return nullptr;
        }
        return ((AsyncFilePersister*)persister)->setup(serializer, capacity);
    }

    PyTypeObject AsyncFilePersister_Type = {
        .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = MODULE "AsyncFilePersister",
        .tp_basicsize = sizeof(AsyncFilePersister),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)AsyncFilePersister::dealloc,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Async file persister -- serializes and writes to file on a background thread",
        .tp_methods = AsyncFilePersister_methods,
        .tp_getset = AsyncFilePersister_getset,
        .tp_init = (initproc)AsyncFilePersister::init,
        .tp_new = AsyncFilePersister::tp_new,
    };
}
