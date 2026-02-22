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
#include <unordered_map>

#ifndef _WIN32
    #include <unistd.h>
    #include <fcntl.h>
    #include <sys/file.h>
    #include <sys/stat.h>
    #include <sys/socket.h>
    #include <sys/un.h>
    #include <limits.h>
#endif

namespace retracesoftware_stream {

    // ── AsyncFilePersister ───────────────────────────────────────
    //
    // Owns the SPSC queue consumer side, a MessageStream for
    // serialization, and a background thread that processes entries.
    // ObjectWriter pushes tagged uint64_t entries; the persister
    // thread deserializes objects and writes PID-framed output.

    struct AsyncFilePersister : PyObject {
        int fd;
        bool is_fifo;
        bool is_socket;
        size_t output_buf_size;
        std::thread writer_thread;
        std::thread return_thread;
        std::atomic<bool> shutdown_flag;
        std::atomic<bool> return_shutdown;
        bool closed;
        bool thread_started;
        std::string stored_path;

        rigtorp::SPSCQueue<uint64_t>* queue;
        rigtorp::SPSCQueue<PyObject*>* return_queue;
        PidFramedOutput* output;
        MessageStream* stream;
        std::atomic<int64_t>* total_removed_ptr;
        PyObject* writer_key;         // thread-local dict key for looking up the thread handle
        PyThreadState* last_tstate;
        std::unordered_map<PyThreadState*, PyObject*>* thread_cache;

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

        void return_obj(PyObject* obj) {
            if (!return_queue->try_push(obj)) {
                Py_DECREF(obj);
            }
        }

        void consume_and_write_value() {
            uint64_t e = consume_next();
            switch (tag_of(e)) {
                case TAG_OBJECT: {
                    PyObject* obj = as_ptr(e);
                    try { stream->write(obj); } catch (...) { PyErr_Clear(); }
                    return_obj(obj);
                    break;
                }
                case TAG_COMMAND:
                    switch (cmd_of(e)) {
                        case CMD_LIST: {
                            uint32_t n = len_of(e);
                            try { stream->write_list_header(n); } catch (...) { PyErr_Clear(); }
                            for (uint32_t i = 0; i < n; i++) consume_and_write_value();
                            break;
                        }
                        case CMD_TUPLE: {
                            uint32_t n = len_of(e);
                            try { stream->write_tuple_header(n); } catch (...) { PyErr_Clear(); }
                            for (uint32_t i = 0; i < n; i++) consume_and_write_value();
                            break;
                        }
                        case CMD_DICT: {
                            uint32_t n = len_of(e);
                            try { stream->write_dict_header(n); } catch (...) { PyErr_Clear(); }
                            for (uint32_t i = 0; i < n; i++) {
                                consume_and_write_value();
                                consume_and_write_value();
                            }
                            break;
                        }
                        case CMD_PICKLED: {
                            PyObject* bytes_obj = consume_ptr();
                            try { stream->write_pre_pickled(bytes_obj); } catch (...) { PyErr_Clear(); }
                            return_obj(bytes_obj);
                            break;
                        }
                        default: break;
                    }
                    break;
                default: break;
            }
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

                    switch (tag_of(e)) {
                        case TAG_OBJECT: {
                            PyObject* obj = as_ptr(e);
                            try { self->stream->write(obj); } catch (...) { PyErr_Clear(); }
                            self->return_obj(obj);
                            break;
                        }
                        case TAG_DELETE: {
                            try { self->stream->object_freed(as_ptr(e)); } catch (...) { PyErr_Clear(); }
                            break;
                        }
                        case TAG_THREAD: {
                            PyThreadState* tstate = as_tstate(e);
                            if (tstate != self->last_tstate) {
                                self->last_tstate = tstate;
                                auto& cache = *self->thread_cache;
                                auto it = cache.find(tstate);
                                PyObject* handle;
                                if (it != cache.end()) {
                                    handle = it->second;
                                } else {
                                    handle = PyDict_GetItem(tstate->dict, self->writer_key);
                                    if (handle) {
                                        Py_INCREF(handle);
                                        cache[tstate] = handle;
                                    }
                                }
                                if (handle) {
                                    try { self->stream->write_thread_switch(handle); }
                                    catch (...) { PyErr_Clear(); }
                                }
                            }
                            break;
                        }
                        case TAG_COMMAND: {
                            switch (cmd_of(e)) {
                                case CMD_BIND: {
                                    PyObject* obj = self->consume_ptr();
                                    try { self->stream->bind(obj, false); } catch (...) { PyErr_Clear(); }
                                    self->return_obj(obj);
                                    break;
                                }
                                case CMD_EXT_BIND: {
                                    PyObject* obj  = self->consume_ptr();
                                    PyObject* type = self->consume_ptr();
                                    try { self->stream->bind(obj, true); } catch (...) { PyErr_Clear(); }
                                    self->return_obj(obj);
                                    self->return_obj(type);
                                    break;
                                }
                                case CMD_NEW_HANDLE: {
                                    PyObject* obj = self->consume_ptr();
                                    try { self->stream->write_new_handle(obj); } catch (...) { PyErr_Clear(); }
                                    self->return_obj(obj);
                                    break;
                                }
                                case CMD_HANDLE_REF:
                                    try { self->stream->write_handle_ref_by_index(len_of(e)); } catch (...) { PyErr_Clear(); }
                                    break;
                                case CMD_HANDLE_DELETE:
                                    try { self->stream->write_handle_delete(len_of(e)); } catch (...) { PyErr_Clear(); }
                                    break;
                                case CMD_FLUSH:
                                    try { self->stream->flush(); } catch (...) { PyErr_Clear(); }
                                    break;
                                case CMD_PICKLED: {
                                    PyObject* bytes_obj = self->consume_ptr();
                                    try { self->stream->write_pre_pickled(bytes_obj); } catch (...) { PyErr_Clear(); }
                                    self->return_obj(bytes_obj);
                                    break;
                                }
                                case CMD_LIST: {
                                    uint32_t n = len_of(e);
                                    try { self->stream->write_list_header(n); } catch (...) { PyErr_Clear(); }
                                    for (uint32_t i = 0; i < n; i++) self->consume_and_write_value();
                                    break;
                                }
                                case CMD_TUPLE: {
                                    uint32_t n = len_of(e);
                                    try { self->stream->write_tuple_header(n); } catch (...) { PyErr_Clear(); }
                                    for (uint32_t i = 0; i < n; i++) self->consume_and_write_value();
                                    break;
                                }
                                case CMD_DICT: {
                                    uint32_t n = len_of(e);
                                    try { self->stream->write_dict_header(n); } catch (...) { PyErr_Clear(); }
                                    for (uint32_t i = 0; i < n; i++) {
                                        self->consume_and_write_value();
                                        self->consume_and_write_value();
                                    }
                                    break;
                                }
                                case CMD_SHUTDOWN:
                                    try { self->stream->flush(); } catch (...) { PyErr_Clear(); }
                                    self->processed_cursor.fetch_add(1, std::memory_order_release);
                                    PyGILState_Release(gstate);
                                    return;
                            }
                            break;
                        }
                    }

                    self->processed_cursor.fetch_add(1, std::memory_order_release);
                }

                try { self->stream->flush(); } catch (...) { PyErr_Clear(); }

                PyGILState_Release(gstate);
            }
        }

        static void drain_loop(AsyncFilePersister* self) {
            while (true) {
                PyObject** ep;
                while (!(ep = self->return_queue->front())) {
                    if (self->return_shutdown.load(std::memory_order_acquire))
                        return;
                    std::this_thread::yield();
                }

                PyGILState_STATE gstate = PyGILState_Ensure();
                auto batch_start = std::chrono::steady_clock::now();
                int deallocs = 0;

                while ((ep = self->return_queue->front())) {
                    PyObject* obj = *ep;
                    self->return_queue->pop();
                    if (self->total_removed_ptr)
                        self->total_removed_ptr->fetch_add(estimate_size(obj), std::memory_order_relaxed);
                    if (Py_REFCNT(obj) == 1) deallocs++;
                    Py_DECREF(obj);
                    if (deallocs >= 32) {
                        deallocs = 0;
                        auto now = std::chrono::steady_clock::now();
                        if (now - batch_start > std::chrono::microseconds(100)) {
                            PyGILState_Release(gstate);
                            std::this_thread::yield();
                            gstate = PyGILState_Ensure();
                            batch_start = std::chrono::steady_clock::now();
                        }
                    }
                }

                PyGILState_Release(gstate);
            }
        }

        SetupResult setup(PyObject* serializer, size_t queue_capacity,
                         size_t return_queue_capacity, std::atomic<int64_t>* total_removed,
                         PyObject* wkey) {
            if (queue) return {queue, return_queue};

            queue = new rigtorp::SPSCQueue<uint64_t>(queue_capacity);
            return_queue = new rigtorp::SPSCQueue<PyObject*>(return_queue_capacity);
            total_removed_ptr = total_removed;
            writer_key = wkey;
            last_tstate = nullptr;
            thread_cache = new std::unordered_map<PyThreadState*, PyObject*>();

            output = new PidFramedOutput(fd, output_buf_size);
            stream = new MessageStream(PidFramedOutput::write, output, serializer);

            shutdown_flag.store(false, std::memory_order_release);
            return_shutdown.store(false, std::memory_order_release);
            writer_thread = std::thread(writer_loop, this);
            return_thread = std::thread(drain_loop, this);
            thread_started = true;

            return {queue, return_queue};
        }

        void drain_value() {
            while (!queue->front()) std::this_thread::yield();
            uint64_t e = *queue->front();
            queue->pop();
            switch (tag_of(e)) {
                case TAG_OBJECT:
                    Py_DECREF(as_ptr(e));
                    break;
                case TAG_COMMAND:
                    switch (cmd_of(e)) {
                        case CMD_LIST:
                        case CMD_TUPLE:
                            for (uint32_t i = 0, n = len_of(e); i < n; i++) drain_value();
                            break;
                        case CMD_DICT:
                            for (uint32_t i = 0, n = len_of(e) * 2; i < n; i++) drain_value();
                            break;
                        case CMD_PICKLED:
                            while (!queue->front()) std::this_thread::yield();
                            Py_DECREF(as_ptr(*queue->front()));
                            queue->pop();
                            break;
                        default: break;
                    }
                    break;
                default: break;
            }
        }

        void drain_queue_entries() {
            if (!queue) return;
            while (auto* ep = queue->front()) {
                uint64_t e = *ep;
                queue->pop();
                switch (tag_of(e)) {
                    case TAG_OBJECT:
                        Py_DECREF(as_ptr(e));
                        break;
                    case TAG_DELETE:
                    case TAG_THREAD:
                        break;
                    case TAG_COMMAND:
                        switch (cmd_of(e)) {
                            case CMD_BIND:
                            case CMD_NEW_HANDLE:
                                while (!queue->front()) std::this_thread::yield();
                                Py_DECREF(as_ptr(*queue->front()));
                                queue->pop();
                                break;
                            case CMD_EXT_BIND:
                                for (int i = 0; i < 2; i++) {
                                    while (!queue->front()) std::this_thread::yield();
                                    Py_DECREF(as_ptr(*queue->front()));
                                    queue->pop();
                                }
                                break;
                            case CMD_PICKLED:
                                while (!queue->front()) std::this_thread::yield();
                                Py_DECREF(as_ptr(*queue->front()));
                                queue->pop();
                                break;
                            case CMD_LIST:
                            case CMD_TUPLE:
                                for (uint32_t i = 0, n = len_of(e); i < n; i++) drain_value();
                                break;
                            case CMD_DICT:
                                for (uint32_t i = 0, n = len_of(e) * 2; i < n; i++) drain_value();
                                break;
                            default: break;
                        }
                        break;
                    default: break;
                }
            }
        }

        void drain_return_queue() {
            if (!return_queue) return;
            while (auto* ep = return_queue->front()) {
                PyObject* obj = *ep;
                return_queue->pop();
                if (total_removed_ptr)
                    total_removed_ptr->fetch_add(estimate_size(obj), std::memory_order_relaxed);
                Py_DECREF(obj);
            }
        }

        void clear_thread_cache() {
            if (!thread_cache) return;
            for (auto& kv : *thread_cache)
                Py_DECREF(kv.second);
            thread_cache->clear();
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

            return_shutdown.store(true, std::memory_order_release);

            if (return_thread.joinable()) {
                Py_BEGIN_ALLOW_THREADS
                return_thread.join();
                Py_END_ALLOW_THREADS
            }

            thread_started = false;

            drain_return_queue();
            drain_queue_entries();

            if (stream) {
                delete stream;
                stream = nullptr;
            }
            if (output) {
                delete output;
                output = nullptr;
            }

            clear_thread_cache();
            if (thread_cache) {
                delete thread_cache;
                thread_cache = nullptr;
            }

            if (queue) {
                delete queue;
                queue = nullptr;
            }
            if (return_queue) {
                delete return_queue;
                return_queue = nullptr;
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

            return_shutdown.store(true, std::memory_order_release);

            if (return_thread.joinable()) {
                Py_BEGIN_ALLOW_THREADS
                return_thread.join();
                Py_END_ALLOW_THREADS
            }

            drain_return_queue();
            thread_started = false;

            shutdown_flag.store(false, std::memory_order_release);
            return_shutdown.store(false, std::memory_order_release);
        }

        void do_resume() {
            if (closed || fd < 0 || !queue) return;
            if (output) output->stamp_pid();
            last_tstate = nullptr;
            clear_thread_cache();
            shutdown_flag.store(false, std::memory_order_release);
            return_shutdown.store(false, std::memory_order_release);
            writer_thread = std::thread(writer_loop, this);
            return_thread = std::thread(drain_loop, this);
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
                self->is_socket = false;
                self->output_buf_size = PIPE_BUF;
                self->shutdown_flag.store(false);
                self->return_shutdown.store(false);
                self->closed = true;
                self->thread_started = false;
                self->queue = nullptr;
                self->return_queue = nullptr;
                self->output = nullptr;
                self->stream = nullptr;
                self->total_removed_ptr = nullptr;
                self->writer_key = nullptr;
                self->last_tstate = nullptr;
                self->thread_cache = nullptr;
                self->processed_cursor.store(0);
                new (&self->writer_thread) std::thread();
                new (&self->return_thread) std::thread();
                new (&self->stored_path) std::string();
            }
            return (PyObject*)self;
        }

        static size_t query_socket_sndbuf(int fd) {
            int sndbuf = 0;
            socklen_t len = sizeof(sndbuf);
            if (getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &sndbuf, &len) == 0 && sndbuf > 0) {
                return (size_t)sndbuf;
            }
            return 65536;
        }

        static int init(AsyncFilePersister* self, PyObject* args, PyObject* kwds) {
            const char* path;
            int append = 0;

            static const char* kwlist[] = {"path", "append", nullptr};
            if (!PyArg_ParseTupleAndKeywords(args, kwds, "s|p", (char**)kwlist, &path, &append)) {
                return -1;
            }

            struct stat st;
            bool path_is_fifo = false;
            bool path_is_socket = false;

            if (::stat(path, &st) == 0) {
                path_is_fifo = S_ISFIFO(st.st_mode);
                path_is_socket = S_ISSOCK(st.st_mode);
            }

            if (path_is_socket) {
                int sock = ::socket(AF_UNIX, SOCK_DGRAM, 0);
                if (sock < 0) {
                    PyErr_Format(PyExc_IOError,
                        "Could not create socket for: %s: %s", path, strerror(errno));
                    return -1;
                }

                struct sockaddr_un addr;
                memset(&addr, 0, sizeof(addr));
                addr.sun_family = AF_UNIX;
                strncpy(addr.sun_path, path, sizeof(addr.sun_path) - 1);

                if (::connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
                    PyErr_Format(PyExc_IOError,
                        "Could not connect to socket: %s: %s", path, strerror(errno));
                    ::close(sock);
                    return -1;
                }

                self->fd = sock;
                self->is_fifo = false;
                self->is_socket = true;
                self->output_buf_size = query_socket_sndbuf(sock);

            } else {
                int flags = O_WRONLY;
                if (!path_is_fifo) flags |= O_CREAT | (append ? O_APPEND : O_TRUNC);

                self->fd = ::open(path, flags, 0644);
                if (self->fd < 0) {
                    PyErr_Format(PyExc_IOError,
                        "Could not open file: %s for writing: %s", path, strerror(errno));
                    return -1;
                }

                self->is_fifo = path_is_fifo;
                self->is_socket = false;
                self->output_buf_size = path_is_fifo ? PIPE_BUF : 65536;

                if (!path_is_fifo) {
                    if (flock(self->fd, LOCK_EX | LOCK_NB) == -1) {
                        PyErr_Format(PyExc_IOError,
                            "Could not lock file: %s for exclusive access: %s", path, strerror(errno));
                        ::close(self->fd);
                        self->fd = -1;
                        return -1;
                    }
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
            self->return_thread.~thread();

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

    SetupResult AsyncFilePersister_setup(PyObject* persister, PyObject* serializer,
                                         size_t queue_capacity,
                                         size_t return_queue_capacity,
                                         std::atomic<int64_t>* total_removed,
                                         PyObject* writer_key) {
        if (Py_TYPE(persister) != &AsyncFilePersister_Type) {
            PyErr_SetString(PyExc_TypeError, "expected AsyncFilePersister");
            return {nullptr, nullptr};
        }
        return ((AsyncFilePersister*)persister)->setup(serializer, queue_capacity,
                                                       return_queue_capacity, total_removed,
                                                       writer_key);
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
