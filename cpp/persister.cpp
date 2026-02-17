#include "stream.h"
#include <structmember.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <cerrno>
#include <cstring>
#include <string>

#ifndef _WIN32
    #include <unistd.h>
    #include <fcntl.h>
    #include <sys/file.h>
#endif

namespace retracesoftware_stream {

    struct AsyncFilePersister : PyObject {
        int fd;
        std::thread writer_thread;
        std::mutex mtx;
        std::condition_variable cv;

        struct WriteItem {
            const void* data;
            size_t size;
            PyObject* memoryview;
        };

        std::deque<WriteItem> queue;
        bool shutdown;
        bool closed;
        std::string stored_path;

        static void writer_loop(AsyncFilePersister* self) {
            while (true) {
                WriteItem item;
                {
                    std::unique_lock<std::mutex> lock(self->mtx);
                    self->cv.wait(lock, [self] {
                        return !self->queue.empty() || self->shutdown;
                    });

                    if (self->shutdown && self->queue.empty()) break;

                    item = self->queue.front();
                    self->queue.pop_front();
                }

                // Write to file -- no GIL needed, pure syscall
                const uint8_t* ptr = (const uint8_t*)item.data;
                size_t remaining = item.size;
                while (remaining > 0) {
                    ssize_t written = ::write(self->fd, ptr, remaining);
                    if (written < 0) {
                        if (errno == EINTR) continue;
                        perror("AsyncFilePersister: write failed");
                        break;
                    }
                    ptr += written;
                    remaining -= written;
                }

                // Release the memoryview (needs GIL).
                // This triggers bf_releasebuffer on the BufferSlot, clearing in_use.
                PyGILState_STATE gstate = PyGILState_Ensure();
                Py_DECREF(item.memoryview);
                PyGILState_Release(gstate);
            }
        }

        static PyObject* call(AsyncFilePersister* self, PyObject* args, PyObject* kwargs) {
            PyObject* data;
            if (!PyArg_ParseTuple(args, "O", &data)) return nullptr;

            const void* ptr;
            size_t size;

            if (PyMemoryView_Check(data)) {
                Py_buffer* view = PyMemoryView_GET_BUFFER(data);
                ptr = view->buf;
                size = (size_t)view->len;
            } else if (PyBytes_Check(data)) {
                ptr = PyBytes_AS_STRING(data);
                size = (size_t)PyBytes_GET_SIZE(data);
            } else {
                PyErr_SetString(PyExc_TypeError, "expected memoryview or bytes");
                return nullptr;
            }

            Py_INCREF(data);

            {
                std::lock_guard<std::mutex> lock(self->mtx);
                self->queue.push_back({ptr, size, data});
            }
            self->cv.notify_one();

            Py_RETURN_NONE;
        }

        void do_close() {
            if (closed) return;
            closed = true;

            {
                std::lock_guard<std::mutex> lock(mtx);
                shutdown = true;
            }
            cv.notify_one();

            if (writer_thread.joinable()) {
                // Release the GIL so the writer thread can acquire it for
                // Py_DECREF (which triggers bf_releasebuffer on BufferSlots).
                Py_BEGIN_ALLOW_THREADS
                writer_thread.join();
                Py_END_ALLOW_THREADS
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

        static PyObject* tp_new(PyTypeObject* type, PyObject* args, PyObject* kwds) {
            AsyncFilePersister* self = (AsyncFilePersister*)type->tp_alloc(type, 0);
            if (self) {
                self->fd = -1;
                self->shutdown = false;
                self->closed = true;
                new (&self->writer_thread) std::thread();
                new (&self->mtx) std::mutex();
                new (&self->cv) std::condition_variable();
                new (&self->queue) std::deque<WriteItem>();
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

            int flags = O_WRONLY | O_CREAT | (append ? O_APPEND : O_TRUNC);
            self->fd = ::open(path, flags, 0644);
            if (self->fd < 0) {
                PyErr_Format(PyExc_IOError,
                    "Could not open file: %s for writing: %s", path, strerror(errno));
                return -1;
            }

            if (flock(self->fd, LOCK_EX | LOCK_NB) == -1) {
                PyErr_Format(PyExc_IOError,
                    "Could not lock file: %s for exclusive access: %s", path, strerror(errno));
                ::close(self->fd);
                self->fd = -1;
                return -1;
            }

            self->stored_path = path;
            self->shutdown = false;
            self->closed = false;
            self->writer_thread = std::thread(writer_loop, self);

            return 0;
        }

        static void dealloc(AsyncFilePersister* self) {
            self->do_close();

            // Destruct C++ members
            self->stored_path.~basic_string();
            self->queue.~deque();
            self->cv.~condition_variable();
            self->mtx.~mutex();
            self->writer_thread.~thread();

            Py_TYPE(self)->tp_free((PyObject*)self);
        }
    };

    static PyObject* AsyncFilePersister_path_getter(PyObject* obj, void*) {
        AsyncFilePersister* self = (AsyncFilePersister*)obj;
        return PyUnicode_FromStringAndSize(
            self->stored_path.c_str(), self->stored_path.size());
    }

    static PyMethodDef AsyncFilePersister_methods[] = {
        {"close", (PyCFunction)AsyncFilePersister::py_close, METH_NOARGS,
         "Flush pending writes, join writer thread, close file"},
        {NULL}
    };

    static PyGetSetDef AsyncFilePersister_getset[] = {
        {"path", AsyncFilePersister_path_getter, nullptr, "File path", NULL},
        {NULL}
    };

    PyTypeObject AsyncFilePersister_Type = {
        .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = MODULE "AsyncFilePersister",
        .tp_basicsize = sizeof(AsyncFilePersister),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)AsyncFilePersister::dealloc,
        .tp_call = (ternaryfunc)AsyncFilePersister::call,
        .tp_flags = Py_TPFLAGS_DEFAULT,
        .tp_doc = "Async file persister -- writes to file on a background thread",
        .tp_methods = AsyncFilePersister_methods,
        .tp_getset = AsyncFilePersister_getset,
        .tp_init = (initproc)AsyncFilePersister::init,
        .tp_new = AsyncFilePersister::tp_new,
    };
}
