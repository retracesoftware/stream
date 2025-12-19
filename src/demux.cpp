#include "stream.h"
#include "wireformat.h"
#include <structmember.h>
#include <condition_variable>
#include <mutex>
#include <chrono>

namespace retracesoftware_stream {
    class ScopeGuard {
    public:
        // Accept any callable (lambda, function pointer, etc.)
        template<typename Callable>
        explicit ScopeGuard(Callable&& func) 
            : _func(std::forward<Callable>(func)) {}

        // Destructor executes the lambda
        ~ScopeGuard() {
            _func();
        }

        // Prevent copying (RAII should be unique)
        ScopeGuard(const ScopeGuard&) = delete;
        ScopeGuard& operator=(const ScopeGuard&) = delete;

    private:
        std::function<void ()> _func;
    };

    struct Demux : public PyObject {
        PyObject * key_fn;
        PyObject * source;
        PyObject * next = nullptr;
        std::condition_variable waiting;
        std::mutex mtx;
        int timeout = 0;
        map<PyObject *, PyThreadState *> pending;
        vectorcallfunc vectorcall;

        static PyObject* create(PyTypeObject* type, PyObject * args, PyObject * kwds) {
            PyObject * key_fn;
            PyObject * source;
            int timeout = 1000;

            static const char* kwlist[] = {
                "key_fn", 
                "source", 
                "timeout",
                nullptr};  // Keywords allowed

            if (!PyArg_ParseTupleAndKeywords(args, kwds, "OOI", (char **)kwlist, 
                &key_fn, 
                &source,
                &timeout)) {
                return nullptr;
            }

            auto* self = reinterpret_cast<Demux*>(type->tp_alloc(type, 0));
            if (!self) return nullptr;

            // Construct the std::set in-place
            new (&self->pending) map<PyObject *, PyThreadState *>();
            new (&self->mtx) std::mutex();
            new (&self->waiting) std::condition_variable();

            self->key_fn = Py_NewRef(key_fn);
            self->source = Py_NewRef(source);
            self->timeout = timeout;

            self->vectorcall = (vectorcallfunc)call;
            return reinterpret_cast<PyObject*>(self);
        }

        void ensure_next() {
            if (!next) {
                next = PyObject_CallNoArgs(source);
                if (!next) throw nullptr;

                if (pending.size() > 0) {
                    waiting.notify_all();
                }
            }
        }

        bool is_current_key(PyObject * key) {
            PyObject * current_key = PyObject_CallOneArg(key_fn, next);

            if (!current_key) throw nullptr;

            int status = PyObject_RichCompareBool(current_key, key, Py_EQ);

            Py_DECREF(current_key);

            switch (status) {
                case 1: return true;
                case 0: return false;
                default: throw nullptr;
            }
        }

        bool condition(PyObject * key) {
            retracesoftware::GILGuard guard;
            ensure_next();
            return is_current_key(key);
        }

        PyObject * next_and_clear() {
            PyObject * res = next;
            next = nullptr;
            return res;
        }

        void on_timeout() {
            raise(SIGTRAP);
        }

        PyObject * next_for_thread(PyObject * thread) {
            ensure_next();

            if (!is_current_key(thread)) {
                ScopeGuard guard([this, thread] { pending.erase(thread); });

                pending[thread] = PyThreadState_Get();
                retracesoftware::GILReleaseGuard release;
                std::unique_lock<std::mutex> lock(mtx);

                if (!waiting.wait_for(
                    lock, 
                    std::chrono::milliseconds(timeout), 
                    [this, thread]() { return condition(thread); })) {
                        on_timeout();
                        throw nullptr;
                }
            }
            return next_and_clear();
        }

        static void dealloc(Demux* self) {
            PyObject_GC_UnTrack(self);
            clear(self);

            self->pending.~map<PyObject *, PyThreadState *>();

            Py_TYPE(self)->tp_free(reinterpret_cast<PyObject*>(self));
        }

        static int traverse(Demux* self, visitproc visit, void* arg) {
            Py_VISIT(self->key_fn);
            Py_VISIT(self->source);
            Py_VISIT(self->next);
            return 0;
        }

        static int clear(Demux* self) {

            Py_CLEAR(self->key_fn);
            Py_CLEAR(self->source);
            Py_CLEAR(self->next);
            return 0;
        }

        static PyObject* call(Demux *self, PyObject *const *args, size_t nargsf, PyObject *kwnames) {
            Py_ssize_t nargs = PyVectorcall_NARGS(nargsf);

            if (nargs != 1 || kwnames) {
                PyErr_SetString(PyExc_TypeError, "Demux takes one positional arg");
                return nullptr;
            }

            try {
                return self->next_for_thread(args[0]);
            } catch (...) {
                return nullptr;
            }
        }
    };

    PyTypeObject Demux_Type = {
        .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
        .tp_name = MODULE "Demux",
        .tp_basicsize = sizeof(Demux),
        .tp_itemsize = 0,
        .tp_dealloc = (destructor)Demux::dealloc,
        .tp_vectorcall_offset = OFFSET_OF_MEMBER(Demux, vectorcall),
        .tp_call = PyVectorcall_Call,
        .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC | Py_TPFLAGS_HAVE_VECTORCALL,
        .tp_doc = "TODO",
        .tp_traverse = (traverseproc)Demux::traverse,
        .tp_clear = (inquiry)Demux::clear,
        // .tp_methods = fastset_methods,
        // .tp_init = (initproc)Demux::init,
        .tp_new = Demux::create,
    };

}