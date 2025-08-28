#include "stream.h"
// #include "gateway.h"

static PyTypeObject * hidden_types[] = {
    // &_proxy::Pickled_Type,
    &retracesoftware_stream::StreamHandle_Type,
    &retracesoftware_stream::WeakRefCallback_Type,
    nullptr
};

static PyTypeObject * exposed_types[] = {
    &retracesoftware_stream::ObjectWriter_Type,
    &retracesoftware_stream::ObjectReader_Type,
    nullptr
};

static PyMethodDef module_methods[] = {
    // {"create_wrapping_proxy_type", (PyCFunction)create_wrapping_proxy_type, METH_VARARGS | METH_KEYWORDS, "TODO"},
    // {"unwrap_apply", (PyCFunction)unwrap_apply, METH_FASTCALL | METH_KEYWORDS, "Call the wrapped target with unproxied *args/**kwargs."},
    // {"thread_id", (PyCFunction)thread_id, METH_NOARGS, "TODO"},
    // {"set_thread_id", (PyCFunction)set_thread_id, METH_O, "TODO"},
    // {"proxy_test", (PyCFunction)proxy_test, METH_O, "TODO"},
    // {"unwrap", (PyCFunction)unwrap, METH_O, "TODO"},
    // {"yields_callable_instances", (PyCFunction)yields_callable_instances, METH_O, "TODO"},
    // {"yields_weakly_referenceable_instances", (PyCFunction)yields_weakly_referenceable_instances, METH_O, "TODO"},

    {NULL, NULL, 0, NULL}  // Sentinel
};

// Module definition
static PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "retracesoftware_stream",
    "TODO",
    0,
    module_methods
};

PyMODINIT_FUNC PyInit_retracesoftware_stream(void) {
    PyObject* module = PyModule_Create(&moduledef);

    if (!module) {
        return NULL;
    }

    for (int i = 0; hidden_types[i]; i++) {
        if (PyType_Ready(hidden_types[i]) < 0) {
            Py_DECREF(module);
            return nullptr;
        }
    }

    for (int i = 0; exposed_types[i]; i++) {
        if (PyType_Ready(exposed_types[i]) < 0) {
            Py_DECREF(module);
            return nullptr;
        }
        // Find the last dot in the string
        const char *last_dot = strrchr(exposed_types[i]->tp_name, '.');

        // If a dot is found, the substring starts after the dot
        const char *name = (last_dot != NULL) ? (last_dot + 1) : exposed_types[i]->tp_name;

        if (PyModule_AddObject(module, name, (PyObject *)exposed_types[i]) < 0) {
            Py_DECREF(module);
            return nullptr;
        }
    }
    return module;
}