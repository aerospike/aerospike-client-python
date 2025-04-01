#include "types.h"
#include "config_provider.h"

static PyObject *AerospikeConfigProvider_new(PyTypeObject *type, PyObject *args,
                                             PyObject *kwds)
{
    AerospikeConfigProvider *self =
        (AerospikeConfigProvider *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    return (PyObject *)self;
}

static int AerospikeConfigProvider_init(AerospikeConfigProvider *self,
                                        PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {"path", "interval", NULL};
    const char *path = NULL;
    // TODO: need default from c client
    unsigned long interval = 60;

    if (PyArg_ParseTupleAndKeywords(args, kwds, "s|k", kwlist, &path,
                                    &interval) == false) {
        goto error;
    }

    self->path = strdup(path);
    self->interval = interval;

    return 0;

error:
    return -1;
}

static void AerospikeConfigProvider_dealloc(AerospikeConfigProvider *self)
{
    if (self->path) {
        free(self->path);
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

PyTypeObject AerospikeConfigProvider_Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        FULLY_QUALIFIED_TYPE_NAME("ConfigProvider"),
    .tp_basicsize = sizeof(AerospikeConfigProvider),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = AerospikeConfigProvider_new,
    .tp_init = (initproc)AerospikeConfigProvider_init,
    .tp_dealloc = (destructor)AerospikeConfigProvider_dealloc,
};

PyTypeObject *AerospikeConfigProvider_Ready()
{
    return PyType_Ready(&AerospikeConfigProvider_Type) == 0
               ? &AerospikeConfigProvider_Type
               : NULL;
}
