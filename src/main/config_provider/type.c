#include "types.h"
#include "config_provider.h"

static PyObject *AerospikeConfigProvider_new(PyTypeObject *type, PyObject *args,
                                             PyObject *kwds)
{
    AerospikeConfigProvider *self =
        (AerospikeConfigProvider *)type->tp_alloc(type, 0);
    if (self == NULL) {
        goto error;
    }

    static char *kwlist[] = {"path", "interval", NULL};
    const char *path = NULL;
    unsigned long interval = AS_CONFIG_PROVIDER_INTERVAL_DEFAULT;

    if (PyArg_ParseTupleAndKeywords(args, kwds, "s|k", kwlist, &path,
                                    &interval) == false) {
        goto error;
    }

    if (interval > UINT32_MAX) {
        PyErr_Format(PyExc_ValueError,
                     "%s is too large for an unsigned 32-bit integer",
                     kwlist[1]);
        goto error;
    }

    self->path = strdup(path);
    self->interval = interval;

    return (PyObject *)self;
error:
    return NULL;
}

static PyObject *AerospikeConfigProvider_get_path(AerospikeConfigProvider *self,
                                                  void *closure)
{
    PyObject *py_path = PyUnicode_FromString(self->path);
    if (py_path == NULL) {
        return NULL;
    }
    return py_path;
}

static PyObject *
AerospikeConfigProvider_get_interval(AerospikeConfigProvider *self,
                                     void *closure)
{
    PyObject *py_interval =
        PyLong_FromUnsignedLong((unsigned long)self->interval);
    if (py_interval == NULL) {
        return NULL;
    }
    return py_interval;
}

static void AerospikeConfigProvider_dealloc(AerospikeConfigProvider *self)
{
    if (self->path) {
        free(self->path);
    }
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyGetSetDef AerospikeConfigProvider_getsetters[] = {
    {.name = "path", .get = (getter)AerospikeConfigProvider_get_path},
    {.name = "interval", .get = (getter)AerospikeConfigProvider_get_interval},
    {NULL} /* Sentinel */
};

PyTypeObject AerospikeConfigProvider_Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        FULLY_QUALIFIED_TYPE_NAME("ConfigProvider"),
    .tp_basicsize = sizeof(AerospikeConfigProvider),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = AerospikeConfigProvider_new,
    .tp_dealloc = (destructor)AerospikeConfigProvider_dealloc,
    .tp_getset = AerospikeConfigProvider_getsetters};

PyTypeObject *AerospikeConfigProvider_Ready()
{
    return PyType_Ready(&AerospikeConfigProvider_Type) == 0
               ? &AerospikeConfigProvider_Type
               : NULL;
}
