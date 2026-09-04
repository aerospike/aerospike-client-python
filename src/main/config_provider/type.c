#include "types.h"
#include "config_provider.h"
#include "conversions.h"

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
    // We take in a python object and do our own input validation
    // because PyArg_ParseTupleAndKeywords() doesn't check for overflow errors when taking in a C unsigned long
    // i.e when we pass in a larger value than UINT32_MAX, it will be truncated when assigned to an unsigned long var
    // in Windows
    PyObject *py_interval = NULL;

    if (PyArg_ParseTupleAndKeywords(args, kwds, "s|O", kwlist, &path,
                                    &py_interval) == false) {
        goto error;
    }

    uint32_t interval;
    if (py_interval) {
        interval = convert_pyobject_to_uint32_t(py_interval);
        if (PyErr_Occurred()) {
            goto error;
        }
    }
    else {
        interval = AS_CONFIG_PROVIDER_INTERVAL_DEFAULT;
    }

    self->path = strdup(path);
    self->interval = interval;

    return (PyObject *)self;
error:
    Py_TYPE(self)->tp_free((PyObject *)self);
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
