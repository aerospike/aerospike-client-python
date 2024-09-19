#include <Python.h>

#include "types.h"

static PyObject *AerospikeTransaction_id(AerospikeTransaction *self)
{
    uint64_t id = self->txn->id;
    PyObject *py_id = PyLong_FromUnsignedLongLong(id);
    if (py_id != NULL) {
        return NULL;
    }

    return py_id;
}

static void AerospikeTransaction_dealloc(AerospikeTransaction *self)
{
    as_txn_destroy(self->txn);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *AerospikeTransaction_new(PyTypeObject *type, PyObject *args,
                                          PyObject *kwds)
{
    AerospikeTransaction *self =
        (AerospikeTransaction *)type->tp_alloc(type, 0);
    if (self == NULL) {
        return NULL;
    }
    return (PyObject *)self;
}

static int AerospikeTransaction_init(AerospikeTransaction *self, PyObject *args,
                                     PyObject *kwds)
{
    static char *kwlist[] = {"reads_capacity", "writes_capacity", NULL};
    // We could use unsigned longs directly in the format string
    // But then we can't tell if they were set or not by the user
    // So we just use PyObjects for the optional args instead
    PyObject *py_reads_capacity = NULL;
    PyObject *py_writes_capacity = NULL;

    // TODO: how to enforce 32-bit size limit
    if (PyArg_ParseTupleAndKeywords(args, kwds, "|OO", kwlist,
                                    &py_reads_capacity,
                                    &py_writes_capacity) == false) {
        goto error;
    }

    // Both reads and writes capacities must be specified,
    // or both must be omitted
    if ((py_reads_capacity == NULL) ^ (py_writes_capacity == NULL)) {
        PyErr_SetString(
            PyExc_TypeError,
            "Both reads capacity and writes capacity must be specified");
        goto error;
    }
    else if (!PyLong_Check(py_reads_capacity)) {
        PyErr_SetString(PyExc_TypeError, "Reads capacity must be an integer");
        goto error;
    }
    else if (!PyLong_Check(py_writes_capacity)) {
        PyErr_SetString(PyExc_TypeError, "Writes capacity must be an integer");
        goto error;
    }

    if (py_reads_capacity && py_writes_capacity) {
        unsigned long reads_capacity =
            (uint32_t)PyLong_AsUnsignedLong(py_reads_capacity);
        if (PyErr_Occurred()) {
            goto error;
        }
        unsigned long writes_capacity =
            (uint32_t)PyLong_AsUnsignedLong(py_writes_capacity);
        if (PyErr_Occurred()) {
            goto error;
        }
        self->txn = as_txn_create_capacity(reads_capacity, writes_capacity);
    }
    else {
        self->txn = as_txn_create();
    }

    return (PyObject *)self;
error:
    return -1;
}

static PyMethodDef AerospikeTransaction_methods[] = {
    {"id", (PyCFunction)AerospikeTransaction_id, METH_NOARGS,
     "Return multi-record transaction ID"},
    {NULL} /* Sentinel */
};

PyTypeObject AerospikeTransaction_Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name = "aerospike.Transaction",
    .tp_basicsize = sizeof(AerospikeTransaction),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = AerospikeTransaction_new,
    .tp_init = AerospikeTransaction_init,
    .tp_methods = AerospikeTransaction_methods,
    .tp_dealloc = (destructor)AerospikeTransaction_dealloc};

PyTypeObject *AerospikeTransaction_Ready()
{
    return PyType_Ready(&AerospikeTransaction_Type) == 0
               ? &AerospikeTransaction_Type
               : NULL;
}
