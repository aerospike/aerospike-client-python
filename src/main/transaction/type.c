#include <Python.h>

#include "types.h"

static PyObject *AerospikeTransaction_id(AerospikeTransaction *self)
{
    uint64_t id = self->txn->id;
    PyObject *py_id = PyLong_FromUnsignedLongLong(id);
    if (py_id == NULL) {
        return NULL;
    }

    return py_id;
}

static void AerospikeTransaction_dealloc(AerospikeTransaction *self)
{
    // Transaction object can be created but not initialized, so need to check
    if (self->txn != NULL) {
        as_txn_destroy(self->txn);
    }
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

// Error indicator must always be checked after this call
// Constructor parameter name needed for constructing error message
static uint32_t get_uint32_t_from_pyobject(PyObject *pyobject,
                                           const char *param_name_of_pyobj)
{
    if (!PyLong_Check(pyobject)) {
        PyErr_Format(PyExc_TypeError, "%s must be an integer",
                     param_name_of_pyobj);
        goto error;
    }
    unsigned long long_value = PyLong_AsUnsignedLong(pyobject);
    if (PyErr_Occurred()) {
        goto error;
    }

    if (long_value > UINT32_MAX) {
        PyErr_Format(PyExc_ValueError,
                     "%s is too large for an unsigned 32-bit integer",
                     param_name_of_pyobj);
        goto error;
    }

    uint32_t value = (uint32_t)long_value;
    return value;

error:
    return 0;
}

// We don't initialize in __new__ because it's not documented how to raise
// exceptions in __new__
// We can raise an exception and fail out in __init__ though
static int AerospikeTransaction_init(AerospikeTransaction *self, PyObject *args,
                                     PyObject *kwds)
{
    static char *kwlist[] = {"reads_capacity", "writes_capacity", NULL};
    // We could use unsigned longs directly in the format string
    // But then we can't tell if they were set or not by the user
    // So we just use PyObjects for the optional args instead
    PyObject *py_reads_capacity = NULL;
    PyObject *py_writes_capacity = NULL;

    if (PyArg_ParseTupleAndKeywords(args, kwds, "|OO", kwlist,
                                    &py_reads_capacity,
                                    &py_writes_capacity) == false) {
        goto error;
    }

    // Both reads and writes capacities must be specified,
    // or both must be omitted
    as_txn *txn;
    if ((py_reads_capacity == NULL) ^ (py_writes_capacity == NULL)) {
        PyErr_Format(PyExc_TypeError, "Both %s and %s must be specified",
                     kwlist[0], kwlist[1]);
        goto error;
    }
    else if (py_reads_capacity && py_writes_capacity) {
        uint32_t reads_capacity =
            get_uint32_t_from_pyobject(py_reads_capacity, kwlist[0]);
        if (PyErr_Occurred()) {
            goto error;
        }
        uint32_t writes_capacity =
            get_uint32_t_from_pyobject(py_writes_capacity, kwlist[1]);
        if (PyErr_Occurred()) {
            goto error;
        }
        txn = as_txn_create_capacity(reads_capacity, writes_capacity);
    }
    else {
        txn = as_txn_create();
    }

    // If this transaction object was already initialized before, reinitialize it
    if (self->txn) {
        as_txn_destroy(self->txn);
    }
    self->txn = txn;

    return 0;
error:
    return -1;
}

static PyMethodDef AerospikeTransaction_methods[] = {
    {"id", (PyCFunction)AerospikeTransaction_id, METH_NOARGS,
     "Return multi-record transaction ID"},
    {NULL} /* Sentinel */
};

PyTypeObject AerospikeTransaction_Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name =
        FULLY_QUALIFIED_TYPE_NAME("Transaction"),
    .tp_basicsize = sizeof(AerospikeTransaction),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = AerospikeTransaction_new,
    .tp_init = (initproc)AerospikeTransaction_init,
    .tp_methods = AerospikeTransaction_methods,
    .tp_dealloc = (destructor)AerospikeTransaction_dealloc};

PyTypeObject *AerospikeTransaction_Ready()
{
    return PyType_Ready(&AerospikeTransaction_Type) == 0
               ? &AerospikeTransaction_Type
               : NULL;
}
