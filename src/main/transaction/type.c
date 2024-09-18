#include <Python.h>

#include "types.h"

static PyObject *AerospikeTransaction_id(AerospikeTransaction *self)
{
    uint64_t id = self->txn->id;
    PyObject *py_id = PyLong_FromUnsignedLong(id);
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
        return self;
    }

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
        // TODO: Deallocate
        return NULL;
    }

    if (py_reads_capacity && py_writes_capacity) {
        // TODO: how to check if this fails?
        self->txn =
            as_txn_create_capacity(py_reads_capacity, py_writes_capacity);
    }
    else {
        self->txn = as_txn_create();
    }

    return (PyObject *)self;
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
    .tp_methods = AerospikeTransaction_methods,
    .tp_dealloc = (destructor)AerospikeTransaction_dealloc};

PyTypeObject *AerospikeTransaction_Ready()
{
    return PyType_Ready(&AerospikeTransaction_Type) == 0
               ? &AerospikeTransaction_Type
               : NULL;
}
