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
    if (self != NULL) {
        // TODO: how to check if this fails?
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