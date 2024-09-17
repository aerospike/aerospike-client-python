#include <aerospike/as_txn.h>

#include <Python.h>

typedef struct {
    PyObject_HEAD
        /* Type-specific fields go here. */
        as_txn *txn;
} AerospikeTransaction;

static PyObject *AerospikeTransaction_name(AerospikeTransaction *self)
{
    uint64_t id = self->txn->id;
    PyObject *py_id = PyLong_FromUnsignedLong(id);
    return py_id;
}

static PyMethodDef AerospikeTransaction_methods[] = {
    {"id", (PyCFunction)AerospikeTransaction_name, METH_NOARGS,
     "Return multi-record transaction ID"},
    {NULL} /* Sentinel */
};

static PyTypeObject CustomType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name = "aerospike.Transaction",
    .tp_basicsize = sizeof(AerospikeTransaction),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = AerospikeTransaction_new,
    .tp_dealloc = AerospikeTransaction_dealloc};

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
