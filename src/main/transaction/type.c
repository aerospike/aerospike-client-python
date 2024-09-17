#include <aerospike/as_txn.h>

#include <Python.h>

typedef struct {
    PyObject_HEAD
    /* Type-specific fields go here. */
    as_txn *txn;
} AerospikeTransaction;

static PyTypeObject CustomType = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "aerospike.Transaction",
    .tp_basicsize = sizeof(AerospikeTransaction),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
};
