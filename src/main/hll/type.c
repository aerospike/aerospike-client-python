#include "hll.h"

typedef struct {
    PyBytesObject *data;
} HyperLogLog;

static int AerospikeHyperLogLog_init(HyperLogLog *self, PyObject *args,
                                     PyObject *kwds)
{
    if (PyBytes_Type.tp_init((PyObject *)self, args, kwds) < 0)
        return -1;
    return 0;
}

static PyTypeObject AerospikeHyperLogLogType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "aerospike.HyperLogLog",
    .tp_doc = PyDoc_STR("HyperLogLog object"),
    .tp_basicsize = sizeof(HyperLogLog),
    .tp_itemsize = 0,
    .tp_init = (initproc)AerospikeHyperLogLog_init,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
};

PyTypeObject *AerospikeHyperLogLog_Ready()
{
    AerospikeHyperLogLogType.tp_base = &PyBytes_Type;
    return PyType_Ready(&AerospikeHyperLogLogType) == 0
               ? &AerospikeHyperLogLogType
               : NULL;
}
