#include "hll.h"

typedef struct {
    PyBytesObject *data;
} HyperLogLog;

static PyObject *AerospikeHyperLogLog_new(PyTypeObject *type, PyObject *args,
                                          PyObject *kwds)
{
    PyObject *py_hll_instance = PyBytes_Type.tp_new(type, args, kwds);
    return py_hll_instance;
}

static PyTypeObject AerospikeHyperLogLogType = {
    PyVarObject_HEAD_INIT(NULL, 0).tp_name = "aerospike.HyperLogLog",
    .tp_doc = PyDoc_STR("HyperLogLog object"),
    .tp_basicsize = sizeof(HyperLogLog),
    .tp_new = AerospikeHyperLogLog_new,
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
};

PyTypeObject *AerospikeHyperLogLog_Ready()
{
    AerospikeHyperLogLogType.tp_base = &PyBytes_Type;
    return PyType_Ready(&AerospikeHyperLogLogType) == 0
               ? &AerospikeHyperLogLogType
               : NULL;
}
