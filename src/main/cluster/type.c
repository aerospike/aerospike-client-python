#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <aerospike/as_cluster.h>

typedef struct {
    PyObject_HEAD
    /* Type-specific fields go here. */
} Cluster;

static PyMethodDef AerospikeCluster_methods[] = {
    {NULL} /* Sentinel */
};

static PyMemberDef AerospikeGeospatial_Type_Members[] = {
    {"geo_data", T_OBJECT, offsetof(AerospikeGeospatial, geo_data), 0,
     "The aerospike.GeoJSON object"},
    {NULL}};

static PyTypeObject AerospikeCluster_Type = {
    .ob_base = PyVarObject_HEAD_INIT(NULL, 0).tp_name = "aerospike.Cluster",
    .tp_doc = PyDoc_STR("Aerospike Cluster"),
    .tp_basicsize = sizeof(Cluster),
    .tp_itemsize = 0,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_new = PyType_GenericNew,
    .tp_methods = AerospikeCluster_methods};

PyTypeObject *AerospikeScan_Ready()
{
    if (PyType_Ready(&AerospikeCluster_Type) < 0) {
        return &AerospikeCluster_Type;
    }
    return NULL;
}
