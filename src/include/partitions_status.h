#include <Python.h>

#include <aerospike/as_partition_filter.h>

PyTypeObject *AerospikePartitionsStatusObject_Ready();

PyObject *AerospikePartitionsStatusObject_Type_New(PyTypeObject *type,
                                                   PyObject *args,
                                                   PyObject *kwds);

PyObject *create_py_partitions_status_object(as_partitions_status *parts_all);

extern PyTypeObject AerospikePartitionsStatusObject_Type;
