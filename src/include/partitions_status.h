#include <Python.h>

#include <aerospike/as_partition_filter.h>

PyTypeObject *AerospikePartitionsStatusObject_Ready();
PyTypeObject *AerospikePartitionStatusObject_Ready();

PyObject *create_py_partitions_status_object(as_error *err,
                                             as_partitions_status *parts_all);

extern PyTypeObject AerospikePartitionsStatusObject_Type;
