#include <Python.h>

PyObject *AerospikeClient_EnableMetrics(AerospikeClient *self, PyObject *args,
                                        PyObject *kwds);
PyObject *AerospikeClient_DisableMetrics(AerospikeClient *self, PyObject *args);
