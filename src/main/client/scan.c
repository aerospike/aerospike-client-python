#include <Python.h>
#include <stdbool.h>

#include "client.h"
#include "scan.h"

PyObject * AerospikeClient_Scan(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	PyObject * new_args = PyTuple_New(2);
	PyTuple_SetItem(new_args, 0, (PyObject *) self);
	PyTuple_SetItem(new_args, 1, (PyObject *) args);

	return AerospikeScan_Create(NULL, new_args, kwds);
}