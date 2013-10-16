#include <Python.h>
#include <stdbool.h>

#include "client.h"
#include "query.h"

PyObject * AerospikeClient_Query(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	PyObject * new_args = PyTuple_New(2);
	PyTuple_SetItem(new_args, 0, (PyObject *) self);
	PyTuple_SetItem(new_args, 1, (PyObject *) args);

	return AerospikeQuery_Create(NULL, new_args, kwds);
}