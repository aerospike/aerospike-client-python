#include <Python.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>

#include "client.h"

PyObject * AerospikeClient_Close(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	as_error err;
	
	aerospike_close(self->as, &err);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	aerospike_destroy(self->as);
	self->as = NULL;

	Py_INCREF(Py_None);
	return Py_None;
}