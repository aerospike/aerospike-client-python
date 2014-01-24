#include <Python.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>

#include "client.h"
#include "conversions.h"

PyObject * AerospikeClient_Connect(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
	as_error err;
	
	aerospike_connect(self->as, &err);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}
	
	Py_INCREF(self);
	return (PyObject *) self;
}