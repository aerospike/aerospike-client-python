#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "key.h"

PyObject * AerospikeKey_Exists(AerospikeKey * self, PyObject * args, PyObject * kwds)
{
	AerospikeKey * py_key = self;
	AerospikeClient * py_client = py_key->client;
	PyObject * py_policy = NULL;

	static char * kwlist[] = {"policy", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|O:get", kwlist, &py_policy) == false ) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	as_key * key = &py_key->key;

	bool exists = false;

	aerospike_key_exists(py_client->as, &err, NULL, key, &exists);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	if ( exists ) {
		Py_INCREF(Py_True);
		return Py_True;
	}
	else {
		Py_INCREF(Py_False);
		return Py_False;
	}
}