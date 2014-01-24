#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "key.h"

PyObject * AerospikeKey_Put(AerospikeKey * self, PyObject * args, PyObject * kwds)
{
	AerospikeKey * py_key = self;
	AerospikeClient * py_client = py_key->client;
	PyObject * py_bins = NULL;
	PyObject * py_meta = NULL;
	PyObject * py_policy = NULL;
	
	static char * kwlist[] = {"record", "metadata", "policy", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|OO:get", kwlist, &py_bins, &py_meta, &py_policy) == false ) {
		return NULL;
	}
	
	as_error err;
	as_error_init(&err);

	as_key * key = &py_key->key;
	
	as_record rec;
	
	pyobject_to_record(&err, py_bins, py_meta, &rec);

	aerospike_key_put(py_client->as, &err, NULL, key, &rec);
	
	as_record_destroy(&rec);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}
