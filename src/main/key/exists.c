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
	as_record * rec = NULL;

	aerospike_key_exists(py_client->as, &err, NULL, key, &rec);

	PyObject * py_result = NULL;

	if ( err.code == AEROSPIKE_OK ) {

		PyObject * py_result_key = NULL;
		PyObject * py_result_meta = NULL;

		key_to_pyobject(&err, key, &py_result_key);
		metadata_to_pyobject(&err, rec, &py_result_meta);
		
		py_result = PyTuple_New(2);
		PyTuple_SetItem(py_result, 0, py_result_key);
		PyTuple_SetItem(py_result, 1, py_result_meta);
	}
	else if ( err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND ) {
		as_error_reset(&err);

		PyObject * py_result_key = NULL;
		PyObject * py_result_meta = Py_None;

		key_to_pyobject(&err, key, &py_result_key);
		
		py_result = PyTuple_New(2);
		PyTuple_SetItem(py_result, 0, py_result_key);
		PyTuple_SetItem(py_result, 1, py_result_meta);

		Py_INCREF(py_result_meta);
	}

	as_record_destroy(rec);
	
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return py_result;
}