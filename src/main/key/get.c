#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "key.h"

PyObject * AerospikeKey_Get(AerospikeKey * self, PyObject * args, PyObject * kwds)
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
	as_record_init(rec, 0);

	aerospike_key_get(py_client->as, &err, NULL, key, &rec);

	PyObject * py_rec = NULL;

	if ( err.code == AEROSPIKE_OK ) {
		record_to_pyobject(&err, rec, key, &py_rec);
	}
	else if ( err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND ) {
		as_error_reset(&err);

		PyObject * py_rec_key = NULL;
		PyObject * py_rec_meta = Py_None;
		PyObject * py_rec_bins = Py_None;

		key_to_pyobject(&err, key, &py_rec_key);
		
		py_rec = PyTuple_New(3);
		PyTuple_SetItem(py_rec, 0, py_rec_key);
		PyTuple_SetItem(py_rec, 1, py_rec_meta);
		PyTuple_SetItem(py_rec, 2, py_rec_bins);

		Py_INCREF(py_rec_meta);
		Py_INCREF(py_rec_bins);
	}

	as_record_destroy(rec);
	
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return py_rec;
}