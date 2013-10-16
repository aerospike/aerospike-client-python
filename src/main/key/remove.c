#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "key.h"

PyObject * AerospikeKey_Remove(AerospikeKey * self, PyObject * args, PyObject * kwds)
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

	aerospike_key_remove(py_client->as, &err, NULL, key);

	if ( err.code != AEROSPIKE_OK ) {
	}

	return PyLong_FromLong(0);
}