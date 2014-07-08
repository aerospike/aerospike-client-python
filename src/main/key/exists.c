#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "key.h"

PyObject * AerospikeKey_Exists(AerospikeKey * key, PyObject * args, PyObject * kwds)
{
	// Client Object
	AerospikeClient * client = key->client;

	// Python Function Arguments
	PyObject * py_key = key->key;
	PyObject * py_policy = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|O:get", kwlist, &py_policy) == false ) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Exists_Invoke(client, py_key, py_policy);
}