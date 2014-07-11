#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "key.h"

PyObject * AerospikeKey_Put(AerospikeKey * key, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_key = key->key;
	PyObject * py_bins = NULL;
	PyObject * py_meta = NULL;
	PyObject * py_policy = NULL;
	
	// Python Function Keyword Arguments
	static char * kwlist[] = {"record", "metadata", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|OO:get", kwlist, &py_bins, &py_meta, &py_policy) == false ) {
		return NULL;
	}
	
	// Invoke Operation
	return AerospikeClient_Put_Invoke(key->client, py_key, py_bins, py_meta, py_policy);
}
