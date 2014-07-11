#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "key.h"

PyObject * AerospikeKey_Apply(AerospikeKey * key, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_key = key->key;
	PyObject * py_module = NULL;
	PyObject * py_function = NULL;
	PyObject * py_arglist = NULL;
	PyObject * py_policy = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"module", "function", "args", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO|O:get", kwlist, 
		&py_module, &py_function, &py_arglist, &py_policy) == false ) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Apply_Invoke(key->client, py_key, py_module, py_function, 
		py_arglist, py_policy);
}