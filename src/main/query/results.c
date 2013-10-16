#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_query.h>
#include <aerospike/as_error.h>
#include <aerospike/as_query.h>

#include "client.h"
#include "conversions.h"
#include "query.h"

#undef TRACE
#define TRACE()

static bool each_result(const as_val * val, void * udata)
{
	if ( !val ) {
		return false;
	}

	PyObject * py_results = (PyObject *) udata;
	PyObject * py_result = NULL;

	as_error err;

	val_to_pyobject(&err, val, &py_result);

	PyList_Append(py_results, py_result);
	return true;
}

PyObject * AerospikeQuery_Results(AerospikeQuery * self, PyObject * args, PyObject * kwds)
{
	TRACE();
	
	AerospikeQuery * py_query = self;
	AerospikeClient * py_client = py_query->client;
	PyObject * py_policy = NULL;

	TRACE();
	
	static char * kwlist[] = {"policy", NULL};

	TRACE();
	
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|O:foreach", kwlist, &py_policy) == false ) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	PyObject * py_results = PyList_New(0);

	aerospike_query_foreach(py_client->as, &err, NULL, &py_query->query, each_result, py_results);

	return py_results;
}