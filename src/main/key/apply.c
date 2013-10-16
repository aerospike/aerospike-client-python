#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "key.h"

PyObject * AerospikeKey_Apply(AerospikeKey * self, PyObject * args, PyObject * kwds)
{
	AerospikeKey * py_key = self;
	AerospikeClient * py_client = py_key->client;
	PyObject * py_module = NULL;
	PyObject * py_function = NULL;
	PyObject * py_arglist = NULL;
	PyObject * py_policy = NULL;

	static char * kwlist[] = {"module", "function", "args", "policy", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO|O:get", kwlist, 
		&py_module, &py_function, &py_arglist, &py_policy) == false ) {
		return NULL;
	}

	as_error err;
	as_error_init(&err);

	as_key * key = &py_key->key;
	
	char * module = PyString_AsString(py_module);
	char * function = PyString_AsString(py_function);

	as_list * arglist = NULL;
	pyobject_to_list(&err, py_arglist, &arglist);

	as_val * result = NULL;

	aerospike_key_apply(py_client->as, &err, NULL, key, module, function, arglist, &result);

	PyObject * py_result = NULL;
	
	if ( err.code == AEROSPIKE_OK ) {
		val_to_pyobject(&err, result, &py_result);
	}
	
	as_list_destroy(arglist);
	as_val_destroy(result);
	
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return py_result;
}