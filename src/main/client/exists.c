#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "key.h"
#include "policy.h"

PyObject * AerospikeClient_Exists_Invoke(
	AerospikeClient * client, 
	PyObject * py_key, PyObject * py_policy)
{
	// Python Return Value
	PyObject * py_result = NULL;

	// Aerospike Client Arguments
	as_error err;
	as_policy_read policy;
	as_policy_read * policy_p = NULL;
	as_key key;
	as_record * rec = NULL;

	// Initialize error
	as_error_init(&err);

	// Convert python key object to as_key
	pyobject_to_key(&err, py_key, &key);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_exists
	pyobject_to_policy_read(&err, py_policy, &policy, &policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	aerospike_key_exists(client->as, &err, policy_p, &key, &rec);

	if ( err.code == AEROSPIKE_OK ) {

		PyObject * py_result_key = NULL;
		PyObject * py_result_meta = NULL;

		key_to_pyobject(&err, &key, &py_result_key);
		metadata_to_pyobject(&err, rec, &py_result_meta);
		
		py_result = PyTuple_New(2);
		PyTuple_SetItem(py_result, 0, py_result_key);
		PyTuple_SetItem(py_result, 1, py_result_meta);
	}
	else if ( err.code == AEROSPIKE_ERR_RECORD_NOT_FOUND ) {
		as_error_reset(&err);

		PyObject * py_result_key = NULL;
		PyObject * py_result_meta = Py_None;

		key_to_pyobject(&err, &key, &py_result_key);
		
		py_result = PyTuple_New(2);
		PyTuple_SetItem(py_result, 0, py_result_key);
		PyTuple_SetItem(py_result, 1, py_result_meta);

		Py_INCREF(py_result_meta);
	}

CLEANUP:

	as_record_destroy(rec);
	
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return py_result;
}

PyObject * AerospikeClient_Exists(AerospikeClient * client, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_key = NULL;
	PyObject * py_policy = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|O:get", kwlist, 
			&py_key, &py_policy) == false ) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Exists_Invoke(client, py_key, py_policy);
}