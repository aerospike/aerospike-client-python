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

PyObject * AerospikeClient_Put_Invoke(
	AerospikeClient * client, 
	PyObject * py_key, PyObject * py_bins, PyObject * py_meta, PyObject * py_policy)
{
	// Aerospike Client Arguments
	as_error err;
	as_policy_write policy;
	as_policy_write * policy_p = NULL;
	as_key key;
	as_record rec;
	
	// Initialize error
	as_error_init(&err);

	// Convert python key object to as_key
	pyobject_to_key(&err, py_key, &key);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python bins and metadata objects to as_record
	pyobject_to_record(&err, py_bins, py_meta, &rec);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Convert python policy object to as_policy_write
	pyobject_to_policy_write(&err, py_policy, &policy, &policy_p);
	if ( err.code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

	// Invoke operation
	aerospike_key_put(client->as, &err, policy_p, &key, &rec);
	
CLEANUP:

	as_record_destroy(&rec);

	// If an error occurred, tell Python.
	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}

PyObject * AerospikeClient_Put(AerospikeClient * client, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_key = NULL;
	PyObject * py_bins = NULL;
	PyObject * py_meta = NULL;
	PyObject * py_policy = NULL;
	
	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "record", "metadata", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO|OO:get", kwlist, 
			&py_key, &py_bins, &py_meta, &py_policy) == false ) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Put_Invoke(client, 
		py_key, py_bins, py_meta, py_policy
		);
}
