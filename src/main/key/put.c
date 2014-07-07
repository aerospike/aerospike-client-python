#include <Python.h>
#include <stdbool.h>

#include <aerospike/aerospike_key.h>
#include <aerospike/as_key.h>
#include <aerospike/as_error.h>
#include <aerospike/as_record.h>

#include "client.h"
#include "conversions.h"
#include "key.h"

PyObject * AerospikeKey_Put(AerospikeKey * self, PyObject * args, PyObject * kwds)
{
	AerospikeKey * py_key = self;
	AerospikeClient * py_client = py_key->client;
	PyObject * py_bins = NULL;
	PyObject * py_meta = NULL;
	PyObject * py_policy = NULL;
	
	static char * kwlist[] = {"record", "metadata", "policy", NULL};

	if ( PyArg_ParseTupleAndKeywords(args, kwds, "O|OO:get", kwlist, &py_bins, &py_meta, &py_policy) == false ) {
		return NULL;
	}
	
	as_error err;
	as_error_init(&err);

	as_key * key_p = &py_key->key;
	as_policy_write * policy_p = NULL;

	
	as_policy_write policy;

	// Create the Aerospike record object from the Python pieces.
	// The Bins and Meta objects are dictionaries.
	as_record rec;
	pyobject_to_record(&err, py_bins, py_meta, &rec);

	if ( err.code == AEROSPIKE_OK ) {
		// Create the Aerospike as_policy_write object from the Python pieces,
		// assuming that one was initialized and passed in.  Otherwise, we'll just
		// use the default.
		if ( py_policy ) {
			as_policy_write_init(&policy);
			if ( pyobject_to_policy_write(&err, py_policy, &policy) == 0 ) {
				policy_p = &policy;
			}
		}
	}

	if ( err.code == AEROSPIKE_OK ) {
		aerospike_key_put(py_client->as, &err, policy_p, key_p, &rec);
	}
	
	as_record_destroy(&rec);

	if ( err.code != AEROSPIKE_OK ) {
		PyObject * py_err = NULL;
		error_to_pyobject(&err, &py_err);
		PyErr_SetObject(PyExc_Exception, py_err);
		return NULL;
	}

	return PyLong_FromLong(0);
}
