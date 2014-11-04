/*******************************************************************************
 * Copyright 2013-2014 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

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

PyObject *  AerospikeClient_Operate_Invoke(
    AerospikeClient * self,
    as_key * key, PyObject * py_bin, char* val, as_policy_read * policy,
as_error * err, long ttl, long operation, as_operations * ops) {


    // Convert python object into bin string  
    //char val[AS_NAMESPACE_MAX_SIZE];
	switch(operation) {
		case AS_OPERATOR_PREPEND:
    		if( !PyString_Check(py_bin) ) {
        		as_error_update(err, AEROSPIKE_ERR_PARAM, "Bin should be a string");
        		goto CLEANUP;
    		}
    		char *bin = PyString_AsString(py_bin);
			as_operations_add_prepend_str(ops, bin, val);
			break;

		case AS_OPERATOR_TOUCH:
			ops->ttl = ttl;
			as_operations_add_touch(ops);
			break;
	}

CLEANUP:
	 if ( err->code != AEROSPIKE_OK ) {
        PyObject * py_err = NULL;
        error_to_pyobject(err, &py_err);
        PyErr_SetObject(PyExc_Exception, py_err);
        return NULL;
    }

	return PyLong_FromLong(0);
}
PyObject * AerospikeClient_Prepend(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
    // Initialize error
    as_error err;
    as_error_init(&err);

	// Python Function Arguments
	PyObject * py_key = NULL;
    PyObject * py_bin = NULL;
    PyObject * py_val = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_result = NULL;
	PyObject * py_rec = NULL;

	as_operations ops;
    as_policy_read policy;
    as_policy_read * policy_p = NULL;
    as_key key;
   // Initialize record
    as_operations_inita(&ops, 1);

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "bin", "val", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO|O:prepend", kwlist, 
			&py_key, &py_bin, &py_val, &py_policy) == false ) {
		return NULL;
	}

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
    // Convert python object into value string  
    //char val[AS_NAMESPACE_MAX_SIZE];
    if( !PyString_Check(py_val) ) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Value should be a string");
        goto CLEANUP;
    }
    char *value = PyString_AsString(py_val);
    //strncpy(ns, namespace, AS_NAMESPACE_MAX_SIZE);

	py_result = AerospikeClient_Operate_Invoke(self, &key, py_bin, value,
policy_p, &err, 0, AS_OPERATOR_PREPEND, &ops);
	if (py_result)
	{
		aerospike_key_operate(self->as, &err, policy_p, &key, &ops, NULL);
		if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
		//record_to_pyobject(&err, rec, &key, &py_rec);
		}
	}

CLEANUP:
	 if ( err.code != AEROSPIKE_OK ) {
        PyObject * py_err = NULL;
        error_to_pyobject(&err, &py_err);
        PyErr_SetObject(PyExc_Exception, py_err);
        return NULL;
    }
	return PyLong_FromLong(0);
	// Invoke Operation
//	return AerospikeClient_Get_Invoke(self, py_key, py_policy);
}
PyObject * AerospikeClient_Touch(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
    // Initialize error
    as_error err;
    as_error_init(&err);

	// Python Function Arguments
	PyObject * py_key = NULL;
    PyObject * py_val = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_result = NULL;

	as_operations ops;
    as_policy_read policy;
    as_policy_read * policy_p = NULL;
    as_key key;

	long touchvalue = 0;

    as_operations_inita(&ops, 1);

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "val", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:touch", kwlist, 
			&py_key, &py_val, &py_policy) == false ) {
		return NULL;
	}

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
    // Convert python object into value string  
    //char val[AS_NAMESPACE_MAX_SIZE];
    if( !PyInt_Check(py_val) && !PyLong_Check(py_val) ) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Value should be a integer or long");
        goto CLEANUP;
    } else {
    touchvalue = PyInt_AsLong(py_val);
	}
    //strncpy(ns, namespace, AS_NAMESPACE_MAX_SIZE);

	py_result = AerospikeClient_Operate_Invoke(self, &key, "", NULL,
policy_p, &err, touchvalue, AS_OPERATOR_TOUCH, &ops);
	if (py_result)
	{
		aerospike_key_operate(self->as, &err, policy_p, &key, &ops, NULL);
		if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
		//record_to_pyobject(&err, rec, &key, &py_rec);
		}
	}

CLEANUP:
	 if ( err.code != AEROSPIKE_OK ) {
        PyObject * py_err = NULL;
        error_to_pyobject(&err, &py_err);
        PyErr_SetObject(PyExc_Exception, py_err);
        return NULL;
    }
	return PyLong_FromLong(0);
	// Invoke Operation
//	return AerospikeClient_Get_Invoke(self, py_key, py_policy);
}
