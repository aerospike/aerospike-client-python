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
    as_key * key, PyObject * py_bin, char* val, as_error * err, long ttl,
    long initial_value, long offset, long operation, as_operations * ops)
{
    char* bin = NULL;

	switch(operation) {
		case AS_OPERATOR_APPEND:
    		if( !PyString_Check(py_bin) ) {
        		as_error_update(err, AEROSPIKE_ERR_PARAM, "Bin should be a string");
        		goto CLEANUP;
    		}
    		bin = PyString_AsString(py_bin);
			as_operations_add_append_str(ops, bin, val);
			break;
		case AS_OPERATOR_PREPEND:
    		if( !PyString_Check(py_bin) ) {
        		as_error_update(err, AEROSPIKE_ERR_PARAM, "Bin should be a string");
        		goto CLEANUP;
    		}
    		bin = PyString_AsString(py_bin);
			as_operations_add_prepend_str(ops, bin, val);
			break;
        case AS_OPERATOR_INCR:
    		if( !PyString_Check(py_bin) ) {
        		as_error_update(err, AEROSPIKE_ERR_PARAM, "Bin should be a string");
        		goto CLEANUP;
    		}
    		bin = PyString_AsString(py_bin);
            as_val* value_p = NULL;
            const char* select[] = {bin, NULL};
            as_record* get_rec = NULL;
            aerospike_key_select(self->as,
                    err, NULL, key, select, &get_rec);
            if (err->code != AEROSPIKE_OK) {
                goto CLEANUP;
            }
            else 
            {
                if (NULL != (value_p = (as_val *) as_record_get (get_rec, bin))) {
                    if (AS_NIL == value_p->type) {
                        if (!as_operations_add_write_int64(ops, bin,
                                    initial_value)) {
                            goto CLEANUP;
                        }
                    } else {
                        as_operations_add_incr(ops, bin, offset);
                    }
                } else {
                    goto CLEANUP;
                }
            }
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

PyObject * AerospikeClient_convert_pythonObj_to_asType(
        AerospikeClient * self,
        as_error *err, PyObject* py_key, PyObject* py_policy,
        as_key* key_p, as_policy_operate** operate_policy_pp)
{
    as_policy_operate operate_policy;

    pyobject_to_key(err, py_key, key_p);
    if ( err->code != AEROSPIKE_OK ) {
        goto CLEANUP;
    }

    // Convert python policy object to as_policy_operate
    pyobject_to_policy_operate(err, py_policy, &operate_policy, operate_policy_pp);
    if ( err->code != AEROSPIKE_OK ) {
        goto CLEANUP;
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

PyObject * AerospikeClient_Append(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
    // Initialize error
    as_error err;
    as_error_init(&err);

	// Python Function Arguments
	PyObject * py_key = NULL;
    PyObject * py_bin = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_result = NULL;
    char* append_str = NULL;

	as_operations ops;
    //as_policy_operate policy;
    as_policy_operate * policy_p = NULL;
    as_key key;

   // Initialize ops
    as_operations_inita(&ops, 1);

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "bin", "val", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOs|O:append", kwlist, 
			&py_key, &py_bin, &append_str, &py_policy) == false ) {
		return NULL;
	}

    py_result = AerospikeClient_convert_pythonObj_to_asType(self, &err,
            py_key, py_policy, &key, &policy_p);
    if (!py_result) {
        goto CLEANUP;
    }

	py_result = AerospikeClient_Operate_Invoke(self, &key, py_bin, append_str,
            &err, 0, 0, 0, AS_OPERATOR_APPEND, &ops);
	if (py_result)
	{
		aerospike_key_operate(self->as, &err, policy_p, &key, &ops, NULL);
		if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
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
}

PyObject * AerospikeClient_Prepend(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
    // Initialize error
    as_error err;
    as_error_init(&err);

	// Python Function Arguments
	PyObject * py_key = NULL;
    PyObject * py_bin = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_result = NULL;
	//PyObject * py_rec = NULL;
    char* prepend_str = NULL;

	as_operations ops;
    //as_policy_opearte policy;
    as_policy_operate * policy_p = NULL;
    as_key key;
   // Initialize record
    as_operations_inita(&ops, 1);

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "bin", "val", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOs|O:prepend", kwlist, 
			&py_key, &py_bin, &prepend_str, &py_policy) == false ) {
		return NULL;
	}

    py_result = AerospikeClient_convert_pythonObj_to_asType(self, &err,
            py_key, py_policy, &key, &policy_p);
    if (!py_result) {
        goto CLEANUP;
    }

	py_result = AerospikeClient_Operate_Invoke(self, &key, py_bin, prepend_str,
            &err, 0, 0, 0, AS_OPERATOR_PREPEND, &ops);
	if (py_result)
	{
		aerospike_key_operate(self->as, &err, policy_p, &key, &ops, NULL);
		if (err.code != AEROSPIKE_OK) {
        goto CLEANUP;
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
}
PyObject * AerospikeClient_Increment(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
    // Initialize error
    as_error err;
    as_error_init(&err);

	// Python Function Arguments
	PyObject * py_key = NULL;
	PyObject * py_policy = NULL;
	PyObject * py_result = NULL;
    PyObject * py_bin = NULL;

	as_operations ops;
    as_key key;
    as_policy_operate * policy_p = NULL;

    long offset_val = 0;
    long initial_val = 0; 

    as_operations_inita(&ops, 1);

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "bin", "offset", "initial_value", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOl|lO:increment", kwlist, 
			&py_key, &py_bin, &offset_val, &initial_val, &py_policy) == false ) {
		return NULL;
	}

    py_result = AerospikeClient_convert_pythonObj_to_asType(self, &err,
                          py_key, py_policy, &key, &policy_p);
    if (!py_result) {
        goto CLEANUP;
    }

	py_result = AerospikeClient_Operate_Invoke(self, &key, py_bin, NULL,
            &err, 0, initial_val, offset_val, AS_OPERATOR_INCR, &ops);
	if (py_result)
	{
		aerospike_key_operate(self->as, &err, policy_p, &key, &ops, NULL);
		if (err.code != AEROSPIKE_OK) {
            goto CLEANUP;
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
    as_key key;
    as_policy_operate * policy_p = NULL;
	long touchvalue = 0;

    as_operations_inita(&ops, 1);

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "val", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:touch", kwlist, 
			&py_key, &py_val, &py_policy) == false ) {
		return NULL;
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
    py_result = AerospikeClient_convert_pythonObj_to_asType(self, &err,
                          py_key, py_policy, &key, &policy_p);
    if (!py_result) {
        goto CLEANUP;
    }

	py_result = AerospikeClient_Operate_Invoke(self, &key, NULL, NULL,
&err, touchvalue, 0, 0, AS_OPERATOR_TOUCH, &ops);
	if (py_result)
	{
		aerospike_key_operate(self->as, &err, policy_p, &key, &ops, NULL);
		if (err.code != AEROSPIKE_OK) {
            goto CLEANUP;
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
}
