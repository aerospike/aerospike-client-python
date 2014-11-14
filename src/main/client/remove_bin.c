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

/**
 ******************************************************************************************************
 * Removes a bin from a record.
 *
 * @param self                  AerospikeClient object
 * @prama py_key                The key for the record.
 * @pram py_binList             The name of the bins to be removed from the record.
 * @param py_policy             The optional policies.
 * @param err                   The C client's as_error to be set to the encountered error.
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject * AerospikeClient_RemoveBin_Invoke(
        AerospikeClient * self, 
        PyObject * py_key,PyObject* py_binList ,PyObject * py_policy, as_error *err)
{

    // Aerospike Client Arguments
    //as_error err;
    as_policy_write write_policy;
    as_policy_write * write_policy_p = NULL;
    as_key key;
    as_record rec;
    char* binName = NULL;
    int count = 0;

    // Initialize error
    //as_error_init(&err);

    // Get the bin list size;	
    Py_ssize_t size = PyList_Size(py_binList);
    // Initialize record
    as_record_inita(&rec, size);

    // Convert python key object to as_key
    pyobject_to_key(err, py_key, &key);
    if ( err->code != AEROSPIKE_OK ) {
        goto CLEANUP;
    }

    if (py_policy) {
        validate_policy_write(err, py_policy, &write_policy_p);
    }
	if ( err->code != AEROSPIKE_OK ) {
		goto CLEANUP;
	}

    // Convert python policy object to as_policy_write
    pyobject_to_policy_write(err, py_policy, &write_policy, &write_policy_p);
    if ( err->code != AEROSPIKE_OK ) {
        goto CLEANUP;
    }

    // Invoke operation

    for ( count = 0; count < size; count++ ) {
        PyObject * py_val = PyList_GetItem(py_binList, count);
        if( PyString_Check(py_val) ) {
            binName = PyString_AsString(py_val);
            if(!as_record_set_nil(&rec, binName))
            {
                goto CLEANUP;
            }
        }
        else	
        {
            goto CLEANUP;
        }
    }

    if (AEROSPIKE_OK != aerospike_key_put(self->as, err, write_policy_p, &key, &rec)) 
    {
        goto CLEANUP;
    }

CLEANUP:

    as_record_destroy(&rec);

    if ( err->code != AEROSPIKE_OK ) {
        PyObject * py_err = NULL;
        error_to_pyobject(err, &py_err);
        PyErr_SetObject(PyExc_Exception, py_err);
        return NULL;
    }
    return PyLong_FromLong(0);
}

/**
 ******************************************************************************************************
 * Removes a bin from a record.
 * 
 * @param self                  AerospikeClient object
 * @param args                  The args is a tuple object containing an argument
 *                              list passed from Python to a C function
 * @param kwds                  Dictionary of keywords
 *
 * Returns an integer status. 0(Zero) is success value.
 * In case of error,appropriate exceptions will be raised.
 *******************************************************************************************************
 */
PyObject * AerospikeClient_RemoveBin(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
    // Python Function Arguments
    PyObject * py_key = NULL;
    PyObject * py_policy = NULL;
    PyObject * py_binList = NULL;

    as_error err;
    // Initialize error
    as_error_init(&err);
    // Python Function Keyword Arguments
    static char * kwlist[] = {"key", "list", "policy", NULL};

    // Python Function Argument Parsing
    if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:remove_bin", kwlist, 
                &py_key, &py_binList ,&py_policy) == false ) {
        return NULL;
    }

    if(!PyList_Check(py_binList)) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Bins should be a list");
        goto CLEANUP;
    }
    // Invoke Operation
    return AerospikeClient_RemoveBin_Invoke(self, py_key, py_binList, py_policy, &err);
CLEANUP:

    if ( err.code != AEROSPIKE_OK ) {
        PyObject * py_err = NULL;
        error_to_pyobject(&err, &py_err);
        PyErr_SetObject(PyExc_Exception, py_err);
    }
    return NULL;
}

