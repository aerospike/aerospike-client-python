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

PyObject * AerospikeClient_RemoveBin_Invoke(
        AerospikeClient * self, 
        PyObject * py_key,PyObject* py_binList ,PyObject * py_policy)
{

    // Aerospike Client Arguments
    as_error err;
    as_policy_write policy;
    as_policy_write * policy_p = NULL;
    as_key key;
    as_record rec;
    char* binName = NULL;
    int count = 0; 

    // Initialize error
    as_error_init(&err);

    if(!PyList_Check(py_binList))
    {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Bins should be a list");
        goto CLEANUP;
    }

    // Get the bin list size;	
    Py_ssize_t size = PyList_Size(py_binList);

    // Initialize record
    as_record_inita(&rec, size);

    // Convert python key object to as_key
    pyobject_to_key(&err, py_key, &key);
    if ( err.code != AEROSPIKE_OK ) {
        goto CLEANUP;
    }

    // Convert python policy object to as_policy_write
    pyobject_to_policy_write(&err, py_policy, &policy, &policy_p);
    if ( err.code != AEROSPIKE_OK ) {
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

    if (AEROSPIKE_OK != aerospike_key_put(self->as, &err,NULL, &key, &rec)) 
    {
        goto CLEANUP;
    }

CLEANUP:

    as_record_destroy(&rec);

    if ( err.code != AEROSPIKE_OK ) {
        PyObject * py_err = NULL;
        error_to_pyobject(&err, &py_err);
        PyErr_SetObject(PyExc_Exception, py_err);
        return NULL;
    }

    return PyLong_FromLong(0);
}

PyObject * AerospikeClient_RemoveBin(AerospikeClient * self, PyObject * args, PyObject * kwds)
{
    // Python Function Arguments
    PyObject * py_key = NULL;
    PyObject * py_policy = NULL;
    PyObject * py_binList = NULL;

    // Python Function Keyword Arguments
    static char * kwlist[] = {"key", "list", "policy", NULL};

    // Python Function Argument Parsing
    if ( PyArg_ParseTupleAndKeywords(args, kwds, "OO|O:remove_bin", kwlist, 
                &py_key, &py_binList ,&py_policy) == false ) {
        return NULL;
    }



    // Invoke Operation
    return AerospikeClient_RemoveBin_Invoke(self, py_key, py_binList, py_policy);
}

