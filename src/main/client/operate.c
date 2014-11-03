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

	// Python Function Keyword Arguments
	static char * kwlist[] = {"key", "bin", "val", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO|O:get", kwlist, 
			&py_key, &py_bin, &py_val, &py_policy) == false ) {
		return NULL;
	}

    // Convert python object into value string  
    //char val[AS_NAMESPACE_MAX_SIZE];
    if( !PyString_Check(py_val) ) {
        as_error_update(&err, AEROSPIKE_ERR_PARAM, "Value should be a
string");
        goto CLEANUP;
    }
    char *value = PyString_AsString(py_ns);
    //strncpy(ns, namespace, AS_NAMESPACE_MAX_SIZE);

	// Invoke Operation
//	return AerospikeClient_Get_Invoke(self, py_key, py_policy);
}
