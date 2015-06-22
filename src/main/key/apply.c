/*******************************************************************************
 * Copyright 2013-2015 Aerospike, Inc.
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

PyObject * AerospikeKey_Apply(AerospikeKey * key, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_key = key->key;
	PyObject * py_module = NULL;
	PyObject * py_function = NULL;
	PyObject * py_arglist = NULL;
	PyObject * py_policy = NULL;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"module", "function", "args", "policy", NULL};

	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "OOO|O:get", kwlist, 
		&py_module, &py_function, &py_arglist, &py_policy) == false ) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Apply_Invoke(key->client, py_key, py_module, py_function, 
		py_arglist, py_policy);
}
