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

PyObject * AerospikeKey_Remove(AerospikeKey * key, PyObject * args, PyObject * kwds)
{
	// Python Function Arguments
	PyObject * py_key = key->key;
	PyObject * py_policy = NULL;
	long generation = 0;

	// Python Function Keyword Arguments
	static char * kwlist[] = {"policy", NULL};
	
	// Python Function Argument Parsing
	if ( PyArg_ParseTupleAndKeywords(args, kwds, "|lO:get", kwlist, generation, &py_policy) == false ) {
		return NULL;
	}

	// Invoke Operation
	return AerospikeClient_Remove_Invoke(key->client, py_key, generation, py_policy);
}
